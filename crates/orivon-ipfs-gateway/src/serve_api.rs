//! The HTTP side of serve mode: `GET /id`, `POST /blocks`, `POST /dial`, and for a node that
//! listens for connections `POST /provide`, `DELETE /provide/<cid>` and `GET /status`.
//!
//! These answer a single caller, the page that owns the node, so they ignore partitions. Every
//! request must carry `x-orivon-key`, since anything on the machine can reach a loopback port.

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Request, State};
use axum::http::{header, HeaderName, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::Router;
use orivon_ipfs_core::parse_cid;
use orivon_ipfs_serve::{
    parse_and_verify_car_yielding, AddOutcome, DialError, KeptBlocks, ProvideError, ServeNode,
    MAX_PROVIDED,
};
use std::sync::Arc;
use tokio::sync::Semaphore;

pub const KEY_HEADER: &str = "x-orivon-key";
/// Largest `POST /blocks` body.
pub const MAX_CAR_BYTES: usize = 64 * 1024 * 1024;
const MAX_DIAL_BODY_BYTES: usize = 4 * 1024;
const MAX_PROVIDE_BODY_BYTES: usize = 4 * 1024;
pub const FEATURES_HEADER: &str = "x-orivon-node-features";

#[derive(Clone)]
pub struct ServeApiState {
    kept: KeptBlocks,
    node: ServeNode,
    api_key: String,
    // One `POST /blocks` at a time: a body is up to 64 MiB and is hashed before it is kept.
    upload: Arc<Semaphore>,
}

impl ServeApiState {
    pub fn new(kept: KeptBlocks, node: ServeNode, api_key: String) -> Self {
        Self {
            kept,
            node,
            api_key,
            upload: Arc::new(Semaphore::new(1)),
        }
    }
}

pub fn is_valid_api_key(key: &str) -> bool {
    key.len() == 64 && key.bytes().all(|b| b.is_ascii_hexdigit())
}

fn keys_match(expected: &str, given: &[u8]) -> bool {
    let expected = expected.as_bytes();
    let mut difference = u8::from(expected.len() != given.len());
    for (index, byte) in expected.iter().enumerate() {
        difference |= byte ^ given.get(index).copied().unwrap_or(0);
    }
    difference == 0
}

pub fn routes(state: ServeApiState) -> Router {
    let mut router = Router::new()
        .route("/id", get(id))
        .route(
            "/blocks",
            post(post_blocks).layer(DefaultBodyLimit::max(MAX_CAR_BYTES)),
        )
        .route(
            "/dial",
            post(post_dial).layer(DefaultBodyLimit::max(MAX_DIAL_BODY_BYTES)),
        );
    // Only a node that listens has anything to report, and only one with a DHT can announce;
    // without them these paths do not exist, as before.
    if state.node.features().is_some() {
        router = router.route("/status", get(status));
    }
    if state.node.can_provide() {
        router = router
            .route(
                "/provide",
                post(post_provide).layer(DefaultBodyLimit::max(MAX_PROVIDE_BODY_BYTES)),
            )
            .route("/provide/{cid}", delete(delete_provide));
    }
    router
        .layer(middleware::from_fn_with_state(state.clone(), require_key))
        .with_state(state)
}

/// Adds `x-orivon-node-features: <features>` to the answers to `/health`, which is how a page
/// learns what this build of the node can do.
pub fn with_features_header(router: Router, features: &'static str) -> Router {
    router.layer(middleware::from_fn(
        move |request: Request, next: Next| async move {
            let is_health = request.uri().path() == "/health";
            let mut response = next.run(request).await;
            if is_health {
                response.headers_mut().insert(
                    HeaderName::from_static(FEATURES_HEADER),
                    HeaderValue::from_static(features),
                );
            }
            response
        },
    ))
}

async fn require_key(State(state): State<ServeApiState>, request: Request, next: Next) -> Response {
    let given = request
        .headers()
        .get(KEY_HEADER)
        .map(|value| value.as_bytes())
        .unwrap_or_default();
    if !keys_match(&state.api_key, given) {
        return text(StatusCode::UNAUTHORIZED, "missing or wrong x-orivon-key");
    }
    next.run(request).await
}

fn text(status: StatusCode, message: &str) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        message.to_string(),
    )
        .into_response()
}

fn json(value: serde_json::Value) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        value.to_string(),
    )
        .into_response()
}

async fn id(State(state): State<ServeApiState>) -> Response {
    json(serde_json::json!({ "peer": state.node.peer_id().to_string() }))
}

async fn post_blocks(State(state): State<ServeApiState>, body: Bytes) -> Response {
    let Ok(_turn) = state.upload.clone().try_acquire_owned() else {
        return text(
            StatusCode::TOO_MANY_REQUESTS,
            "another upload is in progress; try again",
        );
    };
    let car = match parse_and_verify_car_yielding(&body).await {
        Ok(car) => car,
        Err(err) => return text(StatusCode::BAD_REQUEST, &err.to_string()),
    };
    let count = car.blocks.len();
    match state.kept.insert_all(car.blocks) {
        Ok(()) => json(serde_json::json!({
            "roots": car.roots.iter().map(|root| root.to_string()).collect::<Vec<_>>(),
            "blocks": count,
        })),
        Err(err) => text(StatusCode::INSUFFICIENT_STORAGE, &err.to_string()),
    }
}

async fn post_dial(State(state): State<ServeApiState>, body: Bytes) -> Response {
    let addr = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| value.get("addr")?.as_str().map(str::to_string));
    let Some(addr) = addr else {
        return text(StatusCode::BAD_REQUEST, "body must be {\"addr\": \"<multiaddr>\"}");
    };
    match state.node.dial(&addr).await {
        Ok(peer) => json(serde_json::json!({ "peer": peer.to_string() })),
        Err(err @ DialError::BadAddress(_)) => text(StatusCode::BAD_REQUEST, &err.to_string()),
        Err(DialError::Timeout) => text(StatusCode::GATEWAY_TIMEOUT, "dial timed out"),
        Err(err @ DialError::TooManyPeers) => text(StatusCode::TOO_MANY_REQUESTS, &err.to_string()),
        Err(DialError::Failed(_)) => text(StatusCode::BAD_GATEWAY, "could not connect"),
    }
}

fn json_with(status: StatusCode, value: serde_json::Value) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        value.to_string(),
    )
        .into_response()
}

async fn post_provide(State(state): State<ServeApiState>, body: Bytes) -> Response {
    let cid = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| value.get("cid")?.as_str().map(str::to_string))
        .and_then(|text| parse_cid(&text).ok());
    let Some(cid) = cid else {
        return text(StatusCode::BAD_REQUEST, "body must be {\"cid\": \"<cid>\"}");
    };
    if state.kept.get(&cid).is_none() {
        return text(StatusCode::NOT_FOUND, "that block is not kept");
    }
    match state.node.provide(cid).await {
        Ok(AddOutcome::Added | AddOutcome::AlreadyProvided) => json_with(
            StatusCode::ACCEPTED,
            serde_json::json!({ "cid": cid.to_string() }),
        ),
        Err(ProvideError::TooMany) => text(
            StatusCode::TOO_MANY_REQUESTS,
            &format!("already announcing {MAX_PROVIDED} CIDs"),
        ),
        Err(_) => text(StatusCode::SERVICE_UNAVAILABLE, "the node cannot announce"),
    }
}

async fn delete_provide(State(state): State<ServeApiState>, Path(cid): Path<String>) -> Response {
    let Ok(cid) = parse_cid(&cid) else {
        return text(StatusCode::BAD_REQUEST, "not a CID");
    };
    match state.node.unprovide(cid).await {
        Ok(true) => json(serde_json::json!({ "cid": cid.to_string() })),
        Ok(false) => text(StatusCode::NOT_FOUND, "that CID is not being announced"),
        Err(_) => text(StatusCode::SERVICE_UNAVAILABLE, "the node cannot announce"),
    }
}

async fn status(State(state): State<ServeApiState>) -> Response {
    match state.node.status().await {
        Ok(status) => json(serde_json::json!({
            "peer": status.peer.to_string(),
            "listen": status.listen.iter().map(|addr| addr.to_string()).collect::<Vec<_>>(),
            "external": status.external.iter().map(|addr| addr.to_string()).collect::<Vec<_>>(),
            "relays": status.relays.iter().map(|addr| addr.to_string()).collect::<Vec<_>>(),
            "reachability": status.reachability.as_str(),
            "providing": status.providing.iter().map(|entry| serde_json::json!({
                "cid": entry.cid,
                "announced": entry.announced,
            })).collect::<Vec<_>>(),
        })),
        Err(_) => text(StatusCode::SERVICE_UNAVAILABLE, "the node cannot report"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orivon_ipfs_core::{cid_from_data, encode_car_v1, CarBlock, CODEC_RAW};

    const KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn api_keys_are_64_hex_characters() {
        assert!(is_valid_api_key(KEY));
        assert!(!is_valid_api_key(&KEY[1..]));
        assert!(!is_valid_api_key(&format!("{}g", &KEY[1..])));
        assert!(!is_valid_api_key(""));
    }

    #[test]
    fn key_comparison_checks_every_byte_and_the_length() {
        assert!(keys_match(KEY, KEY.as_bytes()));
        assert!(!keys_match(KEY, b""));
        assert!(!keys_match(KEY, &KEY.as_bytes()[..63]));
        let mut other = KEY.as_bytes().to_vec();
        other[63] ^= 1;
        assert!(!keys_match(KEY, &other));
        let mut longer = KEY.as_bytes().to_vec();
        longer.push(b'0');
        assert!(!keys_match(KEY, &longer));
    }

    #[test]
    fn a_header_whose_length_differs_by_a_multiple_of_256_is_refused() {
        let mut long = KEY.as_bytes().to_vec();
        long.resize(320, b'0');
        assert!(!keys_match(KEY, &long));
        long.resize(64 + 512, b'0');
        assert!(!keys_match(KEY, &long));
    }

    async fn serve_with(allow_local: bool) -> (String, KeptBlocks) {
        let kept = KeptBlocks::default();
        let node = ServeNode::start_with(kept.clone(), allow_local).await.unwrap();
        let app = routes(ServeApiState::new(kept.clone(), node, KEY.to_string()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (base, kept)
    }

    async fn serve() -> (String, KeptBlocks) {
        serve_with(true).await
    }

    fn car_of(blocks: &[&[u8]]) -> Vec<u8> {
        encode_car_v1(
            &blocks
                .iter()
                .map(|data| CarBlock {
                    cid: cid_from_data(CODEC_RAW, data),
                    data: data.to_vec(),
                })
                .collect::<Vec<_>>(),
        )
    }

    #[tokio::test]
    async fn the_key_gates_every_route_and_the_routes_answer() {
        let (base, kept) = serve().await;
        let client = reqwest::Client::new();

        for (method, path) in [("GET", "/id"), ("POST", "/blocks"), ("POST", "/dial")] {
            let request = client.request(method.parse().unwrap(), format!("{base}{path}"));
            assert_eq!(request.send().await.unwrap().status(), 401, "{path} no key");
            let request = client
                .request(method.parse().unwrap(), format!("{base}{path}"))
                .header(KEY_HEADER, KEY.replace('0', "1"));
            assert_eq!(request.send().await.unwrap().status(), 401, "{path} bad key");
        }
        assert!(kept.is_empty());

        let id = client
            .get(format!("{base}/id"))
            .header(KEY_HEADER, KEY)
            .send()
            .await
            .unwrap();
        assert_eq!(id.status(), 200);
        let body: serde_json::Value = id.json().await.unwrap();
        assert!(body["peer"].as_str().unwrap().starts_with("12D3"));

        let ok = client
            .post(format!("{base}/blocks"))
            .header(KEY_HEADER, KEY)
            .body(car_of(&[b"alpha", b"beta"]))
            .send()
            .await
            .unwrap();
        assert_eq!(ok.status(), 200);
        let body: serde_json::Value = ok.json().await.unwrap();
        assert_eq!(body["blocks"], 2);
        assert_eq!(kept.len(), 2);

        let mut bad = car_of(&[b"gamma"]);
        let last = bad.len() - 1;
        bad[last] ^= 1;
        let refused = client
            .post(format!("{base}/blocks"))
            .header(KEY_HEADER, KEY)
            .body(bad)
            .send()
            .await
            .unwrap();
        assert_eq!(refused.status(), 400);
        assert_eq!(kept.len(), 2);

        let junk = client
            .post(format!("{base}/blocks"))
            .header(KEY_HEADER, KEY)
            .body("not a car")
            .send()
            .await
            .unwrap();
        assert_eq!(junk.status(), 400);

        let no_addr = client
            .post(format!("{base}/dial"))
            .header(KEY_HEADER, KEY)
            .body("{}")
            .send()
            .await
            .unwrap();
        assert_eq!(no_addr.status(), 400);
        let bad_addr = client
            .post(format!("{base}/dial"))
            .header(KEY_HEADER, KEY)
            .body(r#"{"addr":"/ip4/127.0.0.1/tcp/1"}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(bad_addr.status(), 400);
        let refused_connection = client
            .post(format!("{base}/dial"))
            .header(KEY_HEADER, KEY)
            .body(r#"{"addr":"/ip4/127.0.0.1/tcp/1/p2p/12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP"}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(refused_connection.status(), 502);
        assert_eq!(refused_connection.text().await.unwrap(), "could not connect");
    }

    #[tokio::test]
    async fn dialing_the_machine_or_its_network_is_refused_by_default() {
        let (base, _) = serve_with(false).await;
        let client = reqwest::Client::new();
        for host in ["ip4/127.0.0.1", "ip4/10.0.0.5", "ip6/::1", "dns4/localhost"] {
            let body = format!(
                r#"{{"addr":"/{host}/tcp/1/p2p/12D3KooWNDpFqyse9kR7aZwgEzh4U1mL6Zz6jEuRNFXJxL5D2KPP"}}"#
            );
            let response = client
                .post(format!("{base}/dial"))
                .header(KEY_HEADER, KEY)
                .body(body)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 400, "{host}");
        }
    }

    #[tokio::test]
    async fn a_second_upload_while_one_runs_answers_429() {
        let kept = KeptBlocks::default();
        let node = ServeNode::start_with(kept.clone(), true).await.unwrap();
        let state = ServeApiState::new(kept, node, KEY.to_string());
        let held = state.upload.clone().try_acquire_owned().unwrap();
        let app = routes(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let response = reqwest::Client::new()
            .post(format!("{base}/blocks"))
            .header(KEY_HEADER, KEY)
            .body(car_of(&[b"x"]))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 429);
        drop(held);
        let response = reqwest::Client::new()
            .post(format!("{base}/blocks"))
            .header(KEY_HEADER, KEY)
            .body(car_of(&[b"x"]))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }

    async fn serve_listening() -> (String, KeptBlocks) {
        let port = {
            let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            socket.local_addr().unwrap().port()
        };
        let kept = KeptBlocks::default();
        let config = orivon_ipfs_serve::P2pConfig::new(port)
            .with_dht(vec![])
            .with_assume_public();
        let node = ServeNode::start_with_p2p(kept.clone(), true, Some(config))
            .await
            .unwrap();
        let app = routes(ServeApiState::new(kept.clone(), node, KEY.to_string()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (base, kept)
    }

    fn raw_cid(data: &[u8]) -> String {
        cid_from_data(CODEC_RAW, data).to_string()
    }

    async fn provide(client: &reqwest::Client, base: &str, cid: &str) -> reqwest::Response {
        client
            .post(format!("{base}/provide"))
            .header(KEY_HEADER, KEY)
            .body(format!(r#"{{"cid":"{cid}"}}"#))
            .send()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn without_a_listener_there_is_no_provide_and_no_status() {
        let (base, _) = serve().await;
        let client = reqwest::Client::new();
        for (method, path) in [("POST", "/provide"), ("GET", "/status"), ("DELETE", "/provide/x")] {
            let response = client
                .request(method.parse().unwrap(), format!("{base}{path}"))
                .header(KEY_HEADER, KEY)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 404, "{method} {path}");
        }
    }

    #[tokio::test]
    async fn a_listener_without_a_dht_has_a_status_but_cannot_provide() {
        let port = {
            let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            socket.local_addr().unwrap().port()
        };
        let kept = KeptBlocks::default();
        let node = ServeNode::start_with_p2p(
            kept.clone(),
            true,
            Some(orivon_ipfs_serve::P2pConfig::new(port)),
        )
        .await
        .unwrap();
        let app = routes(ServeApiState::new(kept, node, KEY.to_string()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let client = reqwest::Client::new();
        let status = client
            .get(format!("{base}/status"))
            .header(KEY_HEADER, KEY)
            .send()
            .await
            .unwrap();
        assert_eq!(status.status(), 200);
        assert_eq!(provide(&client, &base, &raw_cid(b"x")).await.status(), 404);
    }

    #[tokio::test]
    async fn the_key_gates_provide_and_status() {
        let (base, _) = serve_listening().await;
        let client = reqwest::Client::new();
        for (method, path) in [("POST", "/provide"), ("GET", "/status"), ("DELETE", "/provide/x")] {
            let none = client
                .request(method.parse().unwrap(), format!("{base}{path}"))
                .send()
                .await
                .unwrap();
            assert_eq!(none.status(), 401, "{method} {path}");
            let wrong = client
                .request(method.parse().unwrap(), format!("{base}{path}"))
                .header(KEY_HEADER, KEY.replace('0', "1"))
                .send()
                .await
                .unwrap();
            assert_eq!(wrong.status(), 401, "{method} {path}");
        }
    }

    #[tokio::test]
    async fn provide_wants_a_kept_block_and_answers_202() {
        let (base, kept) = serve_listening().await;
        let client = reqwest::Client::new();
        let cid = raw_cid(b"alpha");
        assert_eq!(provide(&client, &base, &cid).await.status(), 404, "not kept yet");
        kept.insert_all(vec![(cid_from_data(CODEC_RAW, b"alpha"), b"alpha".to_vec())])
            .unwrap();
        let accepted = provide(&client, &base, &cid).await;
        assert_eq!(accepted.status(), 202);
        let body: serde_json::Value = accepted.json().await.unwrap();
        assert_eq!(body["cid"], cid);
        assert_eq!(provide(&client, &base, &cid).await.status(), 202, "again is fine");
        for bad in ["{}", "not json", r#"{"cid":"nope"}"#, r#"{"cid":7}"#] {
            let response = client
                .post(format!("{base}/provide"))
                .header(KEY_HEADER, KEY)
                .body(bad)
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), 400, "{bad}");
        }
    }

    #[tokio::test]
    async fn delete_provide_stops_a_cid_and_404s_when_it_was_not_provided() {
        let (base, kept) = serve_listening().await;
        let client = reqwest::Client::new();
        let cid = raw_cid(b"beta");
        kept.insert_all(vec![(cid_from_data(CODEC_RAW, b"beta"), b"beta".to_vec())])
            .unwrap();
        let delete = |path: String| {
            let client = client.clone();
            async move {
                client
                    .delete(path)
                    .header(KEY_HEADER, KEY)
                    .send()
                    .await
                    .unwrap()
                    .status()
            }
        };
        assert_eq!(delete(format!("{base}/provide/{cid}")).await, 404);
        provide(&client, &base, &cid).await;
        assert_eq!(delete(format!("{base}/provide/{cid}")).await, 200);
        assert_eq!(delete(format!("{base}/provide/{cid}")).await, 404);
        assert_eq!(delete(format!("{base}/provide/not-a-cid")).await, 400);
    }

    #[tokio::test]
    async fn status_reports_the_peer_the_addresses_and_what_is_provided() {
        let (base, kept) = serve_listening().await;
        let client = reqwest::Client::new();
        let cid = raw_cid(b"gamma");
        kept.insert_all(vec![(cid_from_data(CODEC_RAW, b"gamma"), b"gamma".to_vec())])
            .unwrap();
        provide(&client, &base, &cid).await;
        let status: serde_json::Value = client
            .get(format!("{base}/status"))
            .header(KEY_HEADER, KEY)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(status["peer"].as_str().unwrap().starts_with("12D3"));
        assert!(status["listen"].as_array().is_some());
        assert!(status["external"].as_array().is_some());
        assert_eq!(status["relays"], serde_json::json!([]));
        assert_eq!(status["reachability"], "public");
        assert_eq!(status["providing"][0]["cid"], cid);
        assert!(status["providing"][0]["announced"].is_null());
    }

    #[tokio::test]
    async fn the_1025th_cid_answers_429() {
        let (base, kept) = serve_listening().await;
        let client = reqwest::Client::new();
        let blocks: Vec<Vec<u8>> = (0..=MAX_PROVIDED as u32).map(|i| i.to_be_bytes().to_vec()).collect();
        kept.insert_all(
            blocks
                .iter()
                .map(|data| (cid_from_data(CODEC_RAW, data), data.clone()))
                .collect(),
        )
        .unwrap();
        for data in &blocks[..MAX_PROVIDED] {
            assert_eq!(provide(&client, &base, &raw_cid(data)).await.status(), 202);
        }
        let last = raw_cid(&blocks[MAX_PROVIDED]);
        assert_eq!(provide(&client, &base, &last).await.status(), 429);
        assert_eq!(provide(&client, &base, &raw_cid(&blocks[0])).await.status(), 202);
    }

    #[tokio::test]
    async fn health_carries_the_features_header_only_when_asked() {
        let plain = Router::new().route("/health", get(|| async { "ok" }));
        let featured = with_features_header(plain.clone(), "serve,listen,provide");
        for (app, expected) in [(plain, None), (featured, Some("serve,listen,provide"))] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let response = reqwest::get(format!("{base}/health")).await.unwrap();
            assert_eq!(response.status(), 200);
            assert_eq!(
                response.headers().get(FEATURES_HEADER).and_then(|v| v.to_str().ok()),
                expected
            );
            let other = reqwest::get(format!("{base}/nothing")).await.unwrap();
            assert!(other.headers().get(FEATURES_HEADER).is_none());
        }
    }

    #[tokio::test]
    async fn a_body_over_the_limit_answers_413() {
        let (base, _) = serve().await;
        let response = reqwest::Client::new()
            .post(format!("{base}/dial"))
            .header(KEY_HEADER, KEY)
            .body(vec![b' '; MAX_DIAL_BODY_BYTES + 1])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 413);
    }
}
