use axum::http::{
    header::{CONTENT_RANGE, RANGE},
    StatusCode,
};
use orivon_ipfs_gateway::router_with_provider_and_name_resolver;
use orivon_ipfs_namesys::{
    CachedNameResolver, CloudflareDohResolver, DefaultNameResolver, DelegatedIpnsResolver,
    FallbackIpnsResolver,
};
use orivon_ipfs_retrieval::FetchingBlockProvider;
use orivon_ipfs_routing::{
    AutoRoutingClient, DelegatedRoutingClient, DhtIpnsResolver, LightDhtClient,
    DEFAULT_DELEGATED_ROUTER,
};
use orivon_ipfs_store::SqliteBlockStore;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

const DEFAULT_CORPUS: &str = include_str!("../../../tests/fixtures/public_corpus.txt");
const REQUEST_ATTEMPTS: usize = 5;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "network corpus smoke test against documented public /ipfs and /ipns paths"]
async fn public_corpus_fetches_through_local_gateway() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let corpus =
        env::var("ORIVON_IPFS_LIVE_CORPUS").unwrap_or_else(|_| DEFAULT_CORPUS.to_string());
    let entries = parse_corpus(&corpus);
    assert!(!entries.is_empty(), "public corpus is empty");

    let router = env::var("ORIVON_IPFS_DELEGATED_ROUTER")
        .unwrap_or_else(|_| DEFAULT_DELEGATED_ROUTER.to_string());
    let store = SqliteBlockStore::in_memory(256 * 1024 * 1024).unwrap();
    let dht = LightDhtClient::default();
    let routing = AutoRoutingClient::new(DelegatedRoutingClient::new(router.clone()), dht.clone());
    let provider = Arc::new(FetchingBlockProvider::new(store, routing));
    let stats_provider = provider.clone();
    let name_resolver = Arc::new(CachedNameResolver::new(DefaultNameResolver::new(
        CloudflareDohResolver::default(),
        FallbackIpnsResolver::new(
            DelegatedIpnsResolver::new(router),
            DhtIpnsResolver::new(dht),
        ),
    )));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            router_with_provider_and_name_resolver(provider, name_resolver),
        )
        .await
        .unwrap();
    });
    eprintln!("local gateway listening on http://{addr} with auto routing");

    let client = reqwest::Client::new();
    for entry in entries {
        let url = format!("http://{addr}{}", entry.path);
        let (status, headers, body) =
            fetch_gateway_body_with_retries(&client, &url, None, REQUEST_ATTEMPTS)
                .await
                .unwrap_or_else(|err| {
                    panic!(
                        "public corpus request failed for {} ({}): {err}",
                        entry.name, entry.path
                    )
                });
        assert_eq!(
            status,
            StatusCode::OK,
            "public corpus request failed for {} ({}): {}",
            entry.name,
            entry.path,
            String::from_utf8_lossy(&body)
        );
        assert!(
            body.len() >= entry.min_bytes,
            "public corpus response too small for {} ({}): {} bytes, expected at least {}",
            entry.name,
            entry.path,
            body.len(),
            entry.min_bytes
        );
        eprintln!(
            "fetched corpus entry {} {} through local gateway: {} bytes",
            entry.name,
            entry.path,
            body.len()
        );

        if let Some(range_bytes) = entry.range_bytes {
            assert!(
                body.len() >= range_bytes,
                "public corpus response too small for range check on {} ({}): {} bytes, expected at least {}",
                entry.name,
                entry.path,
                body.len(),
                range_bytes
            );
            assert!(
                headers.get(CONTENT_RANGE).is_none(),
                "full response unexpectedly included content-range for {} ({})",
                entry.name,
                entry.path
            );

            let range_end = range_bytes - 1;
            let range_header = format!("bytes=0-{range_end}");
            let (range_status, range_headers, range_body) = fetch_gateway_body_with_retries(
                &client,
                &url,
                Some(&range_header),
                REQUEST_ATTEMPTS,
            )
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "public corpus range request failed for {} ({}): {err}",
                    entry.name, entry.path
                )
            });
            assert_eq!(
                range_status,
                StatusCode::PARTIAL_CONTENT,
                "public corpus range request returned wrong status for {} ({})",
                entry.name,
                entry.path
            );
            assert_eq!(
                range_body,
                body[..range_bytes],
                "public corpus range response did not match full response prefix for {} ({})",
                entry.name,
                entry.path
            );
            let content_range = range_headers
                .get(CONTENT_RANGE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("");
            assert!(
                content_range.starts_with(&format!("bytes 0-{range_end}/")),
                "invalid content-range for {} ({}): {content_range}",
                entry.name,
                entry.path
            );
            eprintln!(
                "range checked corpus entry {} {} through local gateway: {} bytes",
                entry.name,
                entry.path,
                range_body.len()
            );
        }
    }

    let stats = stats_provider.stats();
    eprintln!(
        "retrieval transport counts: cache_hits={} http_provider_blocks={} bitswap_blocks={}",
        stats.cache_hits, stats.http_provider_blocks, stats.bitswap_blocks
    );
    assert!(
        stats.cache_hits + stats.http_provider_blocks + stats.bitswap_blocks > 0,
        "public corpus smoke did not record any retrieval transport"
    );
}

async fn fetch_gateway_body_with_retries(
    client: &reqwest::Client,
    url: &str,
    range: Option<&str>,
    attempts: usize,
) -> Result<(StatusCode, reqwest::header::HeaderMap, Vec<u8>), String> {
    let mut last_error = None;
    for attempt in 1..=attempts.max(1) {
        let mut request = client.get(url);
        if let Some(range) = range {
            request = request.header(RANGE, range);
        }
        match request.send().await {
            Ok(response) => {
                let status = response.status();
                let headers = response.headers().clone();
                match response.bytes().await {
                    Ok(body) => {
                        if is_transient_gateway_status(status) && attempt < attempts {
                            last_error = Some(format!(
                                "transient gateway status {status}: {}",
                                String::from_utf8_lossy(&body)
                            ));
                        } else {
                            return Ok((status, headers, body.to_vec()));
                        }
                    }
                    Err(err) => last_error = Some(format!("response body error: {err}")),
                }
            }
            Err(err) => last_error = Some(format!("request error: {err}")),
        }
        if attempt < attempts {
            tokio::time::sleep(Duration::from_secs(attempt as u64)).await;
        }
    }
    Err(last_error.unwrap_or_else(|| "request was not attempted".to_string()))
}

fn is_transient_gateway_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT
    )
}

fn parse_corpus(corpus: &str) -> Vec<CorpusEntry> {
    corpus
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            assert!(
                fields.len() == 3 || fields.len() == 4,
                "invalid corpus line: {line}"
            );
            let path = fields[1].to_string();
            assert!(
                path.starts_with("/ipfs/") || path.starts_with("/ipns/"),
                "corpus path must be a local gateway path: {path}"
            );
            let range_bytes = fields.get(3).map(|value| {
                let parsed = value
                    .parse()
                    .unwrap_or_else(|_| panic!("invalid range_bytes in corpus line: {line}"));
                assert!(parsed > 0, "range_bytes must be greater than zero: {line}");
                parsed
            });
            CorpusEntry {
                name: fields[0].to_string(),
                path,
                min_bytes: fields[2]
                    .parse()
                    .unwrap_or_else(|_| panic!("invalid min_bytes in corpus line: {line}")),
                range_bytes,
            }
        })
        .collect()
}

struct CorpusEntry {
    name: String,
    path: String,
    min_bytes: usize,
    range_bytes: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_public_corpus_entries() {
        let entries = parse_corpus(
            r#"
            # comment
            example /ipfs/bafyexample 42 16
            ipns-example /ipns/example.net 7
            "#,
        );

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "example");
        assert_eq!(entries[0].path, "/ipfs/bafyexample");
        assert_eq!(entries[0].min_bytes, 42);
        assert_eq!(entries[0].range_bytes, Some(16));
        assert_eq!(entries[1].name, "ipns-example");
        assert_eq!(entries[1].path, "/ipns/example.net");
        assert_eq!(entries[1].min_bytes, 7);
        assert_eq!(entries[1].range_bytes, None);
    }
}
