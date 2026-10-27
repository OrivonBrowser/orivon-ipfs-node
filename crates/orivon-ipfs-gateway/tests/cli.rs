use axum::Router;
use orivon_ipfs_core::{cid_from_data, encode_car_v1, CarBlock, CODEC_RAW};
use http::StatusCode;
use std::fs;
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;

#[tokio::test]
async fn explicit_offline_routing_runs_cache_only_gateway() {
    let body = b"cli offline fixture";
    let cid = cid_from_data(CODEC_RAW, body);
    let car = encode_car_v1(&[CarBlock {
        cid,
        data: body.to_vec(),
    }]);

    let tempdir = tempfile::tempdir().expect("create tempdir");
    let car_path = tempdir.path().join("fixture.car");
    fs::write(&car_path, car).expect("write fixture CAR");
    let delegated_router_hits = Arc::new(AtomicUsize::new(0));
    let delegated_router_addr =
        start_counting_delegated_router(delegated_router_hits.clone()).await;

    let mut child = ChildGuard::spawn(
        Command::new(env!("CARGO_BIN_EXE_orivon-ipfs-gateway"))
            .arg("--online")
            .arg("--routing-mode")
            .arg("offline")
            .arg("--delegated-router")
            .arg(format!("http://{delegated_router_addr}/routing/v1"))
            .arg("--import-car")
            .arg(&car_path)
            .stdout(Stdio::null())
            .stderr(Stdio::piped()),
    );
    let stderr = child
        .0
        .stderr
        .take()
        .expect("gateway stderr should be piped");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines() {
            if tx.send(line.unwrap_or_default()).is_err() {
                break;
            }
        }
    });

    let addr = wait_for_gateway_addr(&mut child, &rx);
    let client = reqwest::Client::new();
    let health = client
        .get(format!("http://{addr}/health"))
        .send()
        .await
        .expect("health from CLI gateway");
    assert_eq!(health.status(), reqwest::StatusCode::OK);

    // The imported block sits in the shared partition. The node serves only
    // trustless requests, and those read the partition they name (or none at
    // all), so neither form finds it, and the rendered page is refused.
    let rendered = client
        .get(format!("http://{addr}/ipfs/{cid}"))
        .send()
        .await
        .expect("rendered request to CLI gateway");
    assert_eq!(rendered.status(), reqwest::StatusCode::NOT_FOUND);
    for partition in [None, Some("site-a")] {
        let mut request = client.get(format!("http://{addr}/ipfs/{cid}?format=raw"));
        if let Some(partition) = partition {
            request = request.header("x-orivon-partition", partition);
        }
        let response = request.send().await.expect("raw request to CLI gateway");
        assert_eq!(
            response.status(),
            reqwest::StatusCode::NOT_FOUND,
            "{partition:?}"
        );
    }
    let bad = client
        .get(format!("http://{addr}/ipfs/{cid}?format=raw"))
        .header("x-orivon-partition", "not valid!")
        .send()
        .await
        .expect("raw request with a bad partition");
    assert_eq!(bad.status(), reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(delegated_router_hits.load(Ordering::SeqCst), 0);
}

struct ChildGuard(Child);

impl ChildGuard {
    fn spawn(command: &mut Command) -> Self {
        Self(command.spawn().expect("spawn gateway"))
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().expect("poll gateway").is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn wait_for_gateway_addr(child: &mut ChildGuard, rx: &mpsc::Receiver<String>) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        match rx.recv_timeout(Duration::from_millis(100)) {
            Ok(line) => {
                if let Some(addr) = line.strip_prefix("gateway listening on http://") {
                    return addr.to_string();
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Some(status) = child.0.try_wait().expect("poll gateway") {
                    panic!("gateway exited before listening: {status}");
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("gateway stderr closed before listening");
            }
        }
    }

    panic!("timed out waiting for gateway listen address");
}

async fn start_counting_delegated_router(hits: Arc<AtomicUsize>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind delegated router");
    let addr = listener.local_addr().expect("delegated router addr");
    let app = Router::new().fallback(move || {
        let hits = hits.clone();
        async move {
            hits.fetch_add(1, Ordering::SeqCst);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    });
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    addr.to_string()
}
