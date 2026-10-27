use axum::http::StatusCode;
use orivon_ipfs_gateway::router_with_provider_and_name_resolver;
use orivon_ipfs_namesys::{
    CachedNameResolver, CloudflareDohResolver, DefaultNameResolver, DelegatedIpnsResolver,
    FallbackIpnsResolver,
};
use orivon_ipfs_retrieval::{FetchingBlockProvider, RetrievalStats};
use orivon_ipfs_routing::{
    AutoRoutingClient, DelegatedRoutingClient, DhtIpnsResolver, LightDhtClient,
    DEFAULT_DELEGATED_ROUTER,
};
use orivon_ipfs_store::SqliteBlockStore;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

const DEFAULT_SOAK_CORPUS: &str = r#"
vitalik-home /ipfs/bafybeiaql2jo3fu5b7c4lmpoi5drh5sam7yt652shwdgwbky4o7uw33u2u 1024
daicowtf-home /ipfs/bafybeidznfolm74c5cephzdycedx7hk76iawno45wemcvkflieotzo2lne 1024
"#;
const DEFAULT_ROUNDS: usize = 2;
const DEFAULT_MAX_RSS_GROWTH_KIB: u64 = 128 * 1024;
const REQUEST_ATTEMPTS: usize = 5;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "live retrieval soak test against public IPFS; opt in with make live-soak"]
async fn live_retrieval_soak_repeated_cold_gateways_keep_rss_bounded() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let rounds = env_usize("ORIVON_IPFS_LIVE_SOAK_ROUNDS", DEFAULT_ROUNDS);
    let max_rss_growth_kib = env_u64(
        "ORIVON_IPFS_LIVE_SOAK_MAX_RSS_GROWTH_KIB",
        DEFAULT_MAX_RSS_GROWTH_KIB,
    );
    let corpus =
        env::var("ORIVON_IPFS_LIVE_SOAK_CORPUS").unwrap_or_else(|_| DEFAULT_SOAK_CORPUS.into());
    let entries = parse_corpus(&corpus);
    assert!(!entries.is_empty(), "live soak corpus is empty");

    let rss_before = current_rss_kib();
    let mut totals = RetrievalStats::default();
    let mut total_bytes = 0usize;
    for round in 1..=rounds.max(1) {
        let (stats, bytes) = run_cold_gateway_round(round, &entries).await;
        totals.cache_hits += stats.cache_hits;
        totals.http_provider_blocks += stats.http_provider_blocks;
        totals.bitswap_blocks += stats.bitswap_blocks;
        total_bytes += bytes;
    }
    let rss_after = current_rss_kib();

    eprintln!(
        "live retrieval soak completed: rounds={} entries={} bytes={} rss_before_kib={:?} rss_after_kib={:?}",
        rounds,
        entries.len(),
        total_bytes,
        rss_before,
        rss_after
    );
    eprintln!(
        "live retrieval soak transport counts: cache_hits={} http_provider_blocks={} bitswap_blocks={}",
        totals.cache_hits, totals.http_provider_blocks, totals.bitswap_blocks
    );
    assert!(
        totals.cache_hits + totals.http_provider_blocks + totals.bitswap_blocks > 0,
        "live retrieval soak did not record any retrieval transport"
    );
    if let (Some(before), Some(after)) = (rss_before, rss_after) {
        assert!(
            after <= before.saturating_add(max_rss_growth_kib),
            "RSS grew by {} KiB across {rounds} live retrieval rounds, max allowed {max_rss_growth_kib} KiB",
            after.saturating_sub(before)
        );
    }
}

async fn run_cold_gateway_round(round: usize, entries: &[CorpusEntry]) -> (RetrievalStats, usize) {
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
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            router_with_provider_and_name_resolver(provider, name_resolver),
        )
        .await
        .unwrap();
    });
    eprintln!("live soak round {round}: local gateway listening on http://{addr}");

    let client = reqwest::Client::new();
    let mut total_bytes = 0usize;
    for entry in entries {
        let url = format!("http://{addr}{}", entry.path);
        let (status, body) = fetch_gateway_body_with_retries(&client, &url, REQUEST_ATTEMPTS)
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "live soak request failed in round {round} for {} ({}): {err}",
                    entry.name, entry.path
                )
            });
        assert_eq!(
            status,
            StatusCode::OK,
            "live soak request failed in round {round} for {} ({}): {}",
            entry.name,
            entry.path,
            String::from_utf8_lossy(&body)
        );
        assert!(
            body.len() >= entry.min_bytes,
            "live soak response too small in round {round} for {} ({}): {} bytes, expected at least {}",
            entry.name,
            entry.path,
            body.len(),
            entry.min_bytes
        );
        total_bytes += body.len();
        eprintln!(
            "live soak round {round}: fetched {} {} through local gateway: {} bytes",
            entry.name,
            entry.path,
            body.len()
        );
    }

    let stats = stats_provider.stats();
    eprintln!(
        "live soak round {round}: cache_hits={} http_provider_blocks={} bitswap_blocks={}",
        stats.cache_hits, stats.http_provider_blocks, stats.bitswap_blocks
    );
    server.abort();
    let _ = server.await;
    (stats, total_bytes)
}

async fn fetch_gateway_body_with_retries(
    client: &reqwest::Client,
    url: &str,
    attempts: usize,
) -> Result<(StatusCode, Vec<u8>), String> {
    let mut last_error = None;
    for attempt in 1..=attempts.max(1) {
        match client.get(url).send().await {
            Ok(response) => {
                let status = response.status();
                match response.bytes().await {
                    Ok(body) => {
                        if is_transient_gateway_status(status) && attempt < attempts {
                            last_error = Some(format!(
                                "transient gateway status {status}: {}",
                                String::from_utf8_lossy(&body)
                            ));
                        } else {
                            return Ok((status, body.to_vec()));
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
            assert_eq!(fields.len(), 3, "invalid corpus line: {line}");
            let path = fields[1].to_string();
            assert!(
                path.starts_with("/ipfs/") || path.starts_with("/ipns/"),
                "corpus path must be a local gateway path: {path}"
            );
            CorpusEntry {
                name: fields[0].to_string(),
                path,
                min_bytes: fields[2]
                    .parse()
                    .unwrap_or_else(|_| panic!("invalid min_bytes in corpus line: {line}")),
            }
        })
        .collect()
}

struct CorpusEntry {
    name: String,
    path: String,
    min_bytes: usize,
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

#[cfg(target_os = "linux")]
fn current_rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let value = line.strip_prefix("VmRSS:")?;
        value.split_whitespace().next()?.parse().ok()
    })
}

#[cfg(not(target_os = "linux"))]
fn current_rss_kib() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_live_soak_corpus_entries() {
        let entries = parse_corpus(
            r#"
            # comment
            example /ipfs/bafyexample 42
            ipns-example /ipns/example.net 7
            "#,
        );

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "example");
        assert_eq!(entries[0].path, "/ipfs/bafyexample");
        assert_eq!(entries[0].min_bytes, 42);
        assert_eq!(entries[1].name, "ipns-example");
        assert_eq!(entries[1].path, "/ipns/example.net");
        assert_eq!(entries[1].min_bytes, 7);
    }
}
