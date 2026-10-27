use axum::http::StatusCode;
use orivon_ipfs_gateway::router_with_provider_and_name_resolver;
use orivon_ipfs_namesys::{
    CachedNameResolver, CloudflareDohResolver, DefaultNameResolver, DelegatedIpnsResolver,
    FallbackIpnsResolver,
};
use orivon_ipfs_retrieval::{FetchingBlockProvider, RetrievalStats};
use orivon_ipfs_routing::{
    AutoRoutingClient, DelegatedRoutingClient, DhtIpnsResolver, LightDhtClient,
    ProviderRoutingClient, RoutingStats, RoutingStatsHandle, DEFAULT_DELEGATED_ROUTER,
};
use orivon_ipfs_store::SqliteBlockStore;
use serde::Deserialize;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

const REQUEST_ATTEMPTS: usize = 5;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "network smoke test; set ORIVON_IPFS_LIVE_PATHS=/ipfs/<cid>,/ipns/<name>"]
async fn live_gateway_fetches_real_paths_without_public_gateway_fallback() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .ok();

    let mut paths = env::var("ORIVON_IPFS_LIVE_PATHS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    let ens_names = env::var("ORIVON_IPFS_LIVE_ENS")
        .unwrap_or_else(|_| "vitalik.eth,daicowtf.eth".to_string());
    for name in ens_names
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        let resolved = resolve_ens_contenthash(name).await;
        eprintln!("{name} resolved to {resolved}");
        paths.push(resolved);
    }
    assert!(
        !paths.is_empty(),
        "set ORIVON_IPFS_LIVE_PATHS or ORIVON_IPFS_LIVE_ENS"
    );

    let router = env::var("ORIVON_IPFS_DELEGATED_ROUTER")
        .unwrap_or_else(|_| DEFAULT_DELEGATED_ROUTER.to_string());
    let store = SqliteBlockStore::in_memory(256 * 1024 * 1024).unwrap();
    let dht = LightDhtClient::default();
    let routing_stats = RoutingStatsHandle::default();
    let routing = ProviderRoutingClient::from(AutoRoutingClient::new(
        DelegatedRoutingClient::new(router.clone()),
        dht.clone(),
    ))
    .with_stats(routing_stats.clone());
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
    for path in paths {
        assert!(
            path.starts_with("/ipfs/") || path.starts_with("/ipns/"),
            "live path must be externally resolved into /ipfs or /ipns form: {path}"
        );
        let url = format!("http://{addr}{path}");
        let before_retrieval = stats_provider.stats();
        let before_routing = routing_stats.snapshot();
        let (status, body) = fetch_gateway_body_with_retries(&client, &url, REQUEST_ATTEMPTS)
            .await
            .unwrap_or_else(|err| panic!("live gateway request failed for {path}: {err}"));
        let retrieval_delta = retrieval_stats_delta(before_retrieval, stats_provider.stats());
        let routing_delta = routing_stats_delta(before_routing, routing_stats.snapshot());
        assert_eq!(
            status,
            StatusCode::OK,
            "live gateway request failed for {path}: {}",
            String::from_utf8_lossy(&body)
        );
        assert!(
            !body.is_empty(),
            "live gateway returned empty body for {path}"
        );
        eprintln!(
            "fetched {path} through {url}: {} bytes; retrieval_delta={}; routing_delta={}",
            body.len(),
            describe_retrieval_stats(retrieval_delta),
            describe_routing_stats(routing_delta)
        );
    }

    let stats = stats_provider.stats();
    let routing = routing_stats.snapshot();
    eprintln!(
        "retrieval transport counts: cache_hits={} http_provider_blocks={} bitswap_blocks={}",
        stats.cache_hits, stats.http_provider_blocks, stats.bitswap_blocks
    );
    eprintln!(
        "routing provider counts: delegated_lookups={} delegated_results={} delegated_errors={} dht_lookups={} dht_results={} dht_errors={}",
        routing.delegated_provider_lookups,
        routing.delegated_provider_results,
        routing.delegated_provider_errors,
        routing.dht_provider_lookups,
        routing.dht_provider_results,
        routing.dht_provider_errors,
    );
    assert!(
        stats.cache_hits + stats.http_provider_blocks + stats.bitswap_blocks > 0,
        "live smoke did not record any retrieval transport"
    );
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

fn retrieval_stats_delta(before: RetrievalStats, after: RetrievalStats) -> RetrievalStats {
    RetrievalStats {
        cache_hits: after.cache_hits.saturating_sub(before.cache_hits),
        http_provider_blocks: after
            .http_provider_blocks
            .saturating_sub(before.http_provider_blocks),
        bitswap_blocks: after.bitswap_blocks.saturating_sub(before.bitswap_blocks),
    }
}

fn routing_stats_delta(before: RoutingStats, after: RoutingStats) -> RoutingStats {
    RoutingStats {
        delegated_provider_lookups: after
            .delegated_provider_lookups
            .saturating_sub(before.delegated_provider_lookups),
        delegated_provider_results: after
            .delegated_provider_results
            .saturating_sub(before.delegated_provider_results),
        delegated_provider_errors: after
            .delegated_provider_errors
            .saturating_sub(before.delegated_provider_errors),
        dht_provider_lookups: after
            .dht_provider_lookups
            .saturating_sub(before.dht_provider_lookups),
        dht_provider_results: after
            .dht_provider_results
            .saturating_sub(before.dht_provider_results),
        dht_provider_errors: after
            .dht_provider_errors
            .saturating_sub(before.dht_provider_errors),
    }
}

fn describe_retrieval_stats(stats: RetrievalStats) -> String {
    format!(
        "cache_hits={},http_provider_blocks={},bitswap_blocks={}",
        stats.cache_hits, stats.http_provider_blocks, stats.bitswap_blocks
    )
}

fn describe_routing_stats(stats: RoutingStats) -> String {
    format!(
        "delegated_lookups={},delegated_results={},delegated_errors={},dht_lookups={},dht_results={},dht_errors={}",
        stats.delegated_provider_lookups,
        stats.delegated_provider_results,
        stats.delegated_provider_errors,
        stats.dht_provider_lookups,
        stats.dht_provider_results,
        stats.dht_provider_errors,
    )
}

async fn resolve_ens_contenthash(name: &str) -> String {
    let url = format!("https://api.web3.bio/profile/ens/{name}");
    let profile = reqwest::get(url)
        .await
        .expect("ENS profile request failed")
        .error_for_status()
        .expect("ENS profile request returned error")
        .json::<EnsProfile>()
        .await
        .expect("ENS profile JSON parse failed");
    let contenthash = profile
        .contenthash
        .unwrap_or_else(|| panic!("{name} has no contenthash"));
    contenthash_to_gateway_path(&contenthash)
}

fn contenthash_to_gateway_path(contenthash: &str) -> String {
    if let Some(path) = contenthash.strip_prefix("ipfs://") {
        format!("/ipfs/{}", path.trim_start_matches('/'))
    } else if let Some(path) = contenthash.strip_prefix("ipns://") {
        format!("/ipns/{}", path.trim_start_matches('/'))
    } else {
        panic!("unsupported live contenthash: {contenthash}");
    }
}

#[derive(Debug, Deserialize)]
struct EnsProfile {
    contenthash: Option<String>,
}
