use anyhow::{Context, Result};
use axum::Router;
use clap::{Parser, ValueEnum};
use orivon_ipfs_core::parse_cid;
use orivon_ipfs_gateway::{
    router_with_provider_and_name_resolver_config, serve_api, GatewayConfig,
    GatewayHtmlDirectoryPrefetchConfig, GatewayHtmlPrefetchConfig, GatewayHtmlRangeWarmConfig,
    PersistentNameResolver, DEFAULT_GATEWAY_MAX_CONCURRENT_REQUESTS,
    DEFAULT_GATEWAY_REQUEST_QUEUE_TIMEOUT_MS, DEFAULT_GATEWAY_SMALL_BODY_CACHE_MAX_BYTES,
};
use orivon_ipfs_namesys::{
    CachedNameResolver, CloudflareDohResolver, DefaultNameResolver, DelegatedIpnsResolver,
    FallbackIpnsResolver, IpnsRecord, IpnsResolver, NamesysError,
};
use orivon_ipfs_retrieval::FetchingBlockProvider;
use orivon_ipfs_routing::{
    AutoRoutingClient, DelegatedRoutingClient, DhtIpnsResolver, LightDhtClient,
    ProviderRoutingClient, DEFAULT_BOOTSTRAP_PEERS, DEFAULT_DELEGATED_ROUTER,
    DEFAULT_DHT_QUERY_TIMEOUT, DEFAULT_MAX_DHT_PROVIDERS,
};
use orivon_ipfs_serve::{KeptBlocks, P2pConfig, ServeNode};
use orivon_ipfs_store::SqliteBlockStore;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

const DEFAULT_TRACE_FILTER: &str =
    "orivon_ipfs_gateway=info,orivon_ipfs_retrieval=info,orivon_ipfs_namesys=info,orivon_ipfs_routing=info,warn";
const HTML_PREFETCH_MAX_ASSETS_ENV: &str = "ORIVON_IPFS_GATEWAY_HTML_PREFETCH_MAX_ASSETS";
const HTML_PREFETCH_MAX_BYTES_ENV: &str = "ORIVON_IPFS_GATEWAY_HTML_PREFETCH_MAX_BYTES";
const HTML_PREFETCH_CONCURRENCY_ENV: &str = "ORIVON_IPFS_GATEWAY_HTML_PREFETCH_CONCURRENCY";
const HTML_DIRECTORY_PREFETCH_MAX_DIRS_ENV: &str =
    "ORIVON_IPFS_GATEWAY_HTML_DIRECTORY_PREFETCH_MAX_DIRS";
const HTML_DIRECTORY_PREFETCH_MAX_BYTES_ENV: &str =
    "ORIVON_IPFS_GATEWAY_HTML_DIRECTORY_PREFETCH_MAX_BYTES";
const HTML_DIRECTORY_PREFETCH_CONCURRENCY_ENV: &str =
    "ORIVON_IPFS_GATEWAY_HTML_DIRECTORY_PREFETCH_CONCURRENCY";
const HTML_RANGE_WARM_MAX_BYTES_ENV: &str = "ORIVON_IPFS_GATEWAY_HTML_RANGE_WARM_MAX_BYTES";
const HTML_RANGE_WARM_CONCURRENCY_ENV: &str = "ORIVON_IPFS_GATEWAY_HTML_RANGE_WARM_CONCURRENCY";
const RAW_LINK_TSIZE_FAST_HEADERS_ENV: &str = "ORIVON_IPFS_ENABLE_RAW_LINK_TSIZE_FAST_HEADERS";
const STREAM_SMALL_BODIES_ENV: &str = "ORIVON_IPFS_GATEWAY_STREAM_SMALL_BODIES";

#[derive(Debug, Parser)]
#[command(author, version, about = "Orivon IPFS node")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:0")]
    addr: SocketAddr,
    #[arg(long)]
    db: Option<PathBuf>,
    #[arg(long)]
    import_car: Option<PathBuf>,
    #[arg(long)]
    export_car: Option<PathBuf>,
    #[arg(long)]
    root: Option<String>,
    #[arg(long)]
    online: bool,
    #[arg(long, default_value = DEFAULT_DELEGATED_ROUTER)]
    delegated_router: String,
    #[arg(long, value_enum, default_value_t = RoutingMode::Auto)]
    routing_mode: RoutingMode,
    #[arg(long, default_value_t = DEFAULT_GATEWAY_MAX_CONCURRENT_REQUESTS)]
    max_concurrent_requests: usize,
    #[arg(long, default_value_t = DEFAULT_GATEWAY_REQUEST_QUEUE_TIMEOUT_MS)]
    request_queue_timeout_ms: u64,
    #[arg(long, default_value_t = DEFAULT_GATEWAY_SMALL_BODY_CACHE_MAX_BYTES)]
    small_body_cache_max_bytes: usize,
    #[arg(long, default_value_t = DEFAULT_DHT_QUERY_TIMEOUT.as_secs())]
    dht_query_timeout_secs: u64,
    #[arg(long, default_value_t = DEFAULT_MAX_DHT_PROVIDERS)]
    dht_max_providers: usize,
    #[arg(long)]
    trace_output: Option<PathBuf>,
    #[arg(long)]
    trace_filter: Option<String>,
    #[arg(long)]
    trace_span_list: bool,
    /// Accept blocks over HTTP (`POST /blocks`), dial peers (`POST /dial`) and offer the blocks
    /// to them over Bitswap. Off by default.
    #[arg(long, requires = "api_key")]
    serve: bool,
    /// 64 hex characters; every `/id`, `/blocks` and `/dial` request must send it as
    /// `x-orivon-key`.
    #[arg(long, value_parser = parse_api_key)]
    api_key: Option<String>,
    /// Lets `POST /dial` reach loopback and private addresses. For tests and local setups only.
    #[arg(long, hide = true, requires = "serve")]
    allow_local_dial: bool,
    /// Accept libp2p connections on this TCP port (IPv4 and IPv6, every interface), and with an
    /// online `--routing-mode` of `auto` or `light-dht` announce `POST /provide` CIDs on the DHT
    /// while the computer is reachable from outside. Adds `POST /provide`, `DELETE /provide/<cid>`
    /// and `GET /status`, and the `x-orivon-node-features` header on `/health`.
    #[arg(long, requires = "serve", value_parser = clap::value_parser!(u16).range(1..))]
    p2p_port: Option<u16>,
    /// `<address>/p2p/<peer id>,...` the DHT starts from instead of the public bootstrap peers.
    /// For tests only.
    #[arg(long, hide = true, requires = "p2p_port", value_delimiter = ',')]
    test_bootstrap: Vec<String>,
}

/// The listener and DHT settings for `--p2p-port`. The DHT needs the node online with a routing
/// mode that includes the DHT; otherwise the node only accepts connections.
fn p2p_config(args: &Args) -> Option<P2pConfig> {
    let mut config = P2pConfig::new(args.p2p_port?);
    if should_start_online_gateway(args)
        && matches!(args.routing_mode, RoutingMode::Auto | RoutingMode::LightDht)
    {
        let bootstrap = if args.test_bootstrap.is_empty() {
            DEFAULT_BOOTSTRAP_PEERS.iter().map(|peer| peer.to_string()).collect()
        } else {
            args.test_bootstrap.clone()
        };
        config = config.with_dht(bootstrap);
    }
    Some(config)
}

fn parse_api_key(value: &str) -> Result<String, String> {
    if serve_api::is_valid_api_key(value) {
        Ok(value.to_string())
    } else {
        Err("expected 64 hex characters".to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RoutingMode {
    Auto,
    Delegated,
    LightDht,
    Offline,
}

// wasm32-wasip2 has no real OS threads (this port is single-threaded by
// design), so the runtime must be `current_thread` there; every other
// target keeps the default multi-thread flavor unchanged.
#[cfg_attr(not(target_os = "wasi"), tokio::main)]
#[cfg_attr(target_os = "wasi", tokio::main(flavor = "current_thread"))]
async fn main() -> Result<()> {
    let args = Args::parse();
    init_tracing(
        args.trace_output.as_deref(),
        args.trace_filter.as_deref(),
        args.trace_span_list,
    )?;
    let start_online_gateway = should_start_online_gateway(&args);
    let p2p = p2p_config(&args);
    let store = if let Some(path) = args.db {
        SqliteBlockStore::open(path, 256 * 1024 * 1024)?
    } else {
        SqliteBlockStore::in_memory(256 * 1024 * 1024)?
    };

    if let Some(car_path) = args.import_car {
        let bytes = fs::read(&car_path).with_context(|| format!("read {}", car_path.display()))?;
        let imported = store.import_car(&bytes)?;
        eprintln!("imported {} CAR blocks", imported.len());
    }

    if let Some(car_path) = args.export_car {
        let bytes = store.export_car()?;
        fs::write(&car_path, bytes).with_context(|| format!("write {}", car_path.display()))?;
        eprintln!("exported cache CAR to {}", car_path.display());
    }

    if let Some(root) = args.root {
        let root = parse_cid(&root)?;
        eprintln!("root: {root}");
    }

    let gateway_config = GatewayConfig::new(args.max_concurrent_requests)
        .with_request_queue_timeout(Duration::from_millis(args.request_queue_timeout_ms))
        .with_small_body_cache_max_bytes(args.small_body_cache_max_bytes)
        .with_html_prefetch(gateway_html_prefetch_config())
        .with_html_directory_prefetch(gateway_html_directory_prefetch_config())
        .with_html_range_warm(gateway_html_range_warm_config())
        .with_raw_link_tsize_fast_headers(raw_link_tsize_fast_headers_enabled())
        .with_stream_small_bodies(stream_small_bodies_enabled())
        // Orivon keeps every cache per partition; the rendered pages read and
        // write one block store shared by all callers, so this node answers
        // the trustless requests (which name their partition) and nothing else.
        .with_trustless_only(true);
    let router = if start_online_gateway {
        let delegated_routers = args.delegated_router.clone();
        let delegated_router_endpoints = delegated_router_endpoints(&delegated_routers);
        let delegated = delegated_routing_client(delegated_router_endpoints.clone());
        let dht = light_dht_client(args.dht_query_timeout_secs, args.dht_max_providers);
        let routing = match args.routing_mode {
            RoutingMode::Auto => {
                ProviderRoutingClient::from(AutoRoutingClient::new(delegated, dht.clone()))
            }
            RoutingMode::Delegated => ProviderRoutingClient::from(delegated),
            RoutingMode::LightDht => ProviderRoutingClient::from(dht.clone()),
            RoutingMode::Offline => ProviderRoutingClient::Offline,
        };
        let provider = FetchingBlockProvider::new(store.clone(), routing);
        let name_resolver = CachedNameResolver::new(PersistentNameResolver::new(
            DefaultNameResolver::new(
                CloudflareDohResolver::default(),
                ipns_resolver(args.routing_mode, delegated_router_endpoints, dht),
            ),
            store,
        ));
        router_with_provider_and_name_resolver_config(
            Arc::new(provider),
            Arc::new(name_resolver),
            gateway_config,
        )
    } else {
        router_with_provider_and_name_resolver_config(
            Arc::new(store.clone()),
            Arc::new(PersistentNameResolver::cache_only(store)),
            gateway_config,
        )
    };
    let router = if args.serve {
        let kept = KeptBlocks::default();
        let node = ServeNode::start_with_p2p(kept.clone(), args.allow_local_dial, p2p)
            .await
            .map_err(|err| anyhow::anyhow!("start the serve node: {err}"))?;
        let features = node.features();
        let router = router.merge(serve_api::routes(serve_api::ServeApiState::new(
            kept,
            node,
            args.api_key.clone().unwrap_or_default(),
        )));
        match features {
            Some(features) => serve_api::with_features_header(router, features),
            None => router,
        }
    } else {
        router
    };
    serve_router(router, args.addr).await?;
    Ok(())
}

fn init_tracing(
    trace_output: Option<&std::path::Path>,
    trace_filter: Option<&str>,
    trace_span_list: bool,
) -> Result<()> {
    let filter = if let Some(trace_filter) = trace_filter {
        tracing_subscriber::EnvFilter::try_new(trace_filter)
            .with_context(|| format!("parse trace filter {trace_filter:?}"))?
    } else if trace_output.is_some() {
        tracing_subscriber::EnvFilter::new(DEFAULT_TRACE_FILTER)
    } else {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"))
    };

    if let Some(path) = trace_output {
        let file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("open trace output {}", path.display()))?;
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .with_span_list(trace_span_list)
            .with_writer(TraceFileWriter(Arc::new(file)))
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
    Ok(())
}

#[derive(Clone)]
struct TraceFileWriter(Arc<fs::File>);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for TraceFileWriter {
    type Writer = &'a fs::File;

    fn make_writer(&'a self) -> Self::Writer {
        self.0.as_ref()
    }
}

async fn serve_router(router: Router, addr: SocketAddr) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;
    eprintln!("gateway listening on http://{bound}");
    axum::serve(listener, router).await
}

fn should_start_online_gateway(args: &Args) -> bool {
    args.online && args.routing_mode != RoutingMode::Offline
}

fn gateway_html_prefetch_config() -> GatewayHtmlPrefetchConfig {
    let max_assets = env_usize(HTML_PREFETCH_MAX_ASSETS_ENV, 0);
    let max_bytes = env_u64(HTML_PREFETCH_MAX_BYTES_ENV, 64 * 1024);
    let concurrency = env_usize(HTML_PREFETCH_CONCURRENCY_ENV, 2);
    GatewayHtmlPrefetchConfig::new(max_assets, max_bytes, concurrency)
}

fn gateway_html_directory_prefetch_config() -> GatewayHtmlDirectoryPrefetchConfig {
    let max_dirs = env_usize(HTML_DIRECTORY_PREFETCH_MAX_DIRS_ENV, 0);
    let max_bytes = env_u64(HTML_DIRECTORY_PREFETCH_MAX_BYTES_ENV, 64 * 1024);
    let concurrency = env_usize(HTML_DIRECTORY_PREFETCH_CONCURRENCY_ENV, 1);
    GatewayHtmlDirectoryPrefetchConfig::new(max_dirs, max_bytes, concurrency)
}

fn gateway_html_range_warm_config() -> GatewayHtmlRangeWarmConfig {
    let max_bytes = env_u64(HTML_RANGE_WARM_MAX_BYTES_ENV, 0);
    let concurrency = env_usize(HTML_RANGE_WARM_CONCURRENCY_ENV, 1);
    GatewayHtmlRangeWarmConfig::new(max_bytes, concurrency)
}

fn raw_link_tsize_fast_headers_enabled() -> bool {
    std::env::var_os(RAW_LINK_TSIZE_FAST_HEADERS_ENV).is_some()
}

fn stream_small_bodies_enabled() -> bool {
    std::env::var_os(STREAM_SMALL_BODIES_ENV).is_some()
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn light_dht_client(dht_query_timeout_secs: u64, dht_max_providers: usize) -> LightDhtClient {
    LightDhtClient::default()
        .with_query_timeout(Duration::from_secs(dht_query_timeout_secs))
        .with_max_providers(dht_max_providers)
}

fn delegated_routing_client(delegated_routers: Vec<String>) -> DelegatedRoutingClient {
    DelegatedRoutingClient::with_endpoints(delegated_routers)
}

fn delegated_router_endpoints(delegated_routers: &str) -> Vec<String> {
    delegated_routers
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_string)
        .collect()
}

fn ipns_resolver(
    routing_mode: RoutingMode,
    delegated_routers: Vec<String>,
    dht: LightDhtClient,
) -> Arc<dyn IpnsResolver> {
    match routing_mode {
        RoutingMode::Auto => Arc::new(FallbackIpnsResolver::new(
            DelegatedIpnsResolver::with_endpoints(delegated_routers),
            DhtIpnsResolver::new(dht),
        )),
        RoutingMode::Delegated => {
            Arc::new(DelegatedIpnsResolver::with_endpoints(delegated_routers))
        }
        RoutingMode::LightDht => Arc::new(DhtIpnsResolver::new(dht)),
        RoutingMode::Offline => Arc::new(OfflineIpnsResolver),
    }
}

#[derive(Debug, Clone)]
struct OfflineIpnsResolver;

#[async_trait::async_trait]
impl IpnsResolver for OfflineIpnsResolver {
    async fn resolve_ipns(&self, name: &str) -> orivon_ipfs_namesys::Result<IpnsRecord> {
        Err(NamesysError::NotFound(name.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_comma_separated_delegated_router_endpoints() {
        assert_eq!(
            delegated_router_endpoints(" https://one.example/routing/v1, ,https://two.example "),
            vec![
                "https://one.example/routing/v1".to_string(),
                "https://two.example".to_string()
            ]
        );
    }

    #[test]
    fn offline_routing_mode_disables_online_gateway() {
        let args = Args::try_parse_from([
            "orivon-ipfs-gateway",
            "--online",
            "--routing-mode",
            "offline",
        ])
        .expect("parse offline routing mode");

        assert!(!should_start_online_gateway(&args));
    }

    #[test]
    fn serve_needs_a_valid_api_key() {
        let key = "ab".repeat(32);
        let args = |extra: &[&str]| {
            let mut all = vec!["orivon-ipfs-gateway"];
            all.extend_from_slice(extra);
            Args::try_parse_from(all)
        };
        assert!(args(&["--serve"]).is_err());
        assert!(args(&["--serve", "--api-key", "short"]).is_err());
        let parsed = args(&["--serve", "--api-key", &key]).unwrap();
        assert!(parsed.serve);
        assert!(!args(&[]).unwrap().serve);
    }

    #[test]
    fn p2p_port_needs_serve_and_a_real_port() {
        let key = "ab".repeat(32);
        let parse = |extra: &[&str]| {
            let mut all = vec!["orivon-ipfs-gateway", "--api-key", &key];
            all.extend_from_slice(extra);
            Args::try_parse_from(all)
        };
        assert!(parse(&["--p2p-port", "4801"]).is_err(), "needs --serve");
        assert!(parse(&["--serve", "--p2p-port", "0"]).is_err());
        assert!(parse(&["--serve", "--p2p-port", "70000"]).is_err());
        assert_eq!(parse(&["--serve", "--p2p-port", "4801"]).unwrap().p2p_port, Some(4801));
        assert_eq!(parse(&["--serve"]).unwrap().p2p_port, None);
        assert!(parse(&["--serve", "--test-bootstrap", "/ip4/1.2.3.4/tcp/1/p2p/x"]).is_err(), "needs --p2p-port");
    }

    #[test]
    fn the_dht_needs_an_online_routing_mode_that_includes_it() {
        let key = "ab".repeat(32);
        let config = |extra: &[&str]| {
            let mut all = vec!["orivon-ipfs-gateway", "--serve", "--api-key", &key, "--p2p-port", "4801"];
            all.extend_from_slice(extra);
            p2p_config(&Args::try_parse_from(all).unwrap()).unwrap()
        };
        assert!(config(&["--online", "--routing-mode", "auto"]).dht.is_some());
        assert!(config(&["--online", "--routing-mode", "light-dht"]).dht.is_some());
        assert!(config(&["--online", "--routing-mode", "delegated"]).dht.is_none());
        assert!(config(&["--online", "--routing-mode", "offline"]).dht.is_none());
        assert!(config(&["--routing-mode", "auto"]).dht.is_none(), "not online");
        let default_peers = config(&["--online"]).dht.unwrap().bootstrap;
        assert_eq!(default_peers.len(), DEFAULT_BOOTSTRAP_PEERS.len());
        let test_peers = config(&["--online", "--test-bootstrap", "/ip4/127.0.0.1/tcp/1/p2p/x,/ip4/127.0.0.1/tcp/2/p2p/y"]);
        assert_eq!(test_peers.dht.unwrap().bootstrap.len(), 2);
    }

    #[test]
    fn online_gateway_requires_online_flag() {
        let args = Args::try_parse_from(["orivon-ipfs-gateway", "--routing-mode", "auto"])
            .expect("parse auto routing mode");

        assert!(!should_start_online_gateway(&args));
    }

    #[test]
    fn auto_routing_mode_starts_online_gateway_when_enabled() {
        let args =
            Args::try_parse_from(["orivon-ipfs-gateway", "--online", "--routing-mode", "auto"])
                .expect("parse online auto routing mode");

        assert!(should_start_online_gateway(&args));
    }
}
