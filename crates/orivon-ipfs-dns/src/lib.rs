//! The name servers the WASI build resolves `/dns*` and `/dnsaddr` names with.
//!
//! A program running in Orivon has no system resolver, and its UDP datagrams are dropped unless the
//! host granted a send to the exact server. TCP to a name server is the path a host can grant
//! without opening UDP, so the config here names two servers and speaks TCP to them only. The
//! hosts that run this node must allow TCP connections to both addresses in [`NAME_SERVERS`];
//! port 53 is reserved in Orivon, so the grant has to name the address and port, not just the host.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use hickory_resolver::config::{NameServerConfig, NameServerConfigGroup, ResolverConfig};
use hickory_resolver::proto::xfer::Protocol;

/// Cloudflare's public resolvers, IPv4 only: the node dials out over IPv4 on every host that runs it.
pub const NAME_SERVERS: [SocketAddr; 2] = [
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), 53),
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(1, 0, 0, 1)), 53),
];

/// A resolver config with [`NAME_SERVERS`] over TCP, no UDP, no IPv6 server, no search domain.
pub fn tcp_resolver_config() -> ResolverConfig {
    let servers: Vec<NameServerConfig> = NAME_SERVERS
        .iter()
        .map(|addr| NameServerConfig::new(*addr, Protocol::Tcp))
        .collect();
    ResolverConfig::from_parts(None, Vec::new(), NameServerConfigGroup::from(servers))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_names_the_two_cloudflare_servers_over_tcp_only() {
        let config = tcp_resolver_config();
        let servers = config.name_servers();

        let addrs: Vec<String> = servers.iter().map(|s| s.socket_addr.to_string()).collect();
        assert_eq!(addrs, ["1.1.1.1:53", "1.0.0.1:53"]);
        assert!(
            servers.iter().all(|s| s.protocol == Protocol::Tcp),
            "a UDP name server would send datagrams the host drops"
        );
        assert!(servers.iter().all(|s| s.socket_addr.is_ipv4()));
        assert!(config.domain().is_none());
        assert!(config.search().is_empty());
    }
}
