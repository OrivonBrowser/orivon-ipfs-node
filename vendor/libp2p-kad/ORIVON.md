# libp2p-kad 0.48.0 as Orivon uses it

This is the crates.io release `libp2p-kad` 0.48.0 with two changes, both in `src/behaviour.rs`.

**Addresses of the providers a lookup hears.**

A lookup for the providers of a key (`Behaviour::get_providers`) hears, from each server it asks, the
provider records the server holds, each with the addresses the provider was announced with. The
release passes the provider ids up (`GetProvidersOk::FoundProviders`) and keeps their addresses in
the query only, until the query ends, so the caller cannot learn where a provider can be reached.
A provider that is a DHT server is in the routing table; one that is a DHT client behind a circuit
relay is in no routing table and in no `FIND_NODE` answer, so for it the addresses in the record
are the only ones that exist.

The change: when a server answers a provider lookup, the providers it names are put in the
behaviour's record store with their addresses and an expiry of five minutes (`LOOKUP_PROVIDER_TTL`).
The caller reads them with `store.providers(key)` after `FoundProviders`.

Meant for a client that asks and never answers (`Mode::Client`): a node in server mode would serve
the records it merely heard of. This change goes when a release returns the addresses with the
lookup's result.

**Logs of the announcement.** When the walk of `start_providing` ends, a `debug` line on the target
`orivon_kad_announce` says how many peers the record goes to and how many requests the walk made, and
one more says how many requests the sending phase made. The release logs neither, and the first is
the only way to see that a walk which failed to dial the nearest peers stored the record elsewhere.

The directory and the `libp2p-kad` line of `[patch.crates-io]` go when neither change is needed.
