# LibP2PKadDHT

[![](https://img.shields.io/badge/made%20by-Breth-blue.svg?style=flat-square)](https://breth.app)
[![](https://img.shields.io/badge/project-libp2p-yellow.svg?style=flat-square)](http://libp2p.io/)
[![Swift Package Manager compatible](https://img.shields.io/badge/SPM-compatible-blue.svg?style=flat-square)](https://github.com/apple/swift-package-manager)
![Build & Test (macos and linux)](https://github.com/swift-libp2p/swift-libp2p-kad-dht/actions/workflows/build+test.yml/badge.svg)

> A Kademlia Distributed Hash Table for LibP2P

## Table of Contents

- [Overview](#overview)
- [Install](#install)
- [Usage](#usage)
  - [Registering the DHT](#registering-the-dht)
  - [Reading and writing values](#reading-and-writing-values)
  - [Providing and finding content](#providing-and-finding-content)
  - [Finding peers](#finding-peers)
  - [Discovery](#discovery)
  - [Namespaces and validators](#namespaces-and-validators)
  - [Configuration](#configuration)
  - [Lifecycle](#lifecycle)
- [Testing](#testing)
- [Contributing](#contributing)
- [Credits](#credits)
- [License](#license)

## Overview
The Kademlia Distributed Hash Table (DHT) subsystem in libp2p is a DHT implementation largely based on the Kademlia [0] whitepaper, augmented with notions from S/Kademlia [1], Coral [2] and the BitTorrent DHT.

#### DHT operations

The libp2p Kademlia DHT offers the following types of operations:

- **Peer routing**

  - Finding the closest nodes to a given key via `FIND_NODE`.

- **Value storage and retrieval**

  - Storing a value on the nodes closest to the value's key by looking up the
    closest nodes via `FIND_NODE` and then putting the value to those nodes via
    `PUT_VALUE`.

  - Getting a value by its key from the nodes closest to that key via
    `GET_VALUE`.

- **Content provider advertisement and discovery**

  - Adding oneself to the list of providers for a given key at the nodes closest
    to that key by finding the closest nodes via `FIND_NODE` and then adding
    oneself via `ADD_PROVIDER`.

  - Getting providers for a given key from the nodes closest to that key via
    `GET_PROVIDERS`.

In addition the libp2p Kademlia DHT offers the auxiliary _bootstrap_ operation.

#### Note:
- For more information check out the [Kad DHT Spec](https://github.com/libp2p/specs/blob/master/kad-dht/README.md)

## Install

Include the following dependency in your Package.swift file
```Swift
let package = Package(
    ...
    dependencies: [
        ...
        .package(url: "https://github.com/swift-libp2p/swift-libp2p-kad-dht.git", .upToNextMinor(from: "0.3.0"))
    ],

    ...
)
```

## Usage

### Registering the DHT

```Swift
import LibP2P
import LibP2PKadDHT

let app = try await Application.make(.detect(), peerID: .ephemeral(type: .Ed25519))

/// Client mode with reasonable defaults, bootstrapped with the public IPFS nodes.
app.dht.use(.kadDHT)

/// Or configure it. When operating in`.server` mode, the node registers the `/ipfs/kad/1.0.0`
/// route and the `/pk/` + `/ipns/` validators, so the node answers queries as well as asking them.
app.dht.use(
    .kadDHT(
        mode: .server,
        configuration: .default,
        bootstrapPeers: BootstrapPeerDiscovery.IPFSBootNodes,
        autoUpdate: true
    )
)

/// Access the kademlia DHT anywhere using app.
let dht = app.dht.kadDHT
```

When `autoUpdate: true` (the default) the node schedules two recurring tasks:
1) a maintenance beat every 2 minutes (provider expiry, value GC, record re-publishing) 
2) a routing-table refresh every 10 minutes. 
With `autoUpdate: false` nothing is scheduled and you drive the `heartbeat()` yourself.

### Reading and writing values

Keys are namespaced byte arrays — `/pk/<multihash>`, `/ipns/<name>`, or your own namespace.

```Swift
/// Publish our own public key under /pk/<our multihash>
let record = try KadDHT.createPubKeyRecord(peerID: app.peerID)
let stored = try await dht.storeNew(record.key.byteArray, value: record)

/// Read it back. Local store first, then an iterative GET_VALUE across the network.
if let found = try await dht.get(record.key.byteArray) {
    print(found.value)
}
```

Both are also available as futures...

```Swift
dht.get(key).whenSuccess { record in ... }
```

`getWithTrace(_:)` returns the same record alongside a `LookupTrace` recording every peer the
lookup asked and what each answered, useful for debugging.

### Providing and finding content

```Swift
let cid = try CID("QmSomeContent...").rawBuffer

/// Announce ourselves as a provider to the k closest peers.
try await dht.provide(cid: cid)

/// `announce: false` only records the CID locally, nothing goes on the wire.
try await dht.provide(cid: cid, announce: false)

/// Finds up to 5 providers. Pass 0 to search to convergence.
let addresses = try await dht.findProviders(cid: cid, count: 5)
```

### Finding peers

```Swift
let peerInfo = try await dht.findPeer(peer: somePeerID)
print(peerInfo.addresses)
```

### Discovery

KadDHT also conforms to swift-libp2p's discovery service, so it can be used purely for peer
discovery:

```Swift
app.discovery.use(.kadDHT)

app.dht.kadDHT.onPeerDiscovered = { (peer: PeerInfo) in
    print("Discovered \(peer.peer)")
}
```

### Namespaces and validators

A `PUT` for a namespace with no registered validator is rejected.
The `/pk/` and `/ipns/` namespaces are registered by default in server mode.
You can define any other namespace and install the Validator like so...

```Swift
struct FruitValidator: Validator {
    func validate(key: [UInt8], value: [UInt8]) throws {
        guard !value.isEmpty else { throw MyError.empty }
    }

    /// Which of several conflicting values wins.
    func select(key: [UInt8], values: [[UInt8]]) throws -> Int { 0 }
}

try await dht.handle(namespace: "fruit", validator: FruitValidator()).get()
```

Validators see the `DHT.Record.value`'s bytes

### Configuration

```Swift
let configuration = KadDHT.Configuration(
    bucketSize: 20,
    concurrency: 10,
    connectionTimeout: .seconds(4),
    supportLocalNetwork: true
)
```

| Parameter | Default | |
|---|---|---|
| `bucketSize` | `20` | Bucket size and replication parameter (`k`) |
| `concurrency` | `10` | Requests a query path keeps in flight (`α`) |
| `resiliency` | `3` | Closest peers that must respond to finish a lookup (`β`) |
| `quorum` | `0` | Records a value lookup collects before stopping early, `0` searches to convergence |
| `connectionTimeout` | `4 s` | Per-request timeout |
| `supportLocalNetwork` | `false` | Allow private/local addresses |
| `acceptObservedProviderAddress` | `false` | Trust the observed address when an `ADD_PROVIDER` advertises none |
| `maxValueStoreEntries` | `1000` | Value-store capacity |
| `maxRecordAge` | `48 h` | Longest a value record is held, from its `timeReceived` |
| `valueGCInterval` | `24 h` | Value-store sweep cadence |
| `maxProviderStoreEntries` | `1000` | Provider-store capacity |
| `provideValidity` | `48 h` | How long a held provider record stays valid |
| `reprovideInterval` | `22 h` | How often we re-announce our own provider records |
| `providerAddrTTL` | `24 h` | How long provider multiaddrs are served (not yet implemented) |
| `heartbeatInterval` | `120 s` | Maintenance beat cadence |
| `refreshInterval` | `10 min` | Routing-table refresh cadence |
| `refreshQueryTimeout` | `10 s` | Per-query timeout during a refresh |
| `maxRefreshPrefixLength` | `12` | Deepest bucket a refresh aims a targeted lookup at |
| `routingTableLatencyTolerance` | `10 s` | Acceptable peer latency (not yet implemented) |
| `usefulnessGracePeriod` | `2 * refreshInterval` | How long a peer stays "useful" after it last helped |
| `replacementStrategy` | `.furtherThanReplacement` | How to go about replacing peers in a full bucket |

### Lifecycle

The node registers itself with the app's lifecycle, so it starts on boot and stops on shutdown. To
drive it yourself:

```Swift
try dht.start()

/// Awaitable, non-blocking.
await dht.stop()

/// Or the future...
try await dht.shutdown().get()
```

## Testing

```bash
swift test
```

Two suites are gated behind environment variables and skipped by default:

```bash
## Multiple real nodes over Noise + Yamux on 127.0.0.1
PerformInternalIntegrationTests=true swift test

## Dials the public IPFS Amino DHT
PerformExternalIntegrationTests=true swift test
```

## Contributing

Contributions are welcomed! This code is very much a proof of concept. I can guarantee you there's a better / safer way to accomplish the same results. Any suggestions, improvements, or even just critques, are welcome!

Let's make this code better together! 🤝

## Credits

- [Kad DHT Spec](https://github.com/libp2p/specs/blob/master/kad-dht/README.md)
- [go-libp2p-kad-dht](https://github.com/libp2p/go-libp2p-kad-dht)
- [IPNS Record Spec](https://specs.ipfs.tech/ipns/ipns-record/)
- [[0]: Maymounkov, P., & Mazières, D. (2002). Kademlia: A Peer-to-Peer Information System Based on the XOR Metric. In P. Druschel, F. Kaashoek, & A. Rowstron (Eds.), Peer-to-Peer Systems (pp. 53–65). Berlin, Heidelberg: Springer Berlin Heidelberg.](https://doi.org/10.1007/3-540-45748-8_5)
- [[1]: Baumgart, I., & Mies, S. (2014). S / Kademlia : A practicable approach towards secure key-based routing S / Kademlia : A Practicable Approach Towards Secure Key-Based Routing, (June).](https://doi.org/10.1109/ICPADS.2007.4447808)
- [[2]: Freedman, M. J., & Mazières, D. (2003). Sloppy Hashing and Self-Organizing Clusters. In IPTPS. Springer Berlin / Heidelberg. Retrieved from](www.coralcdn.org/docs/coral-iptps03.ps)
- [bittorrent](http://bittorrent.org/beps/bep_0005.html)
- [uvarint-spec](https://github.com/multiformats/unsigned-varint)
- [ping](https://github.com/libp2p/specs/issues/183)
- [go-libp2p-xor](https://github.com/libp2p/go-libp2p-xor)

## License

[MIT](LICENSE) © 2026 Breth Inc.
