# LibP2PKadDHT

[![](https://img.shields.io/badge/made%20by-Breth-blue.svg?style=flat-square)](https://breth.app)
[![](https://img.shields.io/badge/project-libp2p-yellow.svg?style=flat-square)](http://libp2p.io/)
[![Swift Package Manager compatible](https://img.shields.io/badge/SPM-compatible-blue.svg?style=flat-square)](https://github.com/apple/swift-package-manager)

> A Kademlia Distributed Hash Table for LibP2P

## Table of Contents

- [Overview](#overview)
- [Install](#install)
- [Usage](#usage)
  - [Example](#example)
  - [API](#api)
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
        .package(name: "LibP2PKadDHT", url: "https://github.com/swift-libp2p/swift-libp2p-kad-dht.git", .upToNextMajor(from: "0.0.1"))
    ],
    
    ...
)
```

## Usage

### Example 
check out the [tests]() for more examples

```Swift

import LibP2PKadDHT

let app = Application(.detect())

/// If you'd like to use the DHT as a DHT...
app.dht.use( .kadDHT )

/// Or if you're just interested in its peer discovery functionality
app.discovery.use(.kadDHT)

```

### API
```Swift
TODO
```

## Contributing

Contributions are welcomed! This code is very much a proof of concept. I can guarantee you there's a better / safer way to accomplish the same results. Any suggestions, improvements, or even just critques, are welcome! 

Let's make this code better together! 🤝

[![](https://cdn.rawgit.com/jbenet/contribute-ipfs-gif/master/img/contribute.gif)](https://github.com/ipfs/community/blob/master/contributing.md)

## Credits

- [MPLEX Spec](https://github.com/libp2p/specs/blob/master/mplex/README.md) 
- [[0]: Maymounkov, P., & Mazières, D. (2002). Kademlia: A Peer-to-Peer Information System Based on the XOR Metric. In P. Druschel, F. Kaashoek, & A. Rowstron (Eds.), Peer-to-Peer Systems (pp. 53–65). Berlin, Heidelberg: Springer Berlin Heidelberg.](https://doi.org/10.1007/3-540-45748-8_5)
- [[1]: Baumgart, I., & Mies, S. (2014). S / Kademlia : A practicable approach towards secure key-based routing S / Kademlia : A Practicable Approach Towards Secure Key-Based Routing, (June).](https://doi.org/10.1109/ICPADS.2007.4447808)
- [[2]: Freedman, M. J., & Mazières, D. (2003). Sloppy Hashing and Self-Organizing Clusters. In IPTPS. Springer Berlin / Heidelberg. Retrieved from](www.coralcdn.org/docs/coral-iptps03.ps)
- [bittorrent](http://bittorrent.org/beps/bep_0005.html)
- [uvarint-spec](https://github.com/multiformats/unsigned-varint)
- [ping](https://github.com/libp2p/specs/issues/183)
- [go-libp2p-xor](https://github.com/libp2p/go-libp2p-xor)

## License

[MIT](LICENSE) © 2022 Breth Inc.

