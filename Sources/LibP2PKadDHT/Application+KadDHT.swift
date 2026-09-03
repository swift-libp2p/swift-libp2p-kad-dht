//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-libp2p open source project
//
// Copyright (c) 2022-2025 swift-libp2p project authors
// Licensed under MIT
//
// See LICENSE for license information
// See CONTRIBUTORS for the list of swift-libp2p project authors
//
// SPDX-License-Identifier: MIT
//
//===----------------------------------------------------------------------===//

import LibP2P

extension Application.DHTServices.Provider {

    /// Starts the KadDHT in client mode with the default (Amino) configuration
    public static var kadDHT: Self {
        .init {
            $0.dht.use { app -> KadDHT.Node in
                let dht = KadDHT.Node(
                    network: app,
                    mode: .client,
                    bootstrapPeers: BootstrapPeerDiscovery.IPFSBootNodes,
                    configuration: .default
                )
                app.lifecycle.use(dht)
                app.discovery.use { _ in dht }  // Does this work??
                return dht
            }
        }
    }

    /// Configures a KadDHT Node with the specified parameters
    public static func kadDHT(
        mode: KadDHT.Mode,
        configuration: KadDHT.Configuration = .default,
        bootstrapPeers: [PeerInfo] = BootstrapPeerDiscovery.IPFSBootNodes,
        autoUpdate: Bool = true
    ) -> Self {
        .init {
            $0.dht.use { app -> KadDHT.Node in
                let dht = KadDHT.Node(
                    network: app,
                    mode: mode,
                    bootstrapPeers: bootstrapPeers,
                    configuration: configuration
                )
                dht.autoUpdate = autoUpdate
                if case .server = mode {
                    let _ = dht.handle(namespace: "pk", validator: KadDHT.PubKeyValidator())
                    let _ = dht.handle(namespace: "ipns", validator: KadDHT.IPNSValidator())
                }
                app.lifecycle.use(dht)
                app.discovery.use { _ in dht }  // Does this work??
                return dht
            }
        }
    }
}

extension Application.DHTServices {

    public var kadDHT: KadDHT.Node {
        guard let kad = self.service(for: KadDHT.Node.self) else {
            fatalError(
                "KadDHT accessed without instantiating it first. Use app.dht.use(.kadDHT) to initialize a shared KadDHT instance."
            )
        }
        return kad
    }
}

/// KadDHT as a PeerDiscovery extension
extension Application.DiscoveryServices.Provider {
    /// Starts the KadDHT in client mode with options best fit for primary use as a Peer Discovery Service
    public static var kadDHT: Self {
        .init {
            $0.discovery.use { app -> KadDHT.Node in
                let dht = KadDHT.Node(
                    network: app,
                    mode: .client,
                    bootstrapPeers: BootstrapPeerDiscovery.IPFSBootNodes,
                    configuration: .default
                )
                app.lifecycle.use(dht)
                app.dht.use { _ in dht }  // Does this work??
                return dht
            }
        }
    }
}
