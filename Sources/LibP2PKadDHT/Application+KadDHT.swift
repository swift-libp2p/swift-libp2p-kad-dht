//===----------------------------------------------------------------------===//
//
// This source file is part of the swift-libp2p open source project
//
// Copyright (c) 2022-2026 swift-libp2p project authors
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
                    bootstrapPeers: BootstrapPeerDiscovery.ipfsBootNodes,
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
        bootstrapPeers: [PeerInfo] = BootstrapPeerDiscovery.ipfsBootNodes,
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

    /// The shared KadDHT node.
    ///
    /// - Warning: Traps when no KadDHT node is installed. Prefer ``kadDHTIfAvailable`` anywhere the
    ///   code can run while the `Application` is tearing down.
    public var kadDHT: KadDHT.Node {
        guard let kad = self.kadDHTIfAvailable else {
            /// `Application.dht` hands out *empty* subsystem storage from the moment shutdown
            /// starts, so an absent service there means teardown, not a missing `use(.kadDHT)`.
            if self.application.isShuttingDown {
                fatalError(
                    "KadDHT accessed while the Application was shutting down, its subsystem storage is already gone. Use `app.dht.kadDHTIfAvailable` on paths that can run during teardown."
                )
            }
            fatalError(
                "KadDHT accessed without instantiating it first. Use app.dht.use(.kadDHT) to initialize a shared KadDHT instance."
            )
        }
        return kad
    }

    /// The shared KadDHT node, or `nil` when none is installed.
    ///
    /// Also `nil` once the `Application` starts shutting down, because ``Application/dht`` returns
    /// empty subsystem storage from that point on.
    public var kadDHTIfAvailable: KadDHT.Node? {
        self.service(for: KadDHT.Node.self)
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
                    bootstrapPeers: BootstrapPeerDiscovery.ipfsBootNodes,
                    configuration: .default
                )
                app.lifecycle.use(dht)
                app.dht.use { _ in dht }  // Does this work??
                return dht
            }
        }
    }
}
