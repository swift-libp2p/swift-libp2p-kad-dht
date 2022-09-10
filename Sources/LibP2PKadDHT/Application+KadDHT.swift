//
//  Application+KadDHT.swift
//  
//
//  Created by Brandon Toms on 4/30/22.
//

import LibP2P

extension Application.DHTServices.Provider {
    
    /// Starts the KadDHT in client mode with default options
    public static var kadDHT: Self {
        .init {
            $0.dht.use { app -> KadDHT.Node in
                let dht = try! KadDHT.Node(network: app, mode: .client, bootstrapPeers: BootstrapPeerDiscovery.IPFSBootNodes, options: KadDHT.NodeOptions())
                app.lifecycle.use(dht)
                app.discovery.use { _ in dht } // Does this work??
                return dht
            }
        }
    }
    
    /// Configures a KadDHT Node with the specified parameters
    public static func kadDHT(mode: KadDHT.Mode, options: KadDHT.NodeOptions? = nil, bootstrapPeers:[PeerInfo] = BootstrapPeerDiscovery.IPFSBootNodes, autoUpdate:Bool = true) -> Self {
        .init {
            $0.dht.use { app -> KadDHT.Node in
                let dht = try! KadDHT.Node(network: app, mode: mode, bootstrapPeers: bootstrapPeers, options: options ?? KadDHT.NodeOptions())
                dht.autoUpdate = autoUpdate
                app.lifecycle.use(dht)
                app.discovery.use { _ in dht } // Does this work??
                return dht
            }
        }
    }
    
    /// Configures a KadDHT Node with the specified parameters
    public static func kadDHT(
        mode: KadDHT.Mode,
        connectionTimeout: TimeAmount,
        maxConcurrentConnections: Int,
        bucketSize: Int,
        maxPeers: Int,
        maxKeyValueStoreEntries:Int,
        autoUpdate:Bool = true,
        bootstrappedPeers:[PeerInfo] = BootstrapPeerDiscovery.IPFSBootNodes
    ) -> Self {
        .init {
            $0.dht.use { app -> KadDHT.Node in
                let dht = try! KadDHT.Node(
                    network: app,
                    mode: mode,
                    bootstrapPeers: bootstrappedPeers,
                    options: KadDHT.NodeOptions(
                        connectionTimeout: connectionTimeout,
                        maxConcurrentConnections: maxConcurrentConnections,
                        bucketSize: bucketSize,
                        maxPeers: maxPeers,
                        maxKeyValueStoreEntries: maxKeyValueStoreEntries
                    )
                )
                dht.autoUpdate = autoUpdate
                app.lifecycle.use(dht)
                app.discovery.use { _ in dht } // Does this work??
                return dht
            }
        }
    }
}

extension Application.DHTServices {
    
    public var kadDHT:KadDHT.Node {
        guard let kad = self.service(for: KadDHT.Node.self) else {
            fatalError("KadDHT accessed without instantiating it first. Use app.dht.use(.kadDHT) to initialize a shared KadDHT instance.")
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
                let dht = try! KadDHT.Node(network: app, mode: .client, bootstrapPeers: BootstrapPeerDiscovery.IPFSBootNodes, options: KadDHT.NodeOptions())
                app.lifecycle.use(dht)
                app.dht.use { _ in dht } // Does this work??
                return dht
            }
        }
    }
}
