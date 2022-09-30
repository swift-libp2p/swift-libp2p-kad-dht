//
//  Application+KadDHT.swift
//  
//
//  Created by Brandon Toms on 4/30/22.
//

import LibP2P

public protocol Database {
    func put() -> Void
    func get() -> Void
}

extension DHT {
    public struct DHTConfiguration {
        /// ProviderStore sets the provider storage manager.
        let providerstore:Database
        /// Datastore configures the DHT to use the specified datastore.
        ///
        /// Defaults to an in-memory (temporary) map.
        let datastore:Database
        /// RoutingTableLatencyTolerance sets the maximum acceptable latency for peers in the routing table's cluster.
        let routingTableLatencyTolerance:TimeAmount
        /// RoutingTableRefreshQueryTimeout sets the timeout for routing table refresh queries.
        let routingTableRefreshTimeout:TimeAmount
        /// RoutingTableRefreshPeriod sets the period for refreshing buckets in the routing table. The DHT will refresh buckets every period by:
        ///
        ///  1. First searching for nearby peers to figure out how many buckets we should try to fill.
        ///  2. Then searching for a random key in each bucket that hasn't been queried in the last refresh period.
        let routingTableRefreshPeriod:TimeAmount
        
        /// Mode configures which mode the DHT operates in (Client, Server, Auto).
        ///
        /// Defaults to ModeAuto.
        let mode:KadDHT.Mode
        
        let namespaces:[Validator]
        
        /// ProtocolPrefix sets an application specific prefix to be attached to all DHT protocols. For example,
        /// /myapp/kad/1.0.0 instead of /ipfs/kad/1.0.0. Prefix should be of the form /myapp.
        ///
        /// Defaults to dht.DefaultPrefix
        let protocolPrefix:String
        
        /// ProtocolExtension adds an application specific protocol to the DHT protocol. For example,
        /// /ipfs/lan/kad/1.0.0 instead of /ipfs/kad/1.0.0. extension should be of the form /lan.
        let protocolExtension:String
        
        /// BucketSize configures the bucket size (k in the Kademlia paper) of the routing table.
        ///
        /// The default value is 20.
        let bucketSize:Int
        
        /// Concurrency configures the number of concurrent requests (alpha in the Kademlia paper) for a given query path.
        ///
        /// The default value is 10.
        let concurrency:Int
        
        /// Resiliency configures the number of peers closest to a target that must have responded in order for a given query path to complete.
        ///
        /// The default value is 3.
        let resiliency:Int
        
        /// MaxRecordAge specifies the maximum time that any node will hold onto a record ("PutValue record") from the time its received.
        /// This does not apply to any other forms of validity that the record may contain.
        /// For example, a record may contain an ipns entry with an EOL saying its valid until the year 2020 (a great time in the future).
        /// For that record to stick around it must be rebroadcasted more frequently than once every 'MaxRecordAge'
        let maxRecordAge:TimeAmount
        
        /// DisableAutoRefresh completely disables 'auto-refresh' on the DHT routing table.
        /// This means that we will neither refresh the routing table periodically nor when the routing table size goes below the minimum threshold.
        let disableAutoRefresh:Bool
        
        /// DisableProviders disables storing and retrieving provider records.
        ///
        /// Defaults to enabled.
        ///
        /// - WARNING: Do not change this unless you're using a forked DHT (i.e., a private network and/or distinct DHT protocols with the `Protocols` option).
        let disableProviders:Bool
        
        /// DisableValues disables storing and retrieving value records (including public keys).
        ///
        /// Defaults to enabled.
        ///
        /// - WARNING: Do not change this unless you're using a forked DHT (i.e., a private network and/or distinct DHT protocols with the `Protocols` option).
        let disableValues:Bool
        
        /// QueryFilter sets a function that approves which peers may be dialed in a query
        let queryFilter:([PeerInfo]) -> [PeerInfo]
        
        /// RoutingTableFilter sets a function that approves which peers may be added to the routing table.
        /// - Note: The host shouldalready have at least one connection to the peer under consideration.
        let routingTableFilter:(PeerInfo) -> Bool
        
        /// BootstrapPeers configures the bootstrapping nodes that we will connect to to seed and refresh our Routing Table if it becomes empty.
        let bootstrapPeers:[PeerInfo]
        
        /// BootstrapPeersFunc configures the function that returns the bootstrapping nodes that we will connect to to seed and refresh our Routing Table if it becomes empty.
        let bootstrapPeersFunction:() -> [PeerInfo]
        
        let routingTablePeerDiversityFilter:RoutingTable.DiversityFilter
        
        /// disableFixLowPeersRoutine disables the "fixLowPeers" routine in the DHT.
        /// - Note: This is ONLY for tests
        let disableFixLowPeersRoutine:Bool
        
        /// forceAddressUpdateProcessing forces the DHT to handle changes to the hosts addresses.
        /// - Note: This occurs even when AutoRefresh has been disabled.
        /// - Note: This is ONLY for tests.
        let forceAddressUpdateProcessing:Bool
    }
}

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
                if case .server = mode {
                    let _ = dht.handle(namespace: "pk", validator: DHT.PubKeyValidator())
                    //dht.handle(namespace: "ipns", validator:)
                }
                app.lifecycle.use(dht)
                app.discovery.use { _ in dht } // Does this work??
                return dht
            }
        }
    }
    
    /// Configures a KadDHT Node with the specified parameters
    ///
    /// - Note: Do we pass in out namespaces and validators here?
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
                if case .server = mode {
                    let _ = dht.handle(namespace: "pk", validator: DHT.PubKeyValidator())
                    //dht.handle(namespace: "ipns", validator:)
                }
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
