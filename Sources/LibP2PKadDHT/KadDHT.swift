//
//  KadDHT.swift
//
//
//  Created by Brandon Toms on 11/12/21.
//

import LibP2P
//import CryptoKit
import CryptoSwift

public class KadDHT:EventLoopService {
    
    static let protocolID = "/kad/1.0.0"
    static let protocolIDAlt = "ipfs/kad/1.0.0"
    
    /// Configuration Params
    private let kBucketTag = "kbucket"
    private let protectedBuckets = 2
    private let baseConnectionManagerScore = 5
    
    public enum Mode {
        case client
        case server
    }
    
    /// The mode that the DHT is operating in.
    ///
    /// Client mode indicates that we can query the network, but cannot respond to queries
    /// Server mode indicates that we can both query the network and respone to queries
    public var mode:Mode {
        get { self._mode }
    }
    private var _mode:Mode
    
    /// The eventloop that our DHT service is constrained to
    public var eventLoop: EventLoop
    
    /// The services state
    public var state: ServiceLifecycleState {
        get { _state }
    }
    private var _state:ServiceLifecycleState
    
    private weak var host:Application?
    private let localPeerID:PeerID
    //private let localID:DHT.ID
    
    //private let peerstore:PeerStore
    //private let datastore:Datastore
    private let routingTable:RoutingTable
    /// ProviderStore stores & manages the provider records for this DHT peer
    //private let providerStore:ProviderStore
    //private let refreshManager:RefreshManager
    
    private let startedAt:Date
    //private let validator:

    /// DHT protocols we query with. We'll only add peers to our routing table if they speak these protocols
    private var protocols:[SemVerProtocol]
    private var protocolsString:[String]
    
    /// The DHT protocols we can respond to
    private var serverProtocols:[SemVerProtocol]
    
    private let bucketSize:Int
    /// The concurrency parameter per path
    private let alpha:Int
    /// The number of peers closest to a target that we must have responded for a query path to terminate
    private let beta:Int
    
    private let queryPeerFilter:Any?
    private let routingTablePeerFilter:RoutingTable.Filter?
    private let routingTableDiversityFilter:RoutingTable.DiversityFilter?
    
    private var autoRefresh:Bool
    
    /// A list of peers to help get the service up and running
    private let bootstrapPeers:[PeerInfo]
    
    /// Maximum age of a peer record before discarding it / treating it as expired...
    private let maxRecordAge:TimeAmount
    
    /// Allows disabling dht subsystems. These should _only_ be set on "forked" DHTs (e.g., DHTs with custom protocols and/or private networks).
    // private let enableProviders:Bool
    // private let enableValues:Bool
    // private let disableFixLowPeers:Bool
    // private let fixLowPeersChannel:Channel
    // private let addPeerToRoutingTableChannel:Channel
    // private let refreshFinishedChannel:Channel
    // private let routingTableFreezeTimeout:TimeAmount
    
    /// Configuration Variable for tests
    private let testAddressUpdateProcessing:Bool
    
    /// Our logger
    private var logger:Logger
    
    
    init(eventLoop:EventLoop, host:Application) {
        self.eventLoop = eventLoop
        self.logger = Logger(label: "KadDHT[\(UUID().uuidString.prefix(5))]")
        self.logger.logLevel = host.logger.logLevel
        self._mode = .client
        self._state = .stopped
        self.host = host
        self.localPeerID = host.peerID
        self.startedAt = Date()
        
        //self.protocols = host.registeredProtocols.compactMap { SemVerProtocol($0.protocolString()) }
        //self.protocolsString = host.registeredProtocols.compactMap { SemVerProtocol($0.protocolString())?.stringValue }
        //self.protocols = host.registeredProtocols.compactMap { $0.proto }
        //self.protocolsString = host.registeredProtocols.compactMap { $0.proto.stringValue }
        self.protocols = host.routes.registeredProtocols
        self.protocolsString = host.routes.registeredProtocols.compactMap { $0.stringValue }
        self.serverProtocols = self.protocols
        
        self.bucketSize = 20
        self.alpha = 1
        self.beta = 1
        self.autoRefresh = true
        
        self.queryPeerFilter = nil
        self.routingTablePeerFilter = nil
        self.routingTableDiversityFilter = nil
        
        self.bootstrapPeers = []
        
        self.maxRecordAge = .hours(24)
        
        self.testAddressUpdateProcessing = false
        
        self.routingTable = RoutingTable(eventloop: self.eventLoop, bucketSize: self.bucketSize, localPeerID: self.localPeerID, latency: .seconds(10), peerstoreMetrics: [:], usefulnessGracePeriod: .minutes(10))
    }
    
    public func start() throws {
        switch _state {
        case .stopped:
            // Lets start the DHT service...
            self.logger.info("TODO:Start our KadDHT Service")
        case .stopping:
            self.logger.warning("KadDHT is in the process of stopping, please wait for it to stop before starting it again")
        case .started:
            self.logger.warning("KadDHT has already been started")
        case .starting:
            self.logger.warning("KadDHT is in the process of starting, please wait for it to begin")
        }
    }
    
    public func stop() throws {
        switch _state {
        case .stopped:
            // Lets start the DHT service...
            self.logger.warning("KadDHT has already been stopped")
        case .stopping:
            self.logger.warning("KadDHT is in the process of stopping, please wait for it to stop")
        case .started, .starting:
            self.logger.warning("TODO: Stop the KadDHT Service")
        }
    }
    
    public func advertise(service: String, options: Options?) -> EventLoopFuture<TimeAmount> {
        self.eventLoop.makeFailedFuture(Errors.noNetwork)
    }
    
    public func findPeers(supportingService: String, options: Options?) -> EventLoopFuture<DiscoverdPeers> {
        self.eventLoop.makeFailedFuture(Errors.noNetwork)
    }
    
    public var onPeerDiscovered: ((PeerInfo) -> ())? = nil
    
    
}
