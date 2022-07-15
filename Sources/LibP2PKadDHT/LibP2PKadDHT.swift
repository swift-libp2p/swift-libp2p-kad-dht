//
//  KadDHT.swift
//
//
//  Created by Brandon Toms on 11/12/21.
//

import LibP2P
//import CryptoKit
import CryptoSwift

struct DHTPeerInfo:Equatable {
    /// The original PeerID of this DHT Peer
    let id:PeerID
    
    /// LastUsefulAt is the time instant at which the peer was last "useful" to us.
    ///
    /// Please see the DHT docs for the definition of usefulness.
    var lastUsefulAt:TimeInterval?
    
    /// LastSuccessfulOutboundQueryAt is the time instant at which we last got a successful query response from the peer.
    var lastSuccessfulOutboundQueryAt:TimeInterval?
    
    /// AddedAt is the time this peer was added to the routing table.
    let addedAt:TimeInterval
    
    /// The ID of the peer in the DHT XOR keyspace (XOR(PeerID))
    let dhtID:KadDHT.Key
    
    /// If a bucket is full, this peer can be replaced to make space for a new peer.
    var replaceable:Bool
}

public class KadDHT:EventLoopService {
    struct Key:Equatable, Hashable {
        /// - TODO: Move Distance calculations onto KeySpace
        enum KeySpace {
            case xor
        }
        
        let keySpace:KeySpace
        let original:[UInt8]
        let bytes:[UInt8]
        
        init(_ peer:PeerID, keySpace:KeySpace = .xor) {
            self.init(peer.id, keySpace: keySpace)
        }
        
        init(_ bytes:[UInt8], keySpace:KeySpace = .xor) {
            self.keySpace = keySpace
            /// Store the original ID
            self.original = bytes
            /// Hash the ID for DHT Key conformance
            //let sha = SHA256()
            //try! sha.update(withBytes: bytes)
            //self.bytes = sha.finalize().toBytes
            self.bytes = SHA2(variant: .sha256).calculate(for: bytes)
        }
        
        /// Used for testing purposes only
        internal init(preHashedBytes:[UInt8], keySpace:KeySpace = .xor) {
            self.keySpace = keySpace
            self.original = []
            self.bytes = preHashedBytes
        }
        
        /// Creates a key of specified length with a predictable prefix, used for testing purposes
        internal init(prefix:[UInt8], length:Int = 20, keySpace:KeySpace = .xor) {
            self.keySpace = keySpace
            self.original = []
            var prehashedBytes = Array<UInt8>(repeating: 0, count: length)
            for i in 0..<prefix.count {
                prehashedBytes[i] = prefix[i]
            }
            self.bytes = prehashedBytes
        }
    }
    
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

extension KadDHT.Key: Comparable {
    /// Measures the distance between two keys
    internal static func distanceBetween(k0:KadDHT.Key, k1:KadDHT.Key) -> [UInt8] {
        let k0Bytes = k0.bytes
        let k1Bytes = k1.bytes
        guard k0Bytes.count == k1Bytes.count else { print("Error: Keys must be the same length"); return [] }
        return k0Bytes.enumerated().map { idx, byte in
            k1Bytes[idx] ^ byte
        }
    }
    
    /// Measures the distance between us and the specified key
    internal func distanceTo(key:KadDHT.Key) -> [UInt8] {
        return KadDHT.Key.distanceBetween(k0: self, k1: key)
    }
    
    /// Compares the distances between two peers/keys from a certain peer/key
    ///
    /// Returns  1 if the first key is closer
    /// Returns -1 if the second key is closer
    /// Returns  0 if the keys are the same distance apart (aka equal)
    internal static func compareDistances(from:KadDHT.Key, to key1:KadDHT.Key, and key2:KadDHT.Key) -> Int8 {
        let p0Bytes = from.bytes
        let p1Bytes = key1.bytes
        let p2Bytes = key2.bytes
        guard p0Bytes.count == p1Bytes.count, p0Bytes.count == p2Bytes.count else { print("Error: Keys must be the same length"); return 0 }
        for (idx, byte) in p0Bytes.enumerated() {
            let bit1 = p1Bytes[idx] ^ byte
            let bit2 = p2Bytes[idx] ^ byte
            if bit1 > bit2 { return -1 }
            if bit1 < bit2 { return  1 }
        }
        return 0
    }
    
//    enum ComparativeDistance:Int8 {
//        case firstKeyCloser  =  1
//        case secondKeyCloser = -1
//        case equal           =  0
//    }
//
    /// Determines which of the two keys is closer to the this key
    internal func compareDistancesFromSelf(to key1:KadDHT.Key, and key2:KadDHT.Key) -> Int8 {
        return KadDHT.Key.compareDistances(from: self, to: key1, and: key2)
    }
    
    static let ZeroKey = {
        KadDHT.Key(preHashedBytes: Array<UInt8>(repeating: 0, count: 32))
    }()
    
    static func < (lhs: KadDHT.Key, rhs: KadDHT.Key) -> Bool {
        return ZeroKey.compareDistancesFromSelf(to: lhs, and: rhs) > 0
    }
    
    /// Returns the hex string representation of the keys underlying bytes
    func toHex() -> String { self.bytes.toHexString() }
    /// Returns the hex string representation of the keys underlying bytes
    func toString() -> String { self.toHex() }
    /// Returns the hex string representation of the keys underlying bytes
    func toBinary() -> String { self.bytes.asString(base: .base2) }
}

extension PeerID {
    /// Measures the distance between two peers
    internal static func distanceBetween(p0:PeerID, p1:PeerID, using keyspace:KadDHT.Key.KeySpace = .xor) -> [UInt8] {
        return KadDHT.Key.distanceBetween(k0: KadDHT.Key(p0, keySpace: keyspace), k1: KadDHT.Key(p1, keySpace: keyspace))
    }
    
    /// Measures the distance between us and the specified peer
    internal func distanceTo(peer:PeerID, using keyspace:KadDHT.Key.KeySpace = .xor) -> [UInt8] {
        return KadDHT.Key.distanceBetween(k0: KadDHT.Key(self, keySpace: keyspace), k1: KadDHT.Key(peer, keySpace: keyspace))
    }
    
    /// Compares the distances between two peers from a certain peer
    ///
    /// Returns  1 if the first peer is closer
    /// Returns -1 if the second peer is closer
    /// Returns  0 if the peers are the same distance apart
    internal static func compareDistances(from:PeerID, to key1:PeerID, and key2:PeerID, using keyspace:KadDHT.Key.KeySpace = .xor) -> Int8 {
        return KadDHT.Key.compareDistances(from: KadDHT.Key(from, keySpace: keyspace), to: KadDHT.Key(key1, keySpace: keyspace), and: KadDHT.Key(key2, keySpace: keyspace))
    }
    
    /// Compares the distances between two peers from us
    internal func compareDistancesFromSelf(to key1:PeerID, and key2:PeerID, using keyspace:KadDHT.Key.KeySpace = .xor) -> Int8 {
        return KadDHT.Key.compareDistances(from: KadDHT.Key(self, keySpace: keyspace), to: KadDHT.Key(key1, keySpace: keyspace), and: KadDHT.Key(key2, keySpace: keyspace))
    }
    
    internal func absoluteDistance() -> [UInt8] {
        KadDHT.Key(self).bytes
    }
    
}

extension Array where Element == KadDHT.Key {
    /// Sorts an array of `KadDHT.Keys`, in place, based on their `Distance` from the specified key, ordered closest to furthest.
    /// - Note: The notion of `Distance` is derived using the specified `KeySpace` which defaults to Kademlia XOR.
    mutating func sort(byDistanceToKey target:KadDHT.Key) {
        self.sort { lhs, rhs in
            let comp = target.compareDistancesFromSelf(to: lhs, and: rhs)
            return comp > 0
        }
    }
    
    /// Returns an array of sorted `KadDHT.Keys` based on their `Distance` from the specified key, ordered closest to furthest.
    /// - Note: The notion of `Distance` is derived using the specified `KeySpace` which defaults to Kademlia XOR.
    func sorted(byDistanceToKey target:KadDHT.Key) -> [KadDHT.Key] {
        self.sorted { lhs, rhs in
            let comp = target.compareDistancesFromSelf(to: lhs, and: rhs)
            return comp > 0
        }
    }
}

extension Array where Element == PeerID {
    /// Sorts an array of `PeerID`s, in place, based on their `Distance` from the specified peer, ordered closest to furthest.
    /// - Note: The notion of `Distance` is derived using the specified `KeySpace` which defaults to Kademlia XOR.
    /// - Note: This is an expensive, unoptimized, sorting method, it performs redundant SHA256 hashes of our PeerIDs for comparison sake.
    mutating func sort(byDistanceToPeer target:PeerID, usingKeySpace:KadDHT.Key.KeySpace = .xor) {
        let targetKey = KadDHT.Key(target, keySpace: usingKeySpace)
        self.sort { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(to: KadDHT.Key(lhs, keySpace: usingKeySpace), and: KadDHT.Key(rhs, keySpace: usingKeySpace))
            return comp > 0
        }
    }
    
    /// Returns an array of sorted `PeerID`s based on their `Distance` from the specified peer, ordered closest to furthest.
    /// - Note: The notion of `Distance` is derived using the specified `KeySpace` which defaults to Kademlia XOR.
    /// - Note: This is an expensive, unoptimized, sorting method, it performs redundant SHA256 hashes of our PeerIDs for comparison sake.
    func sorted(byDistanceToPeer target:PeerID, usingKeySpace:KadDHT.Key.KeySpace = .xor) -> [PeerID] {
        let targetKey = KadDHT.Key(target, keySpace: usingKeySpace)
        return self.sorted { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(to: KadDHT.Key(lhs, keySpace: usingKeySpace), and: KadDHT.Key(rhs, keySpace: usingKeySpace))
            return comp > 0
        }
    }
    
    /// Returns an array of sorted `PeerID`s based on their `Distance` from the specified peer, ordered closest to furthest.
    /// - Note: The notion of `Distance` is derived using the specified `KeySpace` which defaults to Kademlia XOR.
    /// - Note: This is an expensive, unoptimized, sorting method, it performs redundant SHA256 hashes of our PeerIDs for comparison sake.
    func sortedByAbsoluteDistance(usingKeySpace:KadDHT.Key.KeySpace = .xor) -> [PeerID] {
        let targetKey = KadDHT.Key(preHashedBytes: Array<UInt8>(repeating: 0, count: 32))
        return self.sorted { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(to: KadDHT.Key(lhs, keySpace: usingKeySpace), and: KadDHT.Key(rhs, keySpace: usingKeySpace))
            return comp > 0
        }
    }
    
    
}

extension Array where Element == DHTPeerInfo {
    func sortedAbsolutely(using keyspace:KadDHT.Key.KeySpace = .xor) -> [DHTPeerInfo] {
        let targetKey = KadDHT.Key(preHashedBytes: Array<UInt8>(repeating: 0, count: 32))
        return self.sorted { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(to: lhs.dhtID, and: rhs.dhtID)
            return comp > 0
        }
    }
}

extension Array where Element == PeerID {
    func sortedAbsolutely(using keyspace:KadDHT.Key.KeySpace = .xor) -> [PeerID] {
        let targetKey = KadDHT.Key(preHashedBytes: Array<UInt8>(repeating: 0, count: 32))
        return self.sorted { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(to: KadDHT.Key(lhs, keySpace: keyspace), and: KadDHT.Key(rhs, keySpace: keyspace))
            return comp > 0
        }
    }
    
    func sorted(closestTo:PeerID, using keyspace:KadDHT.Key.KeySpace = .xor) -> [PeerID] {
        let targetKey = KadDHT.Key(closestTo, keySpace: keyspace)
        return self.sorted { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(to: KadDHT.Key(lhs, keySpace: keyspace), and: KadDHT.Key(rhs, keySpace: keyspace))
            return comp > 0
        }
    }
}

extension Array where Element == KadDHT.Node {
    func sortedAbsolutely(using keyspace:KadDHT.Key.KeySpace = .xor) -> [KadDHT.Node] {
        let targetKey = KadDHT.Key(preHashedBytes: Array<UInt8>(repeating: 0, count: 32))
        return self.sorted { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(to: KadDHT.Key(lhs.peerID, keySpace: keyspace), and: KadDHT.Key(rhs.peerID, keySpace: keyspace))
            return comp > 0
        }
    }
    
    func sorted(closestTo:KadDHT.Node, using keyspace:KadDHT.Key.KeySpace = .xor) -> [KadDHT.Node] {
        let targetKey = KadDHT.Key(closestTo.peerID, keySpace: keyspace)
        return self.sorted { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(to: KadDHT.Key(lhs.peerID, keySpace: keyspace), and: KadDHT.Key(rhs.peerID, keySpace: keyspace))
            return comp > 0
        }
    }
}


extension Array where Element == UInt8 {
    func zeroPrefixLength() -> Int {
        var zeros = 0
        for byte in self {
            zeros += byte.leadingZeroBitCount
            if byte.leadingZeroBitCount != 8 { break }
        }
        return zeros
    }
}

extension PeerID {
    internal func commonPrefixLength(with peer:PeerID) -> Int {
        return KadDHT.Key(self).bytes.commonPrefixLength(with: KadDHT.Key(peer).bytes)
    }
}

extension KadDHT.Key {
    internal func commonPrefixLength(with peer:KadDHT.Key) -> Int {
        return self.bytes.commonPrefixLength(with: peer.bytes)
    }
}






