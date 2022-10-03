//
//  Node.swift
//  KademliaDHT
//
//  Created by Brandon Toms on 4/29/22.
//


// Started at 6:03 and 3.1% mem
// 30 minutes later 4.1% mem
// 60 minutes later 4.7% mem
// 80 minutes later 5.1% mem

// Started at 10:35am
import LibP2P
import CID
import Multihash

public protocol Validator {
    func validate(key:[UInt8], value:[UInt8]) throws
    func select(key:[UInt8], values:[[UInt8]]) throws -> Int
}

extension KadDHT {
    static public func createPubKeyRecord(peerID:PeerID) throws -> DHTRecord {
        let df = ISO8601DateFormatter()
        df.formatOptions.insert(.withFractionalSeconds)
        
        let key = "/pk/".bytes + peerID.id
        let record = try DHT.Record.with { rec in
            rec.key = Data(key)
            rec.value = try Data(peerID.marshalPublicKey())
            rec.timeReceived = df.string(from: Date())
        }
        
        return record
    }
}

extension DHT {
    struct BaseValidator:Validator {
        let validateFunction:((_ key:[UInt8], _ value:[UInt8]) throws -> Void)
        let selectFunction:((_ key:[UInt8], _ values:[[UInt8]]) throws -> Int)
        
        init(validationFunction:@escaping (_ key:[UInt8], _ value:[UInt8]) throws -> Void, selectFunction:@escaping (_ key:[UInt8], _ values:[[UInt8]]) throws -> Int) {
            self.validateFunction = validationFunction
            self.selectFunction = selectFunction
        }
        
        func validate(key:[UInt8], value:[UInt8]) throws {
            return try self.validateFunction(key, value)
        }
        
        func select(key:[UInt8], values:[[UInt8]]) throws -> Int {
            return try selectFunction(key, values)
        }
    }
    
    struct PubKeyValidator:Validator {
        func validate(key: [UInt8], value: [UInt8]) throws {
            let record = try DHT.Record(contiguousBytes: value)
            guard Data(key) == record.key else { throw NSError(domain: "Validator::Key Mismatch. Expected \(Data(key)) got \(record.key) ", code: 0) }
            let _ = try PeerID(marshaledPublicKey: Data(record.value))
        }
        
        func select(key: [UInt8], values: [[UInt8]]) throws -> Int {
            let records = values.map { try? DHT.Record(contiguousBytes: $0) }
            guard !records.compactMap({ $0 }).isEmpty else { throw NSError(domain: "Validator::No Records to select", code: 0) }
            guard records.count > 1 && records[0] != nil else { return 0 }

            var bestValueIndex:Int = 0
            var bestValue:DHT.Record? = nil
            for (index, record) in records.enumerated() {
                guard let record = record else { continue }
                guard let newRecord = try? RFC3339Date(string: record.timeReceived) else { continue }
                
                if let currentBest = bestValue {
                    guard let currentRecord = try? RFC3339Date(string: currentBest.timeReceived) else { continue }
                    // If this record is more recent then our currentBest, update our current best!
                    if newRecord > currentRecord {
                        bestValue = record
                        bestValueIndex = index
                    }
                } else {
                    // If we don't have a current best, set it!
                    bestValue = record
                    bestValueIndex = index
                }
            }
            
            guard bestValue != nil else { throw NSError(domain: "Validator::Failed to select a valid record", code: 0) }
            return bestValueIndex
        }
    }
    
    struct IPNSValidator:Validator {
        func validate(key: [UInt8], value: [UInt8]) throws {
            let record = try DHT.Record(contiguousBytes: value)
            guard Data(key) == record.key else { throw NSError(domain: "Validator::Key Mismatch. Expected \(Data(key)) got \(record.key) ", code: 0) }
            let _ = try IpnsEntry(contiguousBytes: record.value)
        }
        
        func select(key: [UInt8], values: [[UInt8]]) throws -> Int {
            let records = values.map { try? DHT.Record(contiguousBytes: $0) }
            guard !records.compactMap({ $0 }).isEmpty else { throw NSError(domain: "Validator::No Records to select", code: 0) }
            guard records.count > 1 && records[0] != nil else { return 0 }
            
            var bestValueIndex:Int = 0
            var bestValue:DHT.Record? = nil
            for (index, record) in records.enumerated() {
                guard let record = record else { continue }
                guard let newRecord = try? RFC3339Date(string: record.timeReceived) else { continue }
                
                if let currentBest = bestValue {
                    guard let currentRecord = try? RFC3339Date(string: currentBest.timeReceived) else { continue }
                    // If this record is more recent then our currentBest, update our current best!
                    if newRecord > currentRecord {
                        bestValue = record
                        bestValueIndex = index
                    }
                } else {
                    // If we don't have a current best, set it!
                    bestValue = record
                    bestValueIndex = index
                }
            }
            
            guard bestValue != nil else { throw NSError(domain: "Validator::Failed to select a valid record", code: 0) }
            return bestValueIndex
        }
    }
}

/// If we abstract the Application into a Network protocol then we can create a FauxNetwork for testing purposes...
protocol Network {
    var logger:Logger { get }
    var eventLoopGroup:EventLoopGroup { get }
    var peerID:PeerID { get }
    var listenAddresses:[Multiaddr] { get }
    var peers:PeerStore { get }
    func registerProtocol(_ proto:SemVerProtocol) throws
    func dialableAddress(_ mas:[Multiaddr], externalAddressesOnly:Bool, on: EventLoop) -> EventLoopFuture<[Multiaddr]>
    func newRequest(to ma:Multiaddr, forProtocol proto:String, withRequest request:Data, style:Application.SingleRequest.Style, withHandlers handlers:HandlerConfig, andMiddleware middleware:MiddlewareConfig, withTimeout timeout:TimeAmount) -> EventLoopFuture<Data>
}

//extension Application: Network { }

extension DHT {
    /// This method attempts to take a key in the form of bytes and convert it into a human readable "/<namespace>/<multihash>" string for debugging
    /// - Parameter key: The key in bytes that you'd like to log
    /// - Returns: The most human readable string we can make
    static func keyToHumanReadableString(_ key:[UInt8]) -> String {
        if let namespaceBytes = DHT.extractNamespace(key), let namespace = String(data: Data(namespaceBytes), encoding: .utf8) {
            if let mh = try? Multihash(Array(key.dropFirst(namespace.count + 2))) {
                return "/\(namespace)/\(mh.b58String)"
            } else if let cid = try? CID(Array(key.dropFirst(namespace.count + 2))) {
                return "/\(namespace)/\(cid.multihash.b58String)"
            } else {
                return "/\(namespace)/\(key.dropFirst(namespaceBytes.count + 2))"
            }
        } else {
            if let mh = try? Multihash(key) {
                return "\(mh.b58String)"
            } else if let cid = try? CID(key) {
                return "\(cid.multihash.b58String)"
            } else {
                return "\(key)"
            }
        }
    }
    
    /// This method attempts to extract a namespace prefixed key of the form "/namespace/<multihash>"
    /// - Parameter key: The key to extract the prefixed namespace from
    /// - Returns: The namespace bytes if they exist (excluding the forward slashes), or nil if the key isn't prefixed with a namespace
    /// - Note: "/" in utf8 == 47
    static func extractNamespace(_ key:[UInt8]) -> [UInt8]? {
        guard key.first == UInt8(47) else { return nil }
        guard let idx = key.dropFirst().firstIndex(of: UInt8(47)) else { return nil }
        return Array(key[1..<idx])
    }
}

//extension DHT.Record:CustomStringConvertible {
//    public var description: String {
//        let header = "--- 📒 DHT Record 📒 ---"
//        return """
//            \(header)
//            Key: \(self.key)
//            Value: \(self.value)
//            Time Received: \(self.timeReceived)
//            \(String(repeating: "-", count: header.count + 2))
//            """
//    }
//}
//
//extension IpnsEntry:CustomStringConvertible {
//    public var description: String {
//        let header = "--- 🌎 IPNS Record 🌎 ---"
//        return """
//            \(header)
//            Bytes: \(self.value.asString(base: .base16))
//            Signature<V1>: \(self.signatureV1.asString(base: .base16))
//            Validity<EOL>: \(self.validity.asString(base: .base16))
//            Sequence: \(self.sequence)
//            TTL: \(self.ttl)
//            \(String(repeating: "-", count: header.count + 2))
//            """
//    }
//}

extension KadDHT {
    public static var multicodec:String = "/ipfs/kad/1.0.0"
    public class Node:DHTCore, EventLoopService, LifecycleHandler, PeerRouting {
        public static var key: String = "KadDHT"
        
        /// This is why there is a "ipfs/lan/kad/1.0.0" protocol...
        let isRunningLocally:Bool = true
        
        enum State {
            case started
            case stopped
        }
        
//        enum Mode {
//            case client
//            case server
//        }
        
        /// A weak reference back to our main LibP2P instance
        weak var network:Application?
        
        /// Wether the DHT is operating in Client or Server mode
        ///
        /// Nodes operating in server mode advertise the libp2p Kademlia protocol identifier via the identify protocol.
        /// In addition server mode nodes accept incoming streams using the Kademlia protocol identifier.
        /// Nodes operating in client mode do not advertise support for the libp2p Kademlia protocol identifier.
        /// In addition they do not offer the Kademlia protocol identifier for incoming streams.
        let mode:KadDHT.Mode
        
        /// Fake Internet Connection Type
        //let connection:InternetType
            
        /// Max number of concurrent requests we can have open at any moment
        let maxConcurrentRequest:Int
        
        /// Max Connection Timeout
        let connectionTimeout:TimeAmount
        
        /// DHT Key:Value Store
        let dhtSize:Int
        var dht:[KadDHT.Key:DHT.Record] = [:]
        
        /// DHT Peer Store
        let routingTable:RoutingTable
        let maxPeers:Int
        
        /// Naive DHT Provider Store
        var providerStore:[KadDHT.Key:[DHT.Message.Peer]] = [:]
        
        /// The event loop that we're operating on...
        public let eventLoop:EventLoop
        
        /// Our nodes multiaddress
        var address:Multiaddr!
            
        /// Our nodes PeerID
        let peerID:PeerID
        
        /// Our Nodes Event History
        var metrics:NodeMetrics
        
        /// Our Logger
        var logger:Logger
        
        /// Known Peers
        //var peerstore:[String:PeerInfo] = [:]
        
        /// Wether the node should start a timer that triggers the heartbeat method, or if it should wait for an external service to call the heartbeat method explicitly
        public var autoUpdate:Bool
        
        var replacementStrategy:RoutingTable.ReplacementStrategy = .furtherThanReplacement
        
        private var heartbeatTask:RepeatedTask? = nil
        
        public private(set) var state:ServiceLifecycleState = .stopped
        
        private var maxLookupDepth:Int = 5
                
        private var firstSearch:Bool = true
        
        private var handler:LibP2P.ProtocolHandler?
        
        private var isRunningHeartbeat:Bool = false
        
        //private var defaultValidator:Validator = Validator(validate: { return true }, select: { return 0 } )
        /// [Namespace:Validator]
        private var validators:[[UInt8]:Validator] = [:]
        
        init(eventLoop:EventLoop, network:Application, mode:KadDHT.Mode, peerID:PeerID, bootstrapedPeers:[PeerInfo], options:NodeOptions) {
            self.eventLoop = eventLoop
            self.network = network
            self.mode = mode
            self.peerID = peerID
            //self.connection = options.connection
            self.maxConcurrentRequest = options.maxConcurrentConnections
            self.connectionTimeout = options.connectionTimeout
            self.dhtSize = options.maxKeyValueStoreSize
            self.maxPeers = options.maxPeers
            self.routingTable = RoutingTable(eventloop: eventLoop, bucketSize: options.bucketSize, localPeerID: peerID, latency: options.connectionTimeout, peerstoreMetrics: [:], usefulnessGracePeriod: .minutes(5))
            self.logger = Logger(label: "DHTNode\(peerID)")
            self.logger.logLevel = network.logger.logLevel
            self.metrics = NodeMetrics()
            self.state = .stopped
            self.autoUpdate = true
            
            /// Add our initialized event
            self.metrics.add(event: .initialized)
            
            /// Add the bootstrapped peers to our routing table
            bootstrapedPeers.compactMap { pInfo -> EventLoopFuture<Bool> in
//                guard let pid = self.multiaddressToPeerID(ma) else { return self.eventLoop.makeSucceededFuture( false ) }
//                let pInfo = PeerInfo(peer: pid, addresses: [ma])
                self.metrics.add(event: .peerDiscovered(pInfo))
                return self.routingTable.addPeer( pInfo.peer ).always { result in
                    switch result {
                    case .success(let didAddPeer):
                        if didAddPeer {
                            self.metrics.add(event: .addedPeer(pInfo))
                            let _ = self.network?.peers.add(peerInfo: pInfo)
                            //self.peerstore[pInfo.peer.b58String] = pInfo
                        } else {
                            self.metrics.add(event: .droppedPeer(pInfo, .failedToAdd))
                        }
                    case .failure:
                        self.metrics.add(event: .droppedPeer(pInfo, .failedToAdd))
                    }
                }
            }.flatten(on: self.eventLoop).whenComplete { res in
                switch res {
                case .success(let bools):
                    if bools.contains(false) {
                        self.logger.warning("Failed to add \(bools.filter({ !$0 }).count) of \(bools.count) bootstrap peers")
                    } else {
                        self.logger.info("Added \(bools.count) bootstrap peers")
                    }
                case .failure(let error):
                    self.logger.error("Failed to add bootstrapped peers: \(error)")
                }
            }
            
            if case .server = mode {
                self.logger.info("Registering KadDHT endpoint for opperation as Server")
                /// register the `/ipfs/kad/1.0.0` endpoint
                try! registerDHTRoute(self.network!)
            } else {
                self.logger.info("Operating in Client Only Mode")
            }
            
        }
        
        convenience init(network:Application, mode:KadDHT.Mode, bootstrapPeers:[PeerInfo], options:NodeOptions) throws {
            self.init(eventLoop: network.eventLoopGroup.next(), network: network, mode: mode, peerID: network.peerID, bootstrapedPeers: bootstrapPeers, options: options)
        }
        
        public func didBoot(_ application: Application) throws {
            try self.start()
        }
        
        public func shutdown(_ application: Application) {
            self.stop()
        }
        
        public func start() throws {
            guard self.state == .stopped else { self.logger.warning("Already Started"); return }
            
            guard let addy = network!.listenAddresses.first else { throw Errors.noNetwork }
            self.address = addy.getPeerID() != nil ? addy : try! addy.encapsulate(proto: .p2p, address: peerID.b58String)
            self.state = .starting
            
            /// Alert our app of the bootstrapped peers...
//            for (_, pInfo) in self.peerstore {
//                self.onPeerDiscovered?(pInfo)
//            }
            if let opd = self.onPeerDiscovered {
                let _ = self.network?.peers.all().map { peers in
                    peers.forEach {
                        opd(PeerInfo(peer: $0.id, addresses: $0.addresses))
                    }
                }
            }
            
            /// Set up the heartbeat task
            if self.autoUpdate == true {
                self.heartbeatTask = self.eventLoop.scheduleRepeatedAsyncTask(initialDelay: .milliseconds(500), delay: .seconds(120), notifying: nil, self._heartbeat)
            }
            
            self.state = .started
            
            self.logger.info("Started")
        }
        
        public func findPeer(peer: PeerID) -> EventLoopFuture<PeerInfo> {
            self.eventLoop.makeFailedFuture(Errors.noNetwork)
        }
        
        public func heartbeat() -> EventLoopFuture<Void> {
            guard self.autoUpdate == false else { return self.eventLoop.makeFailedFuture(Errors.cannotCallHeartbeatWhileNodeIsInAutoUpdateMode) }
            return self._heartbeat()
        }
        
        private func _heartbeat(_ task:RepeatedTask? = nil) -> EventLoopFuture<Void> {
            guard self.isRunningHeartbeat == false else { return self.eventLoop.makeSucceededVoidFuture() }
            return self.eventLoop.flatSubmit {
                self.logger.notice("Running Heartbeat")
                self.isRunningHeartbeat = true
                return self.network!.peers.all().flatMap { peers in
                    self.logger.notice("DHT Keys<\(self.dht.keys.count)> [ \n\(self.dht.keys.map { "\($0)" }.joined(separator: ",\n"))]")
                    self.logger.notice("\(self.routingTable.description)")
                    self.logger.notice("PeerStore<\(peers.count)> [ \n\(peers.map { "\($0.id.b58String)" }.joined(separator: ",\n"))]")
                    return self._shareDHTKVs().flatMap {
                        //self._shareProviderRecords().flatMap {
                            self._searchForPeersLookupStyle()
                        //}
                    }
                }.always { _ in
                    self.logger.notice("Heartbeat Finished")
                    self.isRunningHeartbeat = false
                }
            }
          
            // /// Search for additional peers
            // return self._searchForPeersLookupStyle().and(
            // /// Share our DHT Keys
            // self._shareDHTKVs()
            // ).transform(to: ())
        }
        
        public func advertise(service: String, options: Options?) -> EventLoopFuture<TimeAmount> {
            self.eventLoop.makeFailedFuture(Errors.noNetwork)
        }
        
        public func findPeers(supportingService: String, options: Options?) -> EventLoopFuture<DiscoverdPeers> {
            self.eventLoop.makeFailedFuture(Errors.noNetwork)
        }
        
        public var onPeerDiscovered: ((PeerInfo) -> ())? = nil
        
        /// Handles a new namespace via the provided validator.
        /// - Parameters:
        ///   - namespace: The namespace prefix for the DHT KV pair
        ///   - validator: The validator that ensures the Value being stored is valid and the most desirable
        /// - Returns: Void upon succes, error upon failure.
        public func handle(namespace:String, validator:Validator) -> EventLoopFuture<Void> {
            self.eventLoop.submit {
                if self.validators.updateValue(validator, forKey: namespace.bytes) != nil {
                    self.logger.warning("Overriding Validator for Namesapce: \(namespace)")
                }
            }
        }
        
        /// Removes the Validator bound to the specified namespace
        /// - Parameter namespace: The namespace whos validator should be removed
        /// - Returns: `true` if there was a validator to remove, `false` otherwise.
        /// - Note: Should we remove all stored DHT keys for this namespace? Or just let them expire?
        public func removeValidator(forNamespace namespace: String) -> EventLoopFuture<Bool> {
            self.eventLoop.submit {
                if self.validators.removeValue(forKey: namespace.bytes) != nil {
                    return true
                } else {
                    return false
                }
            }
        }
        
        func processRequest(_ req:Request) -> EventLoopFuture<LibP2P.Response<ByteBuffer>> {
            guard self.mode == .server else {
                self.logger.warning("We received a request while in clientOnly mode")
                return req.eventLoop.makeSucceededFuture(.close)
            }
            switch req.event {
            case .ready:
                return onReady(req)
            case .data:
                return onData(request: req).flatMapError { error -> EventLoopFuture<LibP2P.Response<ByteBuffer>> in
                    self.logger.warning("KadDHT::OnData::Error -> \(error)")
                    return req.eventLoop.makeSucceededFuture(.close)
                }
            case .closed:
                return req.eventLoop.makeSucceededFuture(.close)
            case .error(let error):
                req.logger.error("KadDHT::Error -> \(error)")
                return req.eventLoop.makeSucceededFuture(.close)
            }
        }
        
        private func onReady(_ req:Request) -> EventLoopFuture<LibP2P.Response<ByteBuffer>> {
            self.logger.info("An inbound stream has been opened \(String(describing: req.remotePeer))")
            return req.eventLoop.makeSucceededFuture(.stayOpen)
        }
        
        private func onData(request:Request) -> EventLoopFuture<LibP2P.Response<ByteBuffer>> {
            self.logger.info("We received data from \(String(describing: request.remotePeer))")
            
            /// Is this data from a legitimate peer?
            guard let from = request.remotePeer else {
                self.logger.warning("Inbound Request from unauthenticated stream")
                return request.eventLoop.makeSucceededFuture(.reset(Errors.unknownPeer))
            }
            /// And is it Kad DHT data?
            guard let query = try? Query.decode(Array<UInt8>(request.payload.readableBytesView)) else {
                self.logger.warning("Failed to decode inbound data...")
                //return stream.reset().transform(to: nil)
                //let _ = stream.reset()
                //return request.eventLoop.makeFailedFuture(Errors.unknownPeer)
                return request.eventLoop.makeSucceededFuture(.reset(Errors.DecodingErrorInvalidType))
            }
            
            let pInfo = PeerInfo(peer: from, addresses: [request.addr])

            /// Do we know this peer?
            ///
            /// Nodes, both those operating in client and server mode, add another node to their routing table if and only if that node operates in server mode.
            /// This distinction allows restricted nodes to utilize the DHT, i.e. query the DHT, without decreasing the quality of the distributed hash table, i.e. without polluting the routing tables.
            let _ = self._isPeerOperatingAsServer(from).map { isActingAsServer -> EventLoopFuture<Void> in
                self.network!.peers.getPeerInfo(byID: from.b58String).flatMap { peerInfo in
                    //pInfo = peerInfo
                    return self.addPeerIfSpaceOrCloser(peerInfo)
                }
            }
            
            /// Handle the query
            return request.eventLoop.flatSubmit { //.flatScheduleTask(in: self.connection.responseTime) {
                return self._handleQuery(query, from: pInfo).always { result in
                    switch result {
                    case .success(let res):
                        self.metrics.add(event: .queryResponse(pInfo, res))
                    case .failure(let error):
                        self.logger.error("Error encountered while responding to query \(query) from peer \(from) -> \(error)")
                    }
                }
            }.flatMapThrowing { resp in
                self.logger.info("---")
                self.logger.info("Responding to query \(query) with:")
                self.logger.info("\(resp)")
                self.logger.info("---")
                
                /// Return the response
                return try .respondThenClose(request.allocator.buffer(bytes: resp.encode()))
            }
        }
        
        /// Switches over the Query Type and Handles each appropriately
        func _handleQuery(_ query:Query, from:PeerInfo) -> EventLoopFuture<Response> {
            self.logger.notice("Query::Handling Query \(query) from peer \(from.peer)")
            switch query {
            case .ping:
                return self.eventLoop.makeSucceededFuture( Response.ping )
                
            case .findNode(let pid):
                /// If it's us
                if pid == self.peerID {
                    return self._nearest(self.routingTable.bucketSize, peersToKey: KadDHT.Key(self.peerID)).flatMap { addresses -> EventLoopFuture<Response> in
                        /// .nodeSearch(peers: Array<Multiaddr>(([self.address] + addresses).prefix(self.routingTable.bucketSize))
                        return self.eventLoop.makeSucceededFuture( Response.findNode(closerPeers: addresses.compactMap { try? DHT.Message.Peer($0) }) )
                    }
                } else {
                    /// Otherwise return the closest other peer we know of...
                    //return self.nearestPeerTo(multiaddr)
                    return self.nearest(self.routingTable.bucketSize, toPeer: pid)
                }
                
            case .putValue(let key, let value):
                self.logger.notice("🚨🚨🚨 PutValue Request 🚨🚨🚨")
                self.logger.notice("DHTRecordKey(HEX)::\(key.toHexString())")
                self.logger.notice("DHTRecordValue(HEX)::\((try? value.serializedData().toHexString()) ?? "NIL")")
                guard let namespace = DHT.extractNamespace(key) else {
                    self.logger.warning("Failed to extract namespace for DHT PUT request")
                    self.logger.warning("DHTRecordKey(HEX)::\(key.toHexString())")
                    self.logger.warning("DHTRecordValue(HEX)::\((try? value.serializedData().toHexString()) ?? "NIL")")
                    return self.eventLoop.makeSucceededFuture(.putValue(key: key, record: nil))
                }

                guard let validator = self.validators[namespace] else {
                    self.logger.warning("Query::PutValue::No Validator Set For Namespace '\(String(data: Data(namespace), encoding: .utf8) ?? "???")'")
                    return self.eventLoop.makeSucceededFuture(.putValue(key: key, record: nil))
                }
                
                guard (try? validator.validate(key: key, value: value.serializedData().bytes)) != nil else {
                    self.logger.warning("Query::PutValue::KeyVal failed validation for namespace '\(String(data: Data(namespace), encoding: .utf8) ?? "???")'")
                    return self.eventLoop.makeSucceededFuture(.putValue(key: key, record: nil))
                }
                
                self.logger.notice("Query::PutValue::KeyVal passed validation for namespace '\(String(data: Data(namespace), encoding: .utf8) ?? "???")'")
                self.logger.notice("Query::PutValue::Attempting to store value for key: \(DHT.keyToHumanReadableString(key))")
                return self.addKeyIfSpaceOrCloser(key: key, value: value, usingValidator: validator)
                //return self.addKeyIfSpaceOrCloser2(key: key, value: value, from: from)
                
                
            case .getValue(let key):
                /// If we have the value, send it back!
                let kid = KadDHT.Key(key, keySpace: .xor)
                self.logger.notice("Query::GetValue::\(DHT.keyToHumanReadableString(key))")
                if let val = self.dht[kid] {
                    self.logger.notice("Query::GetValue::Returning value for key: \(DHT.keyToHumanReadableString(key))")
                    return self.eventLoop.makeSucceededFuture( Response.getValue(key: key, record: val, closerPeers: []) )
                } else {
                    /// Otherwise return the k closest peers we know of to the key being searched for (excluding us)
                    return self._nearest(self.routingTable.bucketSize, peersToKey: kid).flatMap { peers in
                        self.logger.notice("Query::GetValue::Returning \(peers.count) closer peers for key: \(DHT.keyToHumanReadableString(key))")
                        return self.eventLoop.makeSucceededFuture( Response.getValue(key: key, record: nil, closerPeers: peers.compactMap { try? DHT.Message.Peer($0) }) )
                    }
                }
                
            case .getProviders(let cid):
                /// Is this correct?? The same thing a getValue?
                guard let CID = try? CID(cid) else { return self.eventLoop.makeSucceededFuture( Response.addProvider(cid: cid, providerPeers: []) ) }
                let kid = KadDHT.Key(cid, keySpace: .xor)
                if self.dht[kid] != nil {
                    let pInfo = PeerInfo(peer: self.peerID, addresses: self.network?.listenAddresses ?? [])
                    
                    self.logger.notice("Query::GetProviders::Returning ourself as a Provider Peer for CID: \(CID.multihash.b58String)")
                    if let dhtPeer = try? DHT.Message.Peer(pInfo) {
                        return self.eventLoop.makeSucceededFuture( Response.getProviders(cid: cid, providerPeers: [dhtPeer], closerPeers: []) )
                    }
                    return self.eventLoop.makeSucceededFuture( Response.getProviders(cid: cid, providerPeers: [], closerPeers: []) )
                } else if let knownProviders = self.providerStore[kid], !knownProviders.isEmpty {
                    self.logger.notice("Query::GetProviders::Returning \(knownProviders.count) Provider Peers for CID: \(CID.multihash.b58String)")
                    return self.eventLoop.makeSucceededFuture( Response.getProviders(cid: cid, providerPeers: knownProviders, closerPeers: []) )
                } else {
                    /// Otherwise return the k closest peers we know of to the key being searched for (excluding us)
                    return self._nearest(self.routingTable.bucketSize, peersToKey: kid).flatMap { peers in
                        self.logger.notice("Query::GetProviders::Returning \(peers.count) Closer Peers for CID: \(CID.multihash.b58String)")
                        return self.eventLoop.makeSucceededFuture( Response.getProviders(cid: cid, providerPeers: [], closerPeers: peers.compactMap { try? DHT.Message.Peer($0) }) )
                    }
                }
            
            case .addProvider(let cid):
                // Ensure the provided CID is valid...
                guard let CID = try? CID(cid) else { return self.eventLoop.makeSucceededFuture( Response.addProvider(cid: cid, providerPeers: []) ) }
                let kid = KadDHT.Key(cid, keySpace: .xor)
                guard let provider = try? DHT.Message.Peer(from) else { return self.eventLoop.makeSucceededFuture( Response.addProvider(cid: cid, providerPeers: []) ) }
                var knownProviders = self.providerStore[kid, default: []]
                if !knownProviders.contains(provider) {
                    knownProviders.append(provider)
                    self.providerStore[kid] = knownProviders
                    self.logger.notice("Query::AddProvider::Added \(from.peer) as a provider for cid: \(CID.multihash.b58String)")
                } else {
                    self.logger.notice("Query::AddProvider::\(from.peer) already a provider for cid: \(CID.multihash.b58String)")
                }
                return self.eventLoop.makeSucceededFuture( Response.addProvider(cid: cid, providerPeers: [provider]) )
            }
        }
        
        /// A method to help make sending queries easier
        func _sendQuery(_ query:Query, to:PeerInfo, on:EventLoop? = nil) -> EventLoopFuture<Response> {
            guard let network = self.network else {
                return (on ?? self.eventLoop).makeFailedFuture(Errors.noNetwork)
            }
         
            guard let payload = try? query.encode() else {
                return (on ?? self.eventLoop).makeFailedFuture(Errors.encodingError)
            }
            
            let queryPromise = (on ?? self.eventLoop).makePromise(of: Response.self)
            /// Create our Timeout Task (if our query doesn't complete by our Timeout time, then we fail it)
            (on ?? self.eventLoop).scheduleTask(in: self.connectionTimeout) {
                queryPromise.fail( Errors.connectionTimedOut )
            }
            
            //self.logger.info("Scanning \(to) for dialable addresses...")
            queryPromise.completeWith(
                /// We should loop through the addresses and determine which one to dial
                /// - Any already open?
                /// - If not, any preferred transports?
                network.dialableAddress(to.addresses, externalAddressesOnly: !isRunningLocally, on: on ?? self.eventLoop).flatMap { dialableAddresses in
                    guard !dialableAddresses.isEmpty else { return (on ?? self.eventLoop).makeFailedFuture( Errors.noDialableAddressesForPeer ) }
                    guard let addy = dialableAddresses.first else { return (on ?? self.eventLoop).makeFailedFuture( Errors.peerIDMultiaddrEncapsulationFailed ) }
                    do {
                        /// We encapsulate the multiaddr with the peers expected public key so we can verify the responder is who we're expecting.
                        let ma = addy.getPeerID() != nil ? addy : try addy.encapsulate(proto: .p2p, address: to.peer.cidString)
                        //let ma = addy.addresses.contains(where: { $0.codec == .p2p }) ? addy : try addy.encapsulate(proto: .p2p, address: to.peer.cidString)
                        self.logger.info("Dialable Addresses For \(to.peer): [\(dialableAddresses.map { $0.description }.joined(separator: ","))]")
                        return network.newRequest(to: ma, forProtocol: KadDHT.multicodec, withRequest: Data(payload), withTimeout: self.connectionTimeout).flatMapThrowing { resp in
                            return try Response.decode(resp.bytes)
                        }
                    } catch {
                        return (on ?? self.eventLoop).makeFailedFuture( Errors.peerIDMultiaddrEncapsulationFailed )
                    }
                    
                }
            )

            return queryPromise.futureResult
        }
        
        /// For each KV in our DHT, we send it to the closest two peers we know of (excluding us)
        private func _shareDHTKVs() -> EventLoopFuture<Void> {
            guard self.dht.count <= 2 else { return self._shareDHTKVsSequentially() }
            return self.dht.compactMap { key, value in
                self.eventLoop.next().submit {
                    self._shareDHTKVWithNearestPeers(key: key, value: value, nearestPeers: 3)
                }.transform(to: ())
            }.flatten(on: self.eventLoop)
        }
        
        private func _shareDHTKVsSequentially() -> EventLoopFuture<Void> {
            let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
            return self.dht.compactMap { key, value in
                group.next().flatSubmit {
                    self._shareDHTKVWithNearestPeers(key: key, value: value, nearestPeers: 3).transform(to: ())
                }
            }.flatten(on: self.eventLoop).always { _ in
                group.shutdownGracefully(queue: .global()) { _ in print("DHT KV ELG shutdown") }
            }
        }
        
        /// Given a KV pair, this method will find the nearest X peers and attempt to share the KV with them.
        private func _shareDHTKVWithNearestPeers(key:KadDHT.Key, value:DHT.Record, nearestPeers peerCount:Int) -> EventLoopFuture<Bool> {
            self.logger.notice("Sharing \(DHT.keyToHumanReadableString(key.original)) with the \(peerCount) closest peers")
            var successfulPuts:[PeerID] = []
            return self._nearest(peerCount, peersToKey: key).flatMap { nearestPeers -> EventLoopFuture<Bool> in
                return nearestPeers.compactMap { peer -> EventLoopFuture<Bool> in
                    return self._sendQuery(.putValue(key: key.original, record: value), to: peer).flatMapAlways { result -> EventLoopFuture<Bool> in
                        switch result {
                        case .success(let res):
                            self.logger.debug("Shared key:value with \(peer.peer)")
                            guard case .putValue(let k, let v) = res else { self.logger.warning("Failed to share key:value with \(peer.peer)"); return self.eventLoop.makeSucceededFuture(false) }
                            guard k == key.original, v != nil else { self.logger.warning("Failed to share key:value with \(peer.peer)"); return self.eventLoop.makeSucceededFuture(false) }
                            self.logger.debug("They Stored It!")
                            successfulPuts.append(peer.peer)
                            return self.eventLoop.makeSucceededFuture(true)
                            
                        case .failure(let error):
                            self.logger.warning("Failed to share key:value with \(peer.peer) -> \(error)")
                            return self.eventLoop.makeSucceededFuture(false)
                        }
                    }
                }.flatten(on: self.eventLoop).map( { $0.contains(true) } ).always { results in
                    self.logger.notice("Done Sharing Key:\(DHT.keyToHumanReadableString(key.original)) with \(successfulPuts.count)/\(nearestPeers.count) peers")
                }
            }
        }
        
        // - TODO: Implement me
//        private func _shareProviderRecords() -> EventLoopFuture<Void> {
//            return self.eventLoop.makeSucceededVoidFuture()
////            self.providerStore.compactMap { key, value in
////
////            }
//        }
        
        /// Checks if the peer specified has announced the "/ipfs/kad/1.0.0" protocol in their Indentify packet.
        /// - Parameter pid: The PeerID to check
        /// - Returns: True if this peer is announcing the /ipfs/kad/1.0.0 protocol
        /// - Note: Peers are only supposed to announce the protocol when in server mode.
        private func _isPeerOperatingAsServer(_ pid:PeerID) -> EventLoopFuture<Bool> {
            guard let network = network else {
                return self.eventLoop.makeSucceededFuture(false)
            }

            return network.peers.getProtocols(forPeer: pid).map { $0.contains { $0.stringValue.contains(KadDHT.multicodec) } }
        }
        
//        public func stop() {
//            guard self.state == .started || self.state == .starting else { self.logger.warning("Already stopped"); return }
//            self.state = .stopping
//            let promise = self.eventLoop.makePromise(of: Void.self)
//            let deadline = self.eventLoop.scheduleTask(in: .seconds(1)) {
//                promise.fail(Errors.connectionTimedOut)
//            }
//            self.heartbeatTask?.cancel(promise: promise)
//            promise.futureResult.whenComplete { result in
//                self.state = .stopped
//                switch result {
//                case .success:
//                    deadline.cancel()
//                    self.logger.info("Node Stopped")
//                case .failure(let error):
//                    self.logger.error("Error encountered while stoping node \(error)")
//                }
//            }
//        }
//        public func stop() {
//            guard self.state == .started || self.state == .starting else { self.logger.warning("Already stopped"); return }
//            self.state = .stopping
//            self.heartbeatTask?.cancel()
//            self.state = .stopped
//            self.logger.info("Node Shutdown")
//        }
        public func stop() {
            guard self.state == .started || self.state == .starting else { self.logger.warning("Already stopped"); return }
            self.state = .stopping
            if self.heartbeatTask != nil {
                let promise = self.eventLoop.makePromise(of: Void.self)
                self.heartbeatTask?.cancel(promise: promise)
                do {
                    try promise.futureResult.wait()
                    self.logger.info("Node Stopped")
                } catch {
                    self.logger.error("Error encountered while stoping node \(error)")
                }
            }
            self.state = .stopped
        }
        
//        public func pingPeers() -> EventLoopFuture<(tried:Int, responses:Int)> {
//            guard self.state == .started else { return self.eventLoop.makeFailedFuture(Errors.noNetwork) }
//            let promise = self.eventLoop.makePromise(of: (tried:Int, responses:Int).self)
//
//            var tried:Int = 0
//            var responses:Int = 0
//
//            self.peerstore.map { key, value in
//                self.logger.info("Attempting to ping \(value.peer)")
//                tried += 1
//                return self._sendQuery(.ping, to: value, on: self.eventLoop).map { resp in
//                    if case .ping = resp {
//                        responses += 1
//                    } else {
//                        self.logger.warning("Recieved unknown response: \(resp)")
//                    }
//                }
//            }.flatten(on: self.eventLoop).whenComplete { result in
//                promise.succeed((tried: tried, responses: responses))
//            }
//
//            return promise.futureResult
//        }
        
        public func store(_ key:[UInt8], value:DHTRecord) -> EventLoopFuture<Bool> {
            let value = value.toProtobuf()
            return self.addKeyIfSpaceOrCloser2(key: key, value: value, from: self.address).map { res in
                guard case .putValue(let k, let rec) = res else {
                    return false
                }
                return k == key && rec == value
            }
        }
        
        public func storeNew(_ key:[UInt8], value:DHTRecord) -> EventLoopFuture<Bool> {
            /// Perform key lookup and request the returned closest k peers to store the value
            let targetID = KadDHT.Key(key)
            let value = value.toProtobuf()
            return self._nearest(self.routingTable.bucketSize, peersToKey: targetID).flatMap { seeds -> EventLoopFuture<Bool> in
                let lookup = KeyLookup(host: self, target: targetID, concurrentRequests: self.maxConcurrentRequest, seeds: seeds)
                return lookup.proceedForPeers().hop(to: self.eventLoop).flatMap { nearestPeers -> EventLoopFuture<Bool> in
                    /// Jump back onto our main event loop to ensure that we're not piggy backing on the lookup's eventloop that's trying to shutdown...
                    let closestPeers = nearestPeers.compactMap { $0.peer == self.peerID ? nil : $0 }
                    /// We have the closest peers to this key that the network knows of...
                    /// Take this opportunity to process / store these new peers
                    return self.addPeersIfSpaceOrCloser(closestPeers).flatMapAlways { _ -> EventLoopFuture<Bool> in
                        /// Lets ask each one to store the value and hope at least one returns true
                        self.logger.warning("Asking the closest \(closestPeers.count) peers to store our value \(value)")
                        /// TODO: We need to limit concurrent queries here as well..
                        //guard let externalAddys = self.network?.externalListeningAddresses, !externalAddys.isEmpty else {
                        //    return self.eventLoop.makeFailedFuture(Errors.cantPutValueWithoutExternallyDialableAddress)
                        //}
                        //let providerInfo = Peer.PeerInfo(id: self.peerID, addresses: externalAddys)
                        var record = DHT.Record()
                        record.key = value.key //Data(targetID.bytes)
                        record.value = value.value
                        //record.timeReceived = value.timeReceived
                        return closestPeers.prefix(4).compactMap { peer in
                            return self._sendQuery(.putValue(key: key, record: record), to: peer, on: self.eventLoop).flatMapAlways { res -> EventLoopFuture<Bool> in
                                switch res {
                                case .success(let response):
                                    self.logger.info("PutValue Response -> \(response)")
                                    guard case .putValue(let k, let rec) = response else {
                                        return self.eventLoop.makeSucceededFuture(false)
                                    }
                                    self.logger.info("PutValue Response from...")
                                    self.logger.info("Peer: \(peer.peer.b58String)")
                                    self.logger.info("Addresses: \(peer.addresses)")
                                    self.logger.info("Query Key: \(key)")
                                    self.logger.info("Response Key: \(k)")
                                    self.logger.info("Query Rec: \(value)")
                                    self.logger.info("Response Rec: \(rec)")
                                    return self.eventLoop.makeSucceededFuture((rec != nil && k == key))
                                case .failure(let error):
                                    self.logger.info("PutValue Error -> \(error)")
                                    return self.eventLoop.makeSucceededFuture(false)
                                }
                            }
                        }.flatten(on: self.eventLoop).flatMap { results -> EventLoopFuture<Bool> in
                            self.logger.warning("\(results.filter({$0}).count)/\(results.count) peers were able to store the value \(value)")
                            /// return true if any of the peers we're able to store the value
                            self.logger.warning("\(self.eventLoop.description)")
                            return self.eventLoop.makeSucceededFuture(results.contains { $0 == true })
                        }
                    }
                }
            }
        }
        
        public func get(_ key:[UInt8]) -> EventLoopFuture<DHTRecord?> {
            self.eventLoop.flatSubmit {
                let kid = KadDHT.Key(key, keySpace: .xor)
                if let val = self.dht[kid] {
                    return self.eventLoop.makeSucceededFuture( val )
                } else {
                    return self._nearestPeerTo(kid).flatMap { peer in
                        return self._lookup(key: key, from: peer, depth: 0)
                    }.flatMap { res -> EventLoopFuture<DHTRecord?> in
                        guard case .getValue(_, let record, let closerPeers) = res else {
                            return self.eventLoop.makeSucceededFuture( nil )
                        }
                        if record == nil {
                            self.logger.info("Failed to find value for key `\(key)`. Lookup terminated without key:val pair and closer peers of [\(closerPeers.map { $0.description }.joined(separator: ", "))]")
                        }
                        return self.eventLoop.makeSucceededFuture( record )
                    }
                }
            }
        }
        
        public func getWithTrace(_ key:[UInt8]) -> EventLoopFuture<(DHTRecord?, LookupTrace)> {
            self.eventLoop.flatSubmit {
                let kid = KadDHT.Key(key, keySpace: .xor)
                let trace = LookupTrace()
                if let val = self.dht[kid] {
                    return self.eventLoop.makeSucceededFuture( (val, trace) )
                } else {
                    return self._nearestPeerTo(kid).flatMap { peer in
                        return self._lookupWithTrace(key: key, from: peer, trace: trace)
                    }.flatMap { res -> EventLoopFuture<(DHTRecord?, LookupTrace)> in
                        guard case .getValue(_, let record, let closerPeers) = res.0 else {
                            return self.eventLoop.makeSucceededFuture( (nil, res.1) )
                        }
                        if record == nil {
                            self.logger.info("Failed to find value for key `\(key)`. Lookup terminated without key:val pair and closer peers of [\(closerPeers.map { $0.description }.joined(separator: ", "))]")
                        }
                        return self.eventLoop.makeSucceededFuture( (record, res.1) )
                    }
                }
            }
        }
        
        /// TODO: We should update this logic to use providers...
        /// -
        public func getUsingLookupList(_ key:[UInt8]) -> EventLoopFuture<DHTRecord?> {
            self.eventLoop.flatSubmit {
                let kid = KadDHT.Key(key, keySpace: .xor)
                if let val = self.dht[kid] {
                    return self.eventLoop.makeSucceededFuture(val)
                } else {
                    return self._nearest(self.routingTable.bucketSize, peersToKey: kid).flatMap { seeds -> EventLoopFuture<DHTRecord?> in
                        let lookupList = KeyLookup(host: self, target: kid, concurrentRequests: self.maxConcurrentRequest, seeds: seeds)
                        return lookupList.proceedForValue().map({ $0 }).hop(to: self.eventLoop)
                    }
                }
            }
        }
      
        public func getProvidersUsingLookupList(_ key:[UInt8]) -> EventLoopFuture<[PeerInfo]> {
            self.eventLoop.flatSubmit {
                let kid = KadDHT.Key(key, keySpace: .xor)
                
                return self._nearest(self.routingTable.bucketSize, peersToKey: kid).flatMap { seeds -> EventLoopFuture<[PeerInfo]> in
                    let lookupList = KeyLookup(host: self, target: kid, concurrentRequests: self.maxConcurrentRequest, seeds: seeds)
                    return lookupList.proceedForProvider().map({ $0 }).hop(to: self.eventLoop)
                }
            }
        }
        
        private func _lookup(key:[UInt8], from peer:PeerInfo, depth:Int) -> EventLoopFuture<Response> {
            guard depth < self.maxLookupDepth else { self.logger.error("Max Lookup Trace Depth Exceeded"); return self.eventLoop.makeFailedFuture(Errors.maxLookupDepthExceeded) }
            return self._sendQuery(.getValue(key: key), to: peer).flatMap { res in
                guard case .getValue(_, let record, let closerPeers) = res else {
                    return self.eventLoop.makeFailedFuture(Errors.DecodingErrorInvalidType)
                }
                if record != nil {
                    return self.eventLoop.makeSucceededFuture(res)
                } else if let nextPeer = closerPeers.first, let pInfo = try? nextPeer.toPeerInfo() {
                    return self._lookup(key: key, from: pInfo, depth: depth + 1)
                } else {
                    return self.eventLoop.makeFailedFuture(Errors.lookupPeersExhausted)
                }
            }
        }
        
        public class LookupTrace:CustomStringConvertible {
            var events:[(TimeInterval,PeerInfo,Response)]
            var depth:Int
            
            init() {
                self.events = []
                self.depth = 0
            }
            
            func add(_ query:Response, from:PeerInfo) {
                self.events.append((Date().timeIntervalSince1970, from, query))
            }
            func incrementDepth() {
                depth = depth + 1
            }
            func containsPeer(_ pInfo:PeerInfo) -> Bool {
                return self.events.contains(where: { $0.1.peer.b58String == pInfo.peer.b58String })
            }
            
            public var description:String {
                """
                Lookup Trace:
                \(eventsToDescription())
                --------------
                """
            }
            
            private func eventsToDescription() -> String {
                if events.isEmpty { return "Node had the key in their DHT" }
                else {
                    return self.events.map { "Asked: \($0.1.peer) got: \(self.responseToDescription($0.2))"}.joined(separator: "\n")
                }
            }
            
            private func responseToDescription(_ query:Response) -> String {
                guard case .getValue(let key, let record, let closerPeers) = query else { return "Invalid Response" }
                if let record = record { return "Result for key \(key.asString(base: .base16)): `\(record.value.asString(base: .base16))`" }
                else if !closerPeers.isEmpty { return "Closer peers [\(closerPeers.compactMap { try? PeerID(fromBytesID: $0.id.bytes).b58String }.joined(separator: "\n"))]"}
                return "Lookup Exhausted"
            }
        }
        
        private func _lookupWithTrace(key:[UInt8], from peer:PeerInfo, trace:LookupTrace) -> EventLoopFuture<(Response, LookupTrace)> {
            guard trace.depth < self.maxLookupDepth else { return self.eventLoop.makeFailedFuture(Errors.maxLookupDepthExceeded) }
            return self._sendQuery(.getValue(key: key), to: peer).flatMap { res in
                trace.add(res, from: peer)
                trace.incrementDepth()
                guard case .getValue(let key, let record, let closerPeers) = res else {
                    return self.eventLoop.makeFailedFuture(Errors.DecodingErrorInvalidType)
                }
                if record != nil {
                    return self.eventLoop.makeSucceededFuture((res, trace))
                } else if let nextPeer = closerPeers.first, let pInfo = try? nextPeer.toPeerInfo() {
                    guard !trace.containsPeer(pInfo) else { return self.eventLoop.makeFailedFuture(Errors.lookupPeersExhausted) }
                    return self._lookupWithTrace(key: key, from: pInfo, trace: trace)
                } else {
                    return self.eventLoop.makeFailedFuture(Errors.lookupPeersExhausted)
                }
            }
        }
        
//        private func _searchForPeers() -> EventLoopFuture<Void> {
//            self.logger.info("Searching for peers")
//            // Create a random peer and search for it?
//            let randomPeer = try! PeerID(fromBytesID: PeerID(.Ed25519).id)
//            //let randomAddress = try! Multiaddr("/ip4/127.0.0.1/tcp/8080/p2p/\(randomPeer.b58String)")
//            return self.routingTable.getPeerInfos().flatMap { peers -> EventLoopFuture<[Response]> in
//                return peers.compactMap { peer -> EventLoopFuture<Response> in
//                    if let pInfo = self.peerstore[peer.id.b58String] {
//                        self.metrics.add(event: .queriedPeer(pInfo, .findNode(id: randomPeer)))
//                        self.logger.debug("Asking \(pInfo.peer) if they know of our randomly generated peer")
//                        return self._sendQuery(.findNode(id: randomPeer), to: pInfo)
//                    } else {
//                        return self.eventLoop.makeFailedFuture(Errors.unknownPeer)
//                    }
//                }.flatten(on: self.eventLoop)
//            }.flatMapEach(on: self.eventLoop) { response -> EventLoopFuture<Void> in
//                guard case let .findNode(peers) = response else { return self.eventLoop.makeFailedFuture(Errors.DecodingErrorInvalidType) }
//                return peers.compactMap { peer -> EventLoopFuture<Void> in
//                    guard let p = try? peer.toPeerInfo() else { return self.eventLoop.makeSucceededVoidFuture() }
//                    return self.addPeerIfSpaceOrCloser(p)
//                }.flatten(on: self.eventLoop)
//            }
//        }
        
        private var numberOfSearches:Int = 0
        /// Performs a node lookup on our the specified address, or a randomly generated one, to try and find the peers closest to it in the current network
        /// - Note: we can fudge the CPL if we're trying to discover peers for a certain k bucket
        private func _searchForPeersLookupStyle(_ peer:PeerID? = nil) -> EventLoopFuture<Void> {
            self.logger.info("Searching for peers... Lookup Style...")
            let addressToSearchFor:PeerID
            if let specified = peer {
                addressToSearchFor = specified
            } else if self.firstSearch || numberOfSearches % 3 == 0 {
                /// If it's our first search, search for our own address...
                self.logger.warning("Searching for our own address")
                self.firstSearch = false
                addressToSearchFor = self.peerID
            } else {
                // Create a random peer and search for it?
                let randomPeer = try! PeerID(.Ed25519)
                //addressToSearchFor = try! Multiaddr("/ip4/127.0.0.1/tcp/8080/p2p/\(randomPeer.b58String)")
                addressToSearchFor = randomPeer
            }
            numberOfSearches += 1
            return self.routingTable.getPeerInfos().flatMap { peers -> EventLoopFuture<Void> in
                return self.dhtPeerToPeerInfo(peers).flatMap { seeds in
                    let lookup = Lookup(host: self, target: addressToSearchFor, concurrentRequests: self.maxConcurrentRequest, seeds: seeds)
                    return lookup.proceed().hop(to: self.eventLoop).flatMap { closestPeers in
                        return closestPeers.compactMap { p in
                            self.logger.info("\(p.peer)")
                            return self.addPeerIfSpaceOrCloser(p)
                        }.flatten(on: self.eventLoop)
                    }
                }
            }
        }
        
        private func dhtPeerToPeerInfo(_ dhtPeers:[DHTPeerInfo]) -> EventLoopFuture<[PeerInfo]> {
            dhtPeers.compactMap {
                self.network?.peers.getPeerInfo(byID: $0.id.b58String)
            }.flatten(on: self.eventLoop)
        }
        
        /// This method adds a peer to our routingTable and peerstore if we either have excess capacity or if the peer is closer to us than the furthest current peer
        private func addPeerIfSpaceOrCloser(_ peer:PeerInfo) -> EventLoopFuture<Void> {
            //guard let pid = try? PeerID(fromBytesID: peer.id.bytes), pid.b58String != self.peerID.b58String else { return self.eventLoop.makeFailedFuture( Errors.unknownPeer ) }
            return self.routingTable.addPeer(peer.peer, isQueryPeer: true, isReplaceable: true, replacementStrategy: self.replacementStrategy).map { success in
                self.logger.notice("\(success ? "Added" : "Did not add") \(peer) to routing table")
                if let network = self.network {
                    network.peers.getPeerInfo(byID: peer.peer.b58String).whenComplete { result in
                        switch result {
                        case .success:
                            return
                        case .failure:
                            let _ = network.peers.add(peerInfo: peer).map { _ in
                                self.metrics.add(event: .peerDiscovered(peer))
                            }
                        }
                    }
                }
                if success {
                    self.metrics.add(event: .addedPeer(peer))
                }
                return
            }
        }
        
        /// Itterates over a collection of peers and attempts to store each one if space or distance permits
        private func addPeersIfSpaceOrCloser(_ peers:[PeerInfo]) -> EventLoopFuture<Void> {
            return peers.map { self.addPeerIfSpaceOrCloser($0) }.flatten(on: self.eventLoop)
        }
        
        /// This method adds a key:value pair to our dht if we either have excess capacity or if the key is closer to us than the furthest current key in the dht
        private func addKeyIfSpaceOrCloser(key:[UInt8], value:DHT.Record, usingValidator validator:Validator) -> EventLoopFuture<Response> {
            let kid = KadDHT.Key(key, keySpace: .xor)
            var added:Bool = false
            if let existingRecord = dht[kid] {
                /// Store the best record...
                let values = [existingRecord, value].compactMap { try? $0.serializedData().bytes }
                let bestIndex = (try? validator.select(key: key, values: values)) ?? 0
                let best = (try? DHT.Record(contiguousBytes: values[bestIndex])) ?? existingRecord
                
                if best == existingRecord {
                    self.logger.notice("We already have `\(key):\(value)` in our DHT")
                } else {
                    self.logger.notice("We already have `\(key):\(value)` in our DHT, but this is a newer record, updating it...")
                }
                
                /// Update the value...
                dht[kid] = best
                added = true
                                
            } else if dht.count < self.dhtSize {
                /// We have space, so lets add it...
                dht[kid] = value
                added = true
                self.logger.notice("We have excess space in DHT, storing `\(key):\(value)`")
                
            } else {
                /// Fetch all current keys, sort by distance to us, if this key is closer than the furthest one, replace it
                let pidAsKey = KadDHT.Key(self.peerID, keySpace: .xor)
                let keys = dht.keys.sorted { lhs, rhs in
                    pidAsKey.compareDistancesFromSelf(to: lhs, and: rhs) == 1
                }
                
                if let furthestKey = keys.last, pidAsKey.compareDistancesFromSelf(to: kid, and: furthestKey) == 1 {
                    /// The new key is closer than our furthest key so lets drop the furthest and add the new key
                    let replacedValue = dht.removeValue(forKey: furthestKey)
                    dht[kid] = value
                    added = true
                    self.logger.notice("Replaced `\(String(data: Data(furthestKey.original), encoding: .utf8) ?? "???")`:`\(String(describing: replacedValue))` with `\(key)`:`\(value)`")
                } else {
                    /// This new value is further away then all of our current keys, lets drop it...
                    added = false
                    self.logger.notice("New Key Value is further away from all current key value pairs, dropping store request.")
                }
            }
            
            return self.eventLoop.makeSucceededFuture(.putValue(key: key, record: added ? value : nil))
        }
        
        /// This version sends the key:value pair onto the next N closest peers, as long as they aren't us or the sender
        private func addKeyIfSpaceOrCloser2(key:[UInt8], value:DHT.Record, from:Multiaddr) -> EventLoopFuture<Response> {
            let kid = KadDHT.Key(key, keySpace: .xor)
            var added:Bool = false
            if dht[kid] != nil {
                /// Update the value...
                dht[kid] = value
                added = true
                self.logger.info("We already have `\(key):\(value)` in our DHT, updating it...")
                return self.eventLoop.makeSucceededFuture(.putValue(key: key, record: value))
                
            } else if dht.count < self.dhtSize {
                /// We have space, so lets add it...
                dht[kid] = value
                added = true
                self.logger.info("We have excess space in DHT, storing `\(key):\(value)`")
                return self.eventLoop.makeSucceededFuture(.putValue(key: key, record: value))
                
            } else {
                /// Fetch all current keys, sort by distance to us, if this key is closer than the furthest one, replace it
                let pidAsKey = KadDHT.Key(self.peerID, keySpace: .xor)
                let keys = dht.keys.sorted { lhs, rhs in
                    pidAsKey.compareDistancesFromSelf(to: lhs, and: rhs) == 1
                }
                
                if let furthestKey = keys.last, pidAsKey.compareDistancesFromSelf(to: kid, and: furthestKey) == 1 {
                    /// The new key is closer than our furthers key so lets drop the furthest and add the new key
                    let replacedValue = dht.removeValue(forKey: furthestKey)
                    dht[kid] = value
                    added = true
                    self.logger.info("Replaced `\(String(data: Data(furthestKey.original), encoding: .utf8) ?? "???")`:`\(String(describing: replacedValue))` with `\(key)`:`\(value)`")
                    return self.eventLoop.makeSucceededFuture(.putValue(key: key, record: value))
                    /// It might be a good idea to pass the replaced value onto another peer at this point...
                    
                } else {
                    /// This new value is further away then all of our current keys, lets drop it...
                    added = false
                    self.logger.info("New Key Value is further away from all current key value pairs, dropping store request.")
                    
                }
                
                /// If we can't store it, lets ask the closest peers ew know about to store it...
                return self._shareDHTKVWithNearestPeers(key: kid, value: value, nearestPeers: 3).map { stored in
                    if stored {
                        return .putValue(key: key, record: value)
                    } else {
                        return .putValue(key: key, record: nil)
                    }
                }
            }
        }
        
        
        private func multiaddressToPeerID(_ ma:Multiaddr) -> PeerID? {
            guard let pidString = ma.getPeerID() else { return nil }
            return try? PeerID(cid: pidString)
        }
        
        /// Returns the closest peer to the given multiaddress, excluding ourselves
        private func nearestPeerTo(_ ma:Multiaddr) -> EventLoopFuture<Response> {
            guard let peer = self.multiaddressToPeerID(ma) else { return self.eventLoop.makeFailedFuture(Errors.unknownPeer) }
            
            return self.routingTable.nearest(1, peersTo: peer).flatMap { peerInfos in
                guard let nearest = peerInfos.first, nearest.id.id != self.peerID.id, let network = self.network else {
                    return self.eventLoop.makeSucceededFuture(Response.findNode(closerPeers: []))
                }
                
                return network.peers.getPeerInfo(byID: nearest.id.b58String).map { pInfo in
                    var closerPeer:[DHT.Message.Peer] = []
                    if let p = try? DHT.Message.Peer(pInfo) {
                        closerPeer.append(p)
                    }
                    return Response.findNode(closerPeers: closerPeer)
                }
            }
        }
        
        /// Returns up to the specified number of closest peers to the provided multiaddress, excluding ourselves
        private func nearest(_ num:Int, toPeer peer:PeerID) -> EventLoopFuture<Response> {
            //guard let peer = self.multiaddressToPeerID(ma) else { return self.eventLoop.makeFailedFuture(Errors.unknownPeer) }
            
            return self.routingTable.nearest(num, peersTo: peer).flatMap { peerInfos in
                return peerInfos.filter { $0.id.id != self.peerID.id }.compactMap {
                    //self.peerstore[$0.id.b58String]
                    self.network?.peers.getPeerInfo(byID: $0.id.b58String)
                }.flatten(on: self.eventLoop).map { ps in
                    Response.findNode(closerPeers: ps.compactMap { try? DHT.Message.Peer($0) })
                }
            }
        }
        
        /// Returns the closest peer we know of to the specified key. This hashes the key using SHA256 before xor'ing it.
        private func _nearestPeerTo(_ key:String) -> EventLoopFuture<PeerInfo> {
            return self._nearestPeerTo(KadDHT.Key(key.bytes))
        }
        
        private func _nearestPeerTo(_ kid:KadDHT.Key) -> EventLoopFuture<PeerInfo> {
            return self.routingTable.nearestPeer(to: kid).flatMap { peer in
                if let peer = peer, let network = self.network {
                    return network.peers.getPeerInfo(byID: peer.id.b58String)
                } else {
                    return self.eventLoop.makeFailedFuture( Errors.unknownPeer )
                }
            }
        }
        
        /// Returns up to the specified number of closest peers to the provided multiaddress, excluding ourselves
        private func _nearest(_ num:Int, peersToKey keyID:KadDHT.Key) -> EventLoopFuture<[PeerInfo]> {
            return self.routingTable.nearest(num, peersToKey: keyID).flatMap { peerInfos in
                return peerInfos.filter { $0.id.id != self.peerID.id }.compactMap {
                    self.network?.peers.getPeerInfo(byID: $0.id.b58String)
                }.flatten(on: self.eventLoop)
            }
        }
        
    }
}

extension PeerStore {
    func getPeerInfo(byID id:String, on:EventLoop? = nil) -> EventLoopFuture<PeerInfo> {
        self.getKey(forPeer: id, on: on).flatMap { key in
            self.getAddresses(forPeer: key, on: on).map { addresses in
                return PeerInfo(peer: key, addresses: addresses)
            }
        }
    }
}

extension DHT.Record: DHTRecord { }

extension DHTRecord {
    func toProtobuf() -> DHT.Record {
        guard self as? DHT.Record == nil else { return self as! DHT.Record }
        return DHT.Record.with { rec in
            rec.key = self.key
            rec.value = self.value
            rec.author = self.author
            rec.signature = self.signature
            rec.timeReceived = self.timeReceived
        }
    }
}

extension KadDHT.Node {
    func dumpPeerstore() {
        self.network?.peers.dumpAll()
//        self.peerstore.forEach {
//            print($0.value)
//        }
    }
}

extension PeerInfo:CustomStringConvertible {
    public var description: String {
        if self.addresses.isEmpty {
            return self.peer.description
        }
        return """
        \(self.peer) [
        \(self.addresses.map({ $0.description }).joined(separator: "\n") )
        ]
        """
    }
}


extension DHT.Message.Peer:CustomStringConvertible {
    var description: String {
        if let pid = try? PeerID(fromBytesID: self.id.bytes) {
            return """
            \(pid) (\(connectionToString(self.connection.rawValue))) [
                \(self.addrs.map { addyBytes -> String in
                    if let ma = try? Multiaddr(addyBytes) {
                        return ma.description
                    } else {
                        return "Invalid Multiaddr"
                    }
                }.joined(separator: "\n"))
            ]
            """
        } else {
            return "Invalid DHT.Message.Peer"
        }
        
    }
    
    func toPeerInfo() throws -> PeerInfo {
        return PeerInfo(
            peer: try PeerID(fromBytesID: self.id.bytes),
            addresses: try self.addrs.map {
                try Multiaddr($0)
            }
        )
    }
    
    init(_ peer:PeerInfo, connection:DHT.Message.ConnectionType = .canConnect) throws {
        self.id = Data(peer.peer.id)
        self.addrs = try peer.addresses.map {
            try $0.binaryPacked()
        }
        self.connection = connection
    }
    
    private func connectionToString(_ type:Int) -> String {
        switch type {
        case 0: return "Not Connected"
        case 1: return "Connected"
        case 2: return "Can Connect"
        case 3: return "Cannot Connect"
        default: return "Invalid Connection Type"
        }
    }
}

