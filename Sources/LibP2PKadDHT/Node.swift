//
//  Node.swift
//  KademliaDHT
//
//  Created by Brandon Toms on 4/29/22.
//

import LibP2P
import CID

protocol Validator {
    func validate(key:String, value:[UInt8]) throws
    func select(key:String, values:[[UInt8]]) throws -> Int
}

extension KadDHT {
    public static var multicodec:String = "/ipfs/kad/1.0.0"
    public class Node:DHTCore, EventLoopService, LifecycleHandler {
        public static var key: String = "KadDHT"
        
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
        var metrics:DHTNodeMetrics
        
        /// Our Logger
        var logger:Logger
        
        /// Known Peers
        var peerstore:[String:PeerInfo] = [:]
        
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
        
        init(eventLoop:EventLoop, network:Application, mode:KadDHT.Mode, peerID:PeerID, bootstrapedPeers:[PeerInfo], options:DHTNodeOptions) {
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
            self.metrics = DHTNodeMetrics()
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
                            self.peerstore[pInfo.peer.b58String] = pInfo
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
                //self.handler = network.register(protocol: SemVerProtocol("/ipfs/kad/1.0.0")!)
                //self.handler!.onReady = onReady
                //self.handler!.onData = onData
            } else {
                self.logger.info("Operating in Client Only Mode")
            }
            
        }
        
        convenience init(network:Application, mode:KadDHT.Mode, bootstrapPeers:[PeerInfo], options:DHTNodeOptions) throws {
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
            for (_, pInfo) in self.peerstore {
                self.onPeerDiscovered?(pInfo)
            }
            
            /// Set up the heartbeat task
            if self.autoUpdate == true {
                self.heartbeatTask = self.eventLoop.scheduleRepeatedAsyncTask(initialDelay: .milliseconds(500), delay: .seconds(180), notifying: nil, self._heartbeat)
            }
            
            self.state = .started
            
            self.logger.info("Started")
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
                self.logger.notice("DHT Keys<\(self.dht.keys.count)> [ \n\(self.dht.keys.map { "\($0)" }.joined(separator: ",\n"))]")
                self.logger.notice("Peers<\(self.peerstore.keys.count)> [ \n\(self.peerstore.keys.map { "\($0)" }.joined(separator: ",\n"))]")
                self.logger.notice("\(self.routingTable.description)")
                return self._shareDHTKVs().flatMap {
                    self._searchForPeersLookupStyle()
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
        
        
        func processRequest(_ req:Request) -> EventLoopFuture<ResponseType<ByteBuffer>> {
            guard self.mode == .server else {
                self.logger.warning("We received a request while in clientOnly mode")
                return req.eventLoop.makeSucceededFuture(.close)
            }
            switch req.event {
            case .ready:
                return onReady(req)
            case .data:
                return onData(request: req)
            case .closed:
                return req.eventLoop.makeSucceededFuture(.close)
            case .error(let error):
                req.logger.error("KadDHT::Error -> \(error)")
                return req.eventLoop.makeSucceededFuture(.close)
            }
        }
        
        private func onReady(_ req:Request) -> EventLoopFuture<ResponseType<ByteBuffer>> {
            self.logger.info("An inbound stream has been opened \(String(describing: req.remotePeer))")
            return req.eventLoop.makeSucceededFuture(.stayOpen)
        }
        
        private func onData(request:Request) -> EventLoopFuture<ResponseType<ByteBuffer>> {
            self.logger.info("We received data from \(String(describing: request.remotePeer))")
            
            /// Is this data from a legitimate peer?
            guard let from = request.remotePeer else {
                self.logger.warning("Inbound Request from unauthenticated stream")
                return request.eventLoop.makeSucceededFuture(.reset(Errors.unknownPeer))
            }
            /// And is it Kad DHT data?
            guard let query = try? DHTQuery.decode(Array<UInt8>(request.payload.readableBytesView)) else {
                self.logger.warning("Failed to decode inbound data...")
                //return stream.reset().transform(to: nil)
                //let _ = stream.reset()
                //return request.eventLoop.makeFailedFuture(Errors.unknownPeer)
                return request.eventLoop.makeSucceededFuture(.reset(Errors.DecodingErrorInvalidType))
            }
            
            var pInfo = PeerInfo(peer: from, addresses: [request.addr])

            /// Do we know this peer?
            ///
            /// Nodes, both those operating in client and server mode, add another node to their routing table if and only if that node operates in server mode.
            /// This distinction allows restricted nodes to utilize the DHT, i.e. query the DHT, without decreasing the quality of the distributed hash table, i.e. without polluting the routing tables.
            if self.peerstore[from.b58String] == nil && from.b58String != self.peerID.b58String {
                self.logger.info("Received a message from a \(from.id) we haven't heard from yet!")
                /// We should only proceed with adding this peer if they are currently opperating in Server mode...
                self._isPeerOperatingAsServer(from).map { isActingAsServer -> EventLoopFuture<Void> in
                    self.logger.info("\(from) \(isActingAsServer ? "is" : "is not") acting as a server...")
                    return self.network!.peers.add(key: from).flatMap {
                        self.network!.peers.getAddresses(forPeer: from).flatMap { addys -> EventLoopFuture<Void> in
                            self.logger.warning("Adding \(addys.count) existing addresses to \(from) from peerstore")
                            pInfo = PeerInfo(peer: from, addresses: pInfo.addresses + addys)
                            return self.addPeerIfSpaceOrCloser(pInfo)
                        }
                    }
                }.whenComplete { _ in
                    self.logger.info("Done processing new peer \(from)")
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
        func _handleQuery(_ query:DHTQuery, from:PeerInfo) -> EventLoopFuture<DHTResponse> {
            self.logger.notice("Query::Handling Query \(query) from peer \(from.peer)")
            switch query {
            case .ping:
                return self.eventLoop.makeSucceededFuture( DHTResponse.ping )
                
            case .findNode(let pid):
                /// If it's us
                if pid == self.peerID {
                    return self._nearest(self.routingTable.bucketSize, peersToKey: KadDHT.Key(self.peerID)).flatMap { addresses -> EventLoopFuture<DHTResponse> in
                        /// .nodeSearch(peers: Array<Multiaddr>(([self.address] + addresses).prefix(self.routingTable.bucketSize))
                        return self.eventLoop.makeSucceededFuture( DHTResponse.findNode(closerPeers: addresses.compactMap { try? DHT.Message.Peer($0) }) )
                    }
                } else {
                    /// Otherwise return the closest other peer we know of...
                    //return self.nearestPeerTo(multiaddr)
                    return self.nearest(self.routingTable.bucketSize, toPeer: pid)
                }
                
            case .putValue(let key, let value):
                // TODO: Validate Key:Value
                if let namespace = self.extractNamespace(key) {
                    if let validator = self.validators[namespace] {
                        if (try? validator.validate(key: String(data: value.key, encoding: .utf8) ?? "", value: value.value.bytes)) != nil {
                            self.logger.notice("Query::PutValue::KeyVal passed validation for namespace \(String(data: Data(namespace), encoding: .utf8) ?? "???")")
                        } else {
                            self.logger.notice("Query::PutValue::KeyVal failed validation for namespace \(String(data: Data(namespace), encoding: .utf8) ?? "???")")
                        }
                    } else {
                        self.logger.notice("Query::PutValue::No Validator Set For Namespace \(String(data: Data(namespace), encoding: .utf8) ?? "???")")
                    }
                } else {
                    self.logger.notice("Query::PutValue::Unknown Namespace...")
                }
                
                self.logger.notice("Query::PutValue::Attempting to store value for key: \(key)")
                return self.addKeyIfSpaceOrCloser(key: key, value: value)
                //return self.addKeyIfSpaceOrCloser2(key: key, value: value, from: from)
                
                
            case .getValue(let key):
                /// If we have the value, send it back!
                let kid = KadDHT.Key(key, keySpace: .xor)
                if let val = self.dht[kid] {
                    self.logger.notice("Query::GetValue::Returning value for key: \(key)")
                    return self.eventLoop.makeSucceededFuture( DHTResponse.getValue(key: key, record: val, closerPeers: []) )
                } else {
                    /// Otherwise return the k closest peers we know of to the key being searched for (excluding us)
                    return self._nearest(self.routingTable.bucketSize, peersToKey: kid).flatMap { peers in
                        self.logger.notice("Query::GetValue::Returning \(peers.count) closer peers for key: \(key)")
                        return self.eventLoop.makeSucceededFuture( DHTResponse.getValue(key: key, record: nil, closerPeers: peers.compactMap { try? DHT.Message.Peer($0) }) )
                    }
                }
                
            case .getProviders(let cid):
                /// Is this correct?? The same thing a getValue?
                let kid = KadDHT.Key(cid, keySpace: .xor)
                if self.dht[kid] != nil {
                    let pInfo = PeerInfo(peer: self.peerID, addresses: self.network?.listenAddresses ?? [])
                    
                    self.logger.notice("Query::GetProviders::Returning ourselves as a Provider Peer for CID: \(cid)")
                    if let dhtPeer = try? DHT.Message.Peer(pInfo) {
                        return self.eventLoop.makeSucceededFuture( DHTResponse.getProviders(cid: cid, providerPeers: [dhtPeer], closerPeers: []) )
                    }
                    return self.eventLoop.makeSucceededFuture( DHTResponse.getProviders(cid: cid, providerPeers: [], closerPeers: []) )
                } else if let knownProviders = self.providerStore[kid], !knownProviders.isEmpty {
                    self.logger.notice("Query::GetProviders::Returning \(knownProviders.count) Provider Peers for CID: \(cid)")
                    return self.eventLoop.makeSucceededFuture( DHTResponse.getProviders(cid: cid, providerPeers: knownProviders, closerPeers: []) )
                } else {
                    /// Otherwise return the k closest peers we know of to the key being searched for (excluding us)
                    return self._nearest(self.routingTable.bucketSize, peersToKey: kid).flatMap { peers in
                        self.logger.notice("Query::GetProviders::Returning \(peers.count) Closer Peers for CID: \(cid)")
                        return self.eventLoop.makeSucceededFuture( DHTResponse.getProviders(cid: cid, providerPeers: [], closerPeers: peers.compactMap { try? DHT.Message.Peer($0) }) )
                    }
                }
            
            case .addProvider(let cid):
                // Ensure the provided CID is valid...
                guard (try? CID(cid)) != nil else { return self.eventLoop.makeSucceededFuture( DHTResponse.addProvider(cid: cid, providerPeers: []) ) }
                let kid = KadDHT.Key(cid, keySpace: .xor)
                guard let provider = try? DHT.Message.Peer(from) else { return self.eventLoop.makeSucceededFuture( DHTResponse.addProvider(cid: cid, providerPeers: []) ) }
                var knownProviders = self.providerStore[kid, default: []]
                if !knownProviders.contains(provider) {
                    knownProviders.append(provider)
                    self.providerStore[kid] = knownProviders
                    self.logger.notice("Query::AddProvider::Added \(from.peer) as a provider for cid: \(cid)")
                } else {
                    self.logger.notice("Query::AddProvider::\(from.peer) already a provider for cid: \(cid)")
                }
                return self.eventLoop.makeSucceededFuture( DHTResponse.addProvider(cid: cid, providerPeers: [provider]) )
            }
        }
        
        /// A method to help make sending queries easier
        func _sendQuery(_ query:DHTQuery, to:PeerInfo, on:EventLoop? = nil) -> EventLoopFuture<DHTResponse> {
            guard let network = self.network else {
                return (on ?? self.eventLoop).makeFailedFuture(Errors.noNetwork)
            }
         
            guard let payload = try? query.encode() else {
                return (on ?? self.eventLoop).makeFailedFuture(Errors.encodingError)
            }
            
            let queryPromise = (on ?? self.eventLoop).makePromise(of: DHTResponse.self)
            /// Create our Timeout Task (if our query doesn't complete by our Timeout time, then we fail it)
            (on ?? self.eventLoop).scheduleTask(in: self.connectionTimeout) {
                queryPromise.fail( Errors.connectionTimedOut )
            }
            
            //self.logger.info("Scanning \(to) for dialable addresses...")
            queryPromise.completeWith(
                /// We should loop through the addresses and determine which one to dial
                /// - Any already open?
                /// - If not, any preferred transports?
                network.dialableAddress(to.addresses, externalAddressesOnly: true, on: on ?? self.eventLoop).flatMap { dialableAddresses in
                    guard !dialableAddresses.isEmpty else { return (on ?? self.eventLoop).makeFailedFuture( Errors.noDialableAddressesForPeer ) }
                    guard let addy = dialableAddresses.first else { return (on ?? self.eventLoop).makeFailedFuture( Errors.peerIDMultiaddrEncapsulationFailed ) }
                    do {
                        /// We encapsulate the multiaddr with the peers expected public key so we can verify the responder is who we're expecting.
                        let ma = addy.getPeerID() != nil ? addy : try addy.encapsulate(proto: .p2p, address: to.peer.cidString)
                        //let ma = addy.addresses.contains(where: { $0.codec == .p2p }) ? addy : try addy.encapsulate(proto: .p2p, address: to.peer.cidString)
                        self.logger.info("Dialable Addresses For \(to.peer): [\(dialableAddresses.map { $0.description }.joined(separator: ","))]")
                        return network.newRequest(to: ma, forProtocol: KadDHT.multicodec, withRequest: Data(payload), withTimeout: self.connectionTimeout).flatMapThrowing { resp in
                            return try DHTResponse.decode(resp.bytes)
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
            self.dht.compactMap { key, value in
                self.logger.notice("Sharing \(key) with the 3 closest peers")
                return self._nearest(3, peersToKey: key).flatMap { nearestPeers -> EventLoopFuture<Void> in
                    return nearestPeers.filter({ $0.peer.b58String != self.peerID.b58String }).prefix(1).compactMap { peer -> EventLoopFuture<Void> in
                        return self._sendQuery(.putValue(key: key.original, record: value), to: peer).transform(to: ())
                    }.flatten(on: self.eventLoop)
                }
            }.flatten(on: self.eventLoop)
        }
        
        /// Checks if the peer specified has announced the "/ipfs/kad/1.0.0" protocol in their Indentify packet.
        /// Peers are only supposed to announce the protocol when in server mode.
        private func _isPeerOperatingAsServer(_ pid:PeerID) -> EventLoopFuture<Bool> {
            guard let network = network else {
                return self.eventLoop.makeSucceededFuture(false)
            }

            return network.peers.getProtocols(forPeer: pid).map { $0.contains { $0.stringValue.contains(KadDHT.multicodec) } }
        }
        
        /// "/" in utf8 == 47
        private func extractNamespace(_ key:[UInt8]) -> [UInt8]? {
            guard key.first == UInt8(47) else { return nil }
            guard let idx = key.dropFirst().firstIndex(of: UInt8(47)) else { return nil }
            return Array(key[1..<idx])
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
        
        public func pingPeers() -> EventLoopFuture<(tried:Int, responses:Int)> {
            guard self.state == .started else { return self.eventLoop.makeFailedFuture(Errors.noNetwork) }
            let promise = self.eventLoop.makePromise(of: (tried:Int, responses:Int).self)
            
            var tried:Int = 0
            var responses:Int = 0
            
            self.peerstore.map { key, value in
                self.logger.info("Attempting to ping \(value.peer)")
                tried += 1
                return self._sendQuery(.ping, to: value, on: self.eventLoop).map { resp in
                    if case .ping = resp {
                        responses += 1
                    } else {
                        self.logger.warning("Recieved unknown response: \(resp)")
                    }
                }
            }.flatten(on: self.eventLoop).whenComplete { result in
                promise.succeed((tried: tried, responses: responses))
            }
            
            return promise.futureResult
        }
        
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
                return lookup.proceedForPeers().hop(to: self.eventLoop).flatMap { closestPeers -> EventLoopFuture<Bool> in
                    /// Jump back onto our main event loop to ensure that we're not piggy backing on the lookup's eventloop that's trying to shutdown...
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
                            return self._sendQuery(.putValue(key: key, record: record), to: peer, on: self.eventLoop).flatMap { res -> EventLoopFuture<Bool> in
                                self.logger.info("PutValue Response -> \(res)")
                                guard case .putValue(let k, let rec) = res else {
                                    return self.eventLoop.makeSucceededFuture(false)
                                }
                                return self.eventLoop.makeSucceededFuture((rec == value && k == key))
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
        
        private func _lookup(key:[UInt8], from peer:PeerInfo, depth:Int) -> EventLoopFuture<DHTResponse> {
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
            var events:[(TimeInterval,PeerInfo,DHTResponse)]
            var depth:Int
            
            init() {
                self.events = []
                self.depth = 0
            }
            
            func add(_ query:DHTResponse, from:PeerInfo) {
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
            
            private func responseToDescription(_ query:DHTResponse) -> String {
                guard case .getValue(let key, let record, let closerPeers) = query else { return "Invalid Response" }
                if let record = record { return "Result for key \(key.asString(base: .base16)): `\(record.value.asString(base: .base16))`" }
                else if !closerPeers.isEmpty { return "Closer peers [\(closerPeers.compactMap { try? PeerID(fromBytesID: $0.id.bytes).b58String }.joined(separator: "\n"))]"}
                return "Lookup Exhausted"
            }
        }
        
        private func _lookupWithTrace(key:[UInt8], from peer:PeerInfo, trace:LookupTrace) -> EventLoopFuture<(DHTResponse, LookupTrace)> {
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
        
        private func _searchForPeers() -> EventLoopFuture<Void> {
            self.logger.info("Searching for peers")
            // Create a random peer and search for it?
            let randomPeer = try! PeerID(fromBytesID: PeerID(.Ed25519).id)
            //let randomAddress = try! Multiaddr("/ip4/127.0.0.1/tcp/8080/p2p/\(randomPeer.b58String)")
            return self.routingTable.getPeerInfos().flatMap { peers -> EventLoopFuture<[DHTResponse]> in
                return peers.compactMap { peer -> EventLoopFuture<DHTResponse> in
                    if let pInfo = self.peerstore[peer.id.b58String] {
                        self.metrics.add(event: .queriedPeer(pInfo, .findNode(id: randomPeer)))
                        self.logger.debug("Asking \(pInfo.peer) if they know of our randomly generated peer")
                        return self._sendQuery(.findNode(id: randomPeer), to: pInfo)
                    } else {
                        return self.eventLoop.makeFailedFuture(Errors.unknownPeer)
                    }
                }.flatten(on: self.eventLoop)
            }.flatMapEach(on: self.eventLoop) { response -> EventLoopFuture<Void> in
                guard case let .findNode(peers) = response else { return self.eventLoop.makeFailedFuture(Errors.DecodingErrorInvalidType) }
                return peers.compactMap { peer -> EventLoopFuture<Void> in
                    guard let p = try? peer.toPeerInfo() else { return self.eventLoop.makeSucceededVoidFuture() }
                    return self.addPeerIfSpaceOrCloser(p)
                }.flatten(on: self.eventLoop)
            }
        }
        
        private var numberOfSearches:Int = 0
        /// Performs a node lookup on our the specified address, or a randomly generated one, to try and find the peers closest to it in the current network
        /// - Note: we can fudge the CPL if we're trying to discover peers for a certain k bucket
        private func _searchForPeersLookupStyle(_ peer:PeerID? = nil) -> EventLoopFuture<Void> {
            self.logger.info("Searching for peers... Lookup Style...")
            let addressToSearchFor:PeerID
            if let specified = peer {
                addressToSearchFor = specified
            } else if self.firstSearch || numberOfSearches % 2 == 0 {
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
                let seeds = peers.reduce(into: Array<PeerInfo>()) { partialResult, dhtPeer in
                    if let pInfo = self.peerstore[dhtPeer.id.b58String] {
                        partialResult.append(pInfo)
                    }
                }
                let lookup = Lookup(host: self, target: addressToSearchFor, concurrentRequests: self.maxConcurrentRequest, seeds: seeds)
                return lookup.proceed().hop(to: self.eventLoop).flatMap { closestPeers in
                    return closestPeers.compactMap { p in
                        self.logger.info("\(p.peer)")
                        return self.addPeerIfSpaceOrCloser(p)
                    }.flatten(on: self.eventLoop)
                }
            }
        }
        
        /// This method adds a peer to our routingTable and peerstore if we either have excess capacity or if the peer is closer to us than the furthest current peer
        private func addPeerIfSpaceOrCloser(_ peer:PeerInfo) -> EventLoopFuture<Void> {
            //guard let pid = try? PeerID(fromBytesID: peer.id.bytes), pid.b58String != self.peerID.b58String else { return self.eventLoop.makeFailedFuture( Errors.unknownPeer ) }
            return self.routingTable.addPeer(peer.peer, isQueryPeer: true, isReplaceable: true, replacementStrategy: self.replacementStrategy).map { success in
                if self.peerstore[peer.peer.b58String] == nil {
                    self.peerstore[peer.peer.b58String] = peer
                    self.metrics.add(event: .peerDiscovered(peer))
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
        private func addKeyIfSpaceOrCloser(key:[UInt8], value:DHT.Record) -> EventLoopFuture<DHTResponse> {
            let kid = KadDHT.Key(key, keySpace: .xor)
            var added:Bool = false
            if dht[kid] != nil {
                /// Update the value...
                dht[kid] = value
                added = true
                self.logger.notice("We already have `\(key):\(value)` in our DHT, updating it...")
                //return self.eventLoop.makeSucceededFuture(.stored(added))
                
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
        private func addKeyIfSpaceOrCloser2(key:[UInt8], value:DHT.Record, from:Multiaddr) -> EventLoopFuture<DHTResponse> {
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
                    
                    /// Find the closest peer to the kid and send it their way...
                    return self._nearest(3, peersToKey: kid).flatMap { nearestPeers -> EventLoopFuture<DHTResponse> in
                        let filtered = nearestPeers.filter { pInfo in
                            /// TODO: Double check this logic
                            pInfo.peer.b58String != self.peerID.b58String && pInfo.peer.b58String != from.getPeerID()
                        }
                        
                        if filtered.isEmpty { return self.eventLoop.makeSucceededFuture(.putValue(key: key, record: nil)) }
                        
                        return filtered.prefix(1).compactMap { nearestPeer -> EventLoopFuture<DHTResponse> in
                            return self._sendQuery(.putValue(key: key, record: value), to: nearestPeer).map { res in
                                guard case .putValue = res else {
                                    return .putValue(key: key, record: nil)
                                }
                                return res
                            }
                        }.flatten(on: self.eventLoop).flatMap { responses in
                            if added { return self.eventLoop.makeSucceededFuture(.putValue(key: key, record: value)) }
                            else if responses.contains(where: { response in
                                    guard case let .putValue(_, record) = response else { return false }
                                    return record != nil
                            }) {
                                return self.eventLoop.makeSucceededFuture(.putValue(key: key, record: value))
                            } else {
                                return self.eventLoop.makeSucceededFuture(.putValue(key: key, record: nil))
                            }
                        }
                    }
                }
            }
        }
        
        
        private func multiaddressToPeerID(_ ma:Multiaddr) -> PeerID? {
            guard let pidString = ma.getPeerID() else { return nil }
            return try? PeerID(cid: pidString)
        }
        
        /// Returns the closest peer to the given multiaddress, excluding ourselves
        private func nearestPeerTo(_ ma:Multiaddr) -> EventLoopFuture<DHTResponse> {
            guard let peer = self.multiaddressToPeerID(ma) else { return self.eventLoop.makeFailedFuture(Errors.unknownPeer) }
            
            return self.routingTable.nearest(1, peersTo: peer).flatMap { peerInfos in
                guard let nearest = peerInfos.first, nearest.id.id != self.peerID.id, let pInfo = self.peerstore[nearest.id.b58String] else {
                    return self.eventLoop.makeSucceededFuture(DHTResponse.findNode(closerPeers: []))
                }
                var closerPeer:[DHT.Message.Peer] = []
                if let p = try? DHT.Message.Peer(pInfo) {
                    closerPeer.append(p)
                }
                
                return self.eventLoop.makeSucceededFuture(DHTResponse.findNode(closerPeers: closerPeer))
            }
        }
        
        /// Returns up to the specified number of closest peers to the provided multiaddress, excluding ourselves
        private func nearest(_ num:Int, toPeer peer:PeerID) -> EventLoopFuture<DHTResponse> {
            //guard let peer = self.multiaddressToPeerID(ma) else { return self.eventLoop.makeFailedFuture(Errors.unknownPeer) }
            
            return self.routingTable.nearest(num, peersTo: peer).flatMap { peerInfos in
                let ps = peerInfos.filter { $0.id.id != self.peerID.id }.compactMap {
                    self.peerstore[$0.id.b58String]
                }
                return self.eventLoop.makeSucceededFuture(DHTResponse.findNode(closerPeers: ps.compactMap { try? DHT.Message.Peer($0) }))
            }
        }
        
        /// Returns the closest peer we know of to the specified key. This hashes the key using SHA256 before xor'ing it.
        private func _nearestPeerTo(_ key:String) -> EventLoopFuture<PeerInfo> {
            return self._nearestPeerTo(KadDHT.Key(key.bytes))
        }
        
        private func _nearestPeerTo(_ kid:KadDHT.Key) -> EventLoopFuture<PeerInfo> {
            return self.routingTable.nearestPeer(to: kid).flatMap { peer in
                if let peer = peer, let ma = self.peerstore[peer.id.b58String] {
                    return self.eventLoop.makeSucceededFuture(ma)
                } else {
                    return self.eventLoop.makeFailedFuture( Errors.unknownPeer )
                }
            }
        }
        
        /// Returns up to the specified number of closest peers to the provided multiaddress, excluding ourselves
        private func _nearest(_ num:Int, peersToKey keyID:KadDHT.Key) -> EventLoopFuture<[PeerInfo]> {
            return self.routingTable.nearest(num, peersToKey: keyID).flatMap { peerInfos in
                let ps = peerInfos.filter { $0.id.id != self.peerID.id }.compactMap {
                    self.peerstore[$0.id.b58String]
                }
                return self.eventLoop.makeSucceededFuture(ps)
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
        self.peerstore.forEach {
            print($0.value)
        }
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

