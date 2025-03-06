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

import CID
import LibP2P
import Multihash

public enum KadDHT {
    public static var multicodec: String = "/ipfs/kad/1.0.0"
    public static var multicodecLAN: String = "ipfs/lan/kad/1.0.0"

    static var CPL_BITS_NOT_BYTES:Bool = true
    
    public enum Mode {
        case client
        case server
    }

    public class Node: DHTCore, EventLoopService, LifecycleHandler, PeerRouting, ContentRouting {
        public static var key: String = "KadDHT"

        enum State {
            case started
            case stopped
        }

        /// A weak reference back to our main LibP2P instance
        weak var network: Application?

        /// Wether the DHT is operating in Client or Server mode
        ///
        /// Nodes operating in server mode advertise the libp2p Kademlia protocol identifier via the identify protocol.
        /// In addition server mode nodes accept incoming streams using the Kademlia protocol identifier.
        /// Nodes operating in client mode do not advertise support for the libp2p Kademlia protocol identifier.
        /// In addition they do not offer the Kademlia protocol identifier for incoming streams.
        let mode: KadDHT.Mode

        /// Fake Internet Connection Type
        //let connection:InternetType

        /// Max number of concurrent requests we can have open at any moment
        let maxConcurrentRequest: Int

        /// Max Connection Timeout
        let connectionTimeout: TimeAmount

        /// DHT Key:Value Store
        let dhtSize: Int
        let dht: EventLoopDictionary<KadDHT.Key, DHT.Record>

        /// DHT Peer Store
        let routingTable: RoutingTable
        let maxPeers: Int

        /// Naive DHT Provider Store
        let providerStore: EventLoopDictionary<KadDHT.Key, [DHT.Message.Peer]>
        let maxProviderStoreSize: Int

        /// The event loop that we're operating on...
        public let eventLoop: EventLoop

        /// Our nodes multiaddress
        var address: Multiaddr!

        /// Our nodes PeerID
        let peerID: PeerID

        /// Our Nodes Event History
        var metrics: NodeMetrics

        /// Our Logger
        var logger: Logger

        /// Known Peers
        let peerstore: PeerStore

        /// Wether the node should start a timer that triggers the heartbeat method, or if it should wait for an external service to call the heartbeat method explicitly
        public var autoUpdate: Bool

        var replacementStrategy: RoutingTable.ReplacementStrategy = .furtherThanReplacement

        private var heartbeatTask: RepeatedTask?

        public private(set) var state: ServiceLifecycleState = .stopped

        private var maxLookupDepth: Int = 5

        private var firstSearch: Bool = true

        private var handler: LibP2P.ProtocolHandler?

        private var isRunningHeartbeat: Bool = false

        private var defaultValidator: Validator = KadDHT.BaseValidator.AllowAll()

        /// [Namespace:Validator]
        private var validators: [[UInt8]: Validator] = [:]

        /// This is why there is a "ipfs/lan/kad/1.0.0" protocol...
        let isRunningLocally: Bool

        init(eventLoop: EventLoop, network: Application, mode: KadDHT.Mode, peerID: PeerID, bootstrapedPeers: [PeerInfo], options: NodeOptions, peerstore: PeerStore? = nil) {
            self.eventLoop = eventLoop
            self.network = network
            self.mode = mode
            self.peerID = peerID
            self.peerstore = peerstore ?? network.peers
            //self.connection = options.connection
            self.maxConcurrentRequest = options.maxConcurrentConnections
            self.connectionTimeout = options.connectionTimeout
            self.dht = EventLoopDictionary(on: eventLoop)
            self.dhtSize = options.maxKeyValueStoreSize
            self.providerStore = EventLoopDictionary(on: eventLoop)
            self.maxProviderStoreSize = options.maxProviderStoreSize
            self.maxPeers = options.maxPeers
            self.routingTable = RoutingTable(eventloop: eventLoop, bucketSize: options.bucketSize, localPeerID: peerID, latency: options.connectionTimeout, peerstoreMetrics: [:], usefulnessGracePeriod: .minutes(5))
            self.logger = Logger(label: "DHTNode\(peerID)")
            self.logger.logLevel = network.logger.logLevel
            self.metrics = NodeMetrics(record: false)
            self.state = .stopped
            self.autoUpdate = true
            self.isRunningLocally = options.supportLocalNetwork

            /// Add our initialized event
            self.metrics.add(event: .initialized)

            /// Add the bootstrapped peers to our routing table
            bootstrapedPeers.compactMap { pInfo -> EventLoopFuture<Bool> in
                self.metrics.add(event: .peerDiscovered(pInfo))
                return self.routingTable.addPeer( pInfo.peer ).always { result in
                    switch result {
                        case .success(let didAddPeer):
                            if didAddPeer {
                                self.metrics.add(event: .addedPeer(pInfo))
                                _ = self.peerstore.add(peerInfo: pInfo).map {
                                    self.markPeerAsNecessary(peer: pInfo.peer)
                                }
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

            /// Register to be notified of peer removal from our RoutingTable so we can mark the peer as prunable in our peerstore.
            self.routingTable.peerRemovedHandler = { peer in
                _ = self.markPeerAsPrunable(peer: peer)
            }

            self.logger.info("DHTNode Initialized")
        }

        convenience init(network: Application, mode: KadDHT.Mode, bootstrapPeers: [PeerInfo], options: NodeOptions) throws {
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
            self.address = addy.getPeerID() != nil ? addy : try! addy.encapsulate(proto: .p2p, address: self.peerID.b58String)
            self.state = .starting

            /// Alert our app of the bootstrapped peers...
//            for (_, pInfo) in self.peerstore {
//                self.onPeerDiscovered?(pInfo)
//            }
            if let opd = self.onPeerDiscovered {
                let _ = self.peerstore.all().map { peers in
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
            print("Attempting to find peer \(peer)")
            return self._searchForPeersLookupStyle(peer).flatMap { closestPeers -> EventLoopFuture<PeerInfo> in
                print("ClosestPeers: \(closestPeers)")
                if let match = closestPeers.first(where: { $0.peer == peer }) {
                    print("Got Match!")
                    return self.eventLoop.makeSucceededFuture(match)
                } else {
                    self.logger.notice("Post Lookup: Querying Peer Store for PeerID \(peer)")
                    return self.peerstore.getPeerInfo(byID: peer.b58String, on: self.eventLoop)
                }
            }
        }

        public func provide(cid: [UInt8], announce: Bool) -> EventLoopFuture<Void> {
            self.eventLoop.makeFailedFuture(Errors.notSupported)
        }

        public func findProviders(cid: [UInt8], count: Int) -> EventLoopFuture<[Multiaddr]> {
            guard let cid = try? CID(cid) else { return self.eventLoop.makeFailedFuture(Errors.invalidCID) }
            return self.getProvidersUsingLookupList(cid.rawBuffer).map { peers in
                peers.reduce(into: [], { partialResult, pInfo in
                    partialResult.append(contentsOf: pInfo.addresses)
                })
            }
        }

        public func heartbeat() -> EventLoopFuture<Void> {
            guard self.autoUpdate == false else { return self.eventLoop.makeFailedFuture(Errors.cannotCallHeartbeatWhileNodeIsInAutoUpdateMode) }
            return self._heartbeat()
        }

        private func _heartbeat(_ task: RepeatedTask? = nil) -> EventLoopFuture<Void> {
            guard self.isRunningHeartbeat == false else { return self.eventLoop.makeSucceededVoidFuture() }
            return self.eventLoop.flatSubmit {
                self.logger.notice("Running Heartbeat")
                self.isRunningHeartbeat = true
                let tic = DispatchTime.now()
                return self.peerstore.all()
                    .and(self.dht.all())
                    .and(self.providerStore.count())
                    .flatMap { arg0, providerRecordCount in
                        let (peers, dhtValues) = arg0
                        self.logger.notice("\(self.routingTable.description)")
                        if let data = try? JSONEncoder().encode(MetadataBook.PrunableMetadata(prunable: .necessary)).bytes {
                            self.logger.notice("Necessary Peers<\(peers.filter({ $0.metadata[MetadataBook.Keys.Prunable.rawValue] == data }).count)>")
                        }
                        self.logger.notice("ProviderStore<\(providerRecordCount)>")
                        self.logger.notice("DHT Keys<\(dhtValues.count)> [ \n\(dhtValues.map { "\($0.key)" }.joined(separator: ",\n"))]")
                        self.logger.notice("PeerStore<\(peers.count)> [ \n\(peers.map { "\($0.id.b58String)" }.joined(separator: ",\n"))]")
                        return self._pruneProviders().flatMap {
                            self._shareDHTKVs().flatMap {
                                // TODO: Share Provider Records
                                self._searchForPeersLookupStyle().transform(to: Void())
                            }
                        }
                    }.always { _ in
                        self.logger.notice("Heartbeat Finished after \((DispatchTime.now().uptimeNanoseconds - tic.uptimeNanoseconds) / 1_000_000)ms")
                        self.isRunningHeartbeat = false
                    }
//                return self.peerstore.all().flatMap { peers in
//                    self.logger.notice("\(self.routingTable.description)")
//                    if let data = try? JSONEncoder().encode(MetadataBook.PrunableMetadata(prunable: .necessary)).bytes {
//                        self.logger.notice("Necessary Peers<\(peers.filter({ $0.metadata[MetadataBook.Keys.Prunable.rawValue] == data }).count)>")
//                    }
//                    let allDHT = self.dht.all().map { }
//                    self.logger.notice("ProviderStore<\(self.providerStore.count)>")
//                    self.logger.notice("DHT Keys<\(self.dht.keys.count)> [ \n\(self.dht.keys.map { "\($0)" }.joined(separator: ",\n"))]")
//                    self.logger.notice("PeerStore<\(peers.count)> [ \n\(peers.map { "\($0.id.b58String)" }.joined(separator: ",\n"))]")
//                    return self._pruneProviders().flatMap {
//                        self._shareDHTKVs().flatMap {
//                            // TODO: Share Provider Records
//                            self._searchForPeersLookupStyle()
//                        }
//                    }
//                }.always { _ in
//                    self.logger.notice("Heartbeat Finished after \((DispatchTime.now().uptimeNanoseconds - tic.uptimeNanoseconds) / 1_000_000)ms")
//                    self.isRunningHeartbeat = false
//                }
            }.flatMapError { error in
                self.logger.notice("Heartbeat encountered error '\(error)'")
                return self.eventLoop.makeSucceededVoidFuture()
            }
        }

        public func advertise(service: String, options: Options?) -> EventLoopFuture<TimeAmount> {
            self.eventLoop.makeFailedFuture(Errors.notSupported)
        }

        public func findPeers(supportingService: String, options: Options?) -> EventLoopFuture<DiscoverdPeers> {
            self.eventLoop.makeFailedFuture(Errors.notSupported)
        }

        public var onPeerDiscovered: ((PeerInfo) -> Void)?

        /// Handles a new namespace via the provided validator.
        /// - Parameters:
        ///   - namespace: The namespace prefix for the DHT KV pair
        ///   - validator: The validator that ensures the Value being stored is valid and the most desirable
        /// - Returns: Void upon succes, error upon failure.
        public func handle(namespace: String, validator: Validator) -> EventLoopFuture<Void> {
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

        /// Prunes the first 10% of provider keys with the fewest providers...
        /// - TODO: We should keep track of when we added entries so we can expire/prune them appropriately
        private func _pruneProviders() -> EventLoopFuture<Void> {
            self.eventLoop.flatSubmit {
                self.logger.notice("✂️✂️✂️ Pruning Provider Entries ✂️✂️✂️")
                return self.providerStore.prune(toAmount: self.maxProviderStoreSize)
            }
        }

        func processGetRequest(_ req: Request) -> EventLoopFuture<LibP2P.Response<ByteBuffer>> {
            return self.processRequest(req)
        }

        func processPutRequest(_ req: Request) -> EventLoopFuture<LibP2P.Response<ByteBuffer>> {
            return self.processRequest(req)
        }

        func processRequest(_ req: Request) -> EventLoopFuture<LibP2P.Response<ByteBuffer>> {
            guard self.mode == .server else {
                self.logger.warning("We received a request while in clientOnly mode")
                return req.eventLoop.makeSucceededFuture(.close)
            }
            switch req.event {
                case .ready:
                    return self.onReady(req)
                case .data:
                    return self.onData(request: req).flatMapError { error -> EventLoopFuture<LibP2P.Response<ByteBuffer>> in
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

        private func onReady(_ req: Request) -> EventLoopFuture<LibP2P.Response<ByteBuffer>> {
            req.logger.info("An inbound stream has been opened \(String(describing: req.remotePeer))")
            return req.eventLoop.makeSucceededFuture(.stayOpen)
        }

        private func onData(request: Request) -> EventLoopFuture<LibP2P.Response<ByteBuffer>> {
            request.logger.info("We received data from \(String(describing: request.remotePeer))")

            /// Is this data from a legitimate peer?
            guard let from = request.remotePeer else {
                request.logger.warning("Inbound Request from unauthenticated stream")
                return request.eventLoop.makeSucceededFuture(.reset(Errors.unknownPeer))
            }
            /// And is it Kad DHT data?
            guard let query = try? Query.decode(Array<UInt8>(request.payload.readableBytesView)) else {
                request.logger.warning("Failed to decode inbound data...")
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
            _ = self._isPeerOperatingAsServer(from).map { isActingAsServer -> EventLoopFuture<Void> in
                self.network!.peers.getPeerInfo(byID: from.b58String).flatMap { peerInfo in
                    //pInfo = peerInfo
                    self.addPeerIfSpaceOrCloser(peerInfo)
                }
            }

            /// Handle the query
            return request.eventLoop.flatSubmit { //.flatScheduleTask(in: self.connection.responseTime) {
                return self._handleQuery(query, from: pInfo, request: request).always { result in
                    switch result {
                        case .success(let res):
                            self.metrics.add(event: .queryResponse(pInfo, res))
                        case .failure(let error):
                            request.logger.error("Error encountered while responding to query \(query) from peer \(from) -> \(error)")
                    }
                }
            }.flatMapThrowing { resp in
                request.logger.info("---")
                request.logger.info("Responding to query \(query) with:")
                request.logger.info("\(resp)")
                request.logger.info("---")

                /// Return the response
                return try .respondThenClose(request.allocator.buffer(bytes: resp.encode()))
            }
        }

        /// Switches over the Query Type and Handles each appropriately
        func _handleQuery(_ query: Query, from: PeerInfo, request: Request) -> EventLoopFuture<Response> {
            request.logger.notice("Query::Handling Query \(query) from peer \(from.peer)")
            switch query {
                case .ping:
                    return self.eventLoop.makeSucceededFuture( Response.ping )

                case .findNode(let pid):
                    /// If it's us
                    if pid == self.peerID {
                        return self._nearest(self.routingTable.bucketSize, peersToKey: KadDHT.Key(self.peerID)).flatMap { addresses -> EventLoopFuture<Response> in
                            /// .nodeSearch(peers: Array<Multiaddr>(([self.address] + addresses).prefix(self.routingTable.bucketSize))
                            self.eventLoop.makeSucceededFuture( Response.findNode(closerPeers: addresses.compactMap { try? DHT.Message.Peer($0) }) )
                        }
                    } else {
                        /// Otherwise return the closest other peer we know of...
                        //return self.nearestPeerTo(multiaddr)
                        return self.nearest(self.routingTable.bucketSize, toPeer: pid)
                    }

                case .putValue(let key, let value):
                    request.logger.notice("🚨🚨🚨 PutValue Request 🚨🚨🚨")
                    request.logger.notice("DHTRecordKey(HEX)::\(key.toHexString())")
                    request.logger.notice("DHTRecordValue(HEX)::\((try? value.serializedData().toHexString()) ?? "NIL")")
                    guard let namespace = KadDHT.extractNamespace(key) else {
                        request.logger.warning("Failed to extract namespace for DHT PUT request")
                        request.logger.warning("DHTRecordKey(HEX)::\(key.toHexString())")
                        request.logger.warning("DHTRecordValue(HEX)::\((try? value.serializedData().toHexString()) ?? "NIL")")
                        return self.eventLoop.makeSucceededFuture(.putValue(key: key, record: nil))
                    }

                    guard let validator = self.validators[namespace] else {
                        request.logger.warning("Query::PutValue::No Validator Set For Namespace '\(String(data: Data(namespace), encoding: .utf8) ?? "???")'")
                        return self.eventLoop.makeSucceededFuture(.putValue(key: key, record: nil))
                    }

                    guard (try? validator.validate(key: key, value: value.serializedData().bytes)) != nil else {
                        request.logger.warning("Query::PutValue::KeyVal failed validation for namespace '\(String(data: Data(namespace), encoding: .utf8) ?? "???")'")
                        return self.eventLoop.makeSucceededFuture(.putValue(key: key, record: nil))
                    }

                    request.logger.notice("Query::PutValue::KeyVal passed validation for namespace '\(String(data: Data(namespace), encoding: .utf8) ?? "???")'")
                    request.logger.notice("Query::PutValue::Attempting to store value for key: \(KadDHT.keyToHumanReadableString(key))")
                    return self.addKeyIfSpaceOrCloser(key: key, value: value, usingValidator: validator, logger: request.logger)
                //return self.addKeyIfSpaceOrCloser2(key: key, value: value, from: from)

                case .getValue(let key):
                    /// If we have the value, send it back!
                    let kid = KadDHT.Key(key, keySpace: .xor)
                    request.logger.notice("Query::GetValue::\(KadDHT.keyToHumanReadableString(key))")
                    return self.dht.getValue(forKey: kid).flatMap { value in
                        if let value = value {
                            request.logger.notice("Query::GetValue::Returning value for key: \(KadDHT.keyToHumanReadableString(key))")
                            return self.eventLoop.makeSucceededFuture( Response.getValue(key: key, record: value, closerPeers: []) )
                        } else {
                            return self._nearest(self.routingTable.bucketSize, peersToKey: kid).flatMap { peers in
                                request.logger.notice("Query::GetValue::Returning \(peers.count) closer peers for key: \(KadDHT.keyToHumanReadableString(key))")
                                return self.eventLoop.makeSucceededFuture( Response.getValue(key: key, record: nil, closerPeers: peers.compactMap { try? DHT.Message.Peer($0) }) )
                            }
                        }
                    }

                case .getProviders(let cid):
                    /// Is this correct?? The same thing a getValue?
                    guard let CID = try? CID(cid) else { return self.eventLoop.makeSucceededFuture( Response.addProvider(cid: cid, providerPeers: []) ) }
                    let kid = KadDHT.Key(cid, keySpace: .xor)

                    return self.providerStore.getValue(forKey: kid).flatMap { value in
                        if let value = value, !value.isEmpty {
                            request.logger.notice("Query::GetProviders::Returning \(value.count) Provider Peers for CID: \(CID.multihash.b58String)")
                            return self.eventLoop.makeSucceededFuture( Response.getProviders(cid: cid, providerPeers: value, closerPeers: []) )
                        } else {
                            /// Otherwise return the k closest peers we know of to the key being searched for (excluding us)
                            return self._nearest(self.routingTable.bucketSize, peersToKey: kid).flatMap { peers in
                                request.logger.notice("Query::GetProviders::Returning \(peers.count) Closer Peers for CID: \(CID.multihash.b58String)")
                                return self.eventLoop.makeSucceededFuture( Response.getProviders(cid: cid, providerPeers: [], closerPeers: peers.compactMap { try? DHT.Message.Peer($0) }) )
                            }
                        }
                    }

                case .addProvider(let cid):
                    // Ensure the provided CID is valid...
                    guard let CID = try? CID(cid) else { return self.eventLoop.makeSucceededFuture( Response.addProvider(cid: cid, providerPeers: []) ) }
                    let kid = KadDHT.Key(cid, keySpace: .xor)
                    guard let provider = try? DHT.Message.Peer(from) else { return self.eventLoop.makeSucceededFuture( Response.addProvider(cid: cid, providerPeers: []) ) }

                    return self.providerStore.getValue(forKey: kid, default: []).flatMap { existingProviders in
                        if !existingProviders.contains(provider) {
                            request.logger.notice("Query::AddProvider::\(from.peer) already a provider for cid: \(CID.multihash.b58String)")
                            return self.eventLoop.makeSucceededFuture( Response.addProvider(cid: cid, providerPeers: [provider]) )
                        } else {
                            return self.providerStore.updateValue(existingProviders + [provider], forKey: kid).map { _ in
                                request.logger.notice("Query::AddProvider::Added \(from.peer) as a provider for cid: \(CID.multihash.b58String)")
                                return Response.addProvider(cid: cid, providerPeers: [provider])
                            }
                        }
                    }
            }
        }

        /// A method to help make sending queries easier
        func _sendQuery(_ query: Query, to: PeerInfo, on: EventLoop? = nil) -> EventLoopFuture<Response> {
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
                network.dialableAddress(to.addresses, externalAddressesOnly: !self.isRunningLocally, on: on ?? self.eventLoop).flatMap { dialableAddresses in
                    guard !dialableAddresses.isEmpty else { return (on ?? self.eventLoop).makeFailedFuture( Errors.noDialableAddressesForPeer ) }
                    guard let addy = dialableAddresses.first else { return (on ?? self.eventLoop).makeFailedFuture( Errors.peerIDMultiaddrEncapsulationFailed ) }
                    do {
                        /// We encapsulate the multiaddr with the peers expected public key so we can verify the responder is who we're expecting.
                        let ma = addy.getPeerID() != nil ? addy : try addy.encapsulate(proto: .p2p, address: to.peer.cidString)
                        //let ma = addy.addresses.contains(where: { $0.codec == .p2p }) ? addy : try addy.encapsulate(proto: .p2p, address: to.peer.cidString)
                        self.logger.info("Dialable Addresses For \(to.peer): [\(dialableAddresses.map { $0.description }.joined(separator: ","))]")
                        return network.newRequest(to: ma, forProtocol: KadDHT.multicodec, withRequest: Data(payload), withTimeout: self.connectionTimeout).flatMapThrowing { resp in
                            try Response.decode(resp.bytes)
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
            self.dht.count().flatMap { count in
                guard count <= 2 else { return self._shareDHTKVsSequentially() }
                return self.dht.all().flatMap { elements in
                    elements.map { key, value in
                        self.eventLoop.next().submit {
                            self._shareDHTKVWithNearestPeers(key: key, value: value, nearestPeers: 3)
                        }.transform(to: ())
                    }.flatten(on: self.eventLoop)
                }
            }
        }

        private func _shareDHTKVsSequentially2() -> EventLoopFuture<Void> {
            let group = MultiThreadedEventLoopGroup(numberOfThreads: 2)
            return self.dht.all().flatMap { elements in
                elements.compactMap { key, value in
                    group.next().flatSubmit {
                        self._shareDHTKVWithNearestPeers(key: key, value: value, nearestPeers: 3).transform(to: ())
                    }
                }.flatten(on: self.eventLoop).always { _ in
                    group.shutdownGracefully(queue: .global()) { _ in print("DHT KV ELG shutdown") }
                }
            }
        }

        private var kvsToShare: [(key: KadDHT.Key, value: DHT.Record)] = []
        private func _shareDHTKVsSequentially(concurrentSharers workers: Int = 4) -> EventLoopFuture<Void> {
            self.eventLoop.flatSubmit {
                guard workers >= 1 else { self.logger.warning("Invalid Worker Count"); return self.eventLoop.makeSucceededVoidFuture() }
                guard self.kvsToShare.isEmpty else { self.logger.warning("Already Sharing KVs, skipping..."); return self.eventLoop.makeSucceededVoidFuture() }
                return self.dht.all().flatMap { elements in
                    self.kvsToShare = elements

                    /// Launch concurrent recursive share routines...
                    self.logger.notice("Launching \(workers) workers in order to share \(self.kvsToShare.count) KV pairs!")
                    return (0..<workers).map { idx in
                        self._recursiveShare().always { _ in
                            self.logger.notice("DHTKVSharer[\(idx)]::Done Sharing DHT KVs")
                        }
                    }.flatten(on: self.eventLoop)
                }.always { _ in
                    self.logger.notice("Done Sharing DHT KVs")
                }
            }
        }

        private func _recursiveShare() -> EventLoopFuture<Void> {
            self.eventLoop.flatSubmit {
                if let kv = self.kvsToShare.popLast() {
                    return self._shareDHTKVWithNearestPeers(key: kv.key, value: kv.value, nearestPeers: 3).flatMap { _ in
                        self._recursiveShare()
                    }
                } else {
                    return self.eventLoop.makeSucceededVoidFuture()
                }
            }
        }

        /// Given a KV pair, this method will find the nearest X peers and attempt to share the KV with them.
        private func _shareDHTKVWithNearestPeers(key: KadDHT.Key, value: DHT.Record, nearestPeers peerCount: Int) -> EventLoopFuture<Bool> {
            self.logger.notice("Sharing \(KadDHT.keyToHumanReadableString(key.original)) with the \(peerCount) closest peers")
            var successfulPuts: [PeerID] = []
            return self._nearest(peerCount, peersToKey: key).flatMap { nearestPeers -> EventLoopFuture<Bool> in
                nearestPeers.compactMap { peer -> EventLoopFuture<Bool> in
                    self._sendQuery(.putValue(key: key.original, record: value), to: peer).flatMapAlways { result -> EventLoopFuture<Bool> in
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
                    self.logger.notice("Done Sharing Key:\(KadDHT.keyToHumanReadableString(key.original)) with \(successfulPuts.count)/\(nearestPeers.count) peers")
                }
            }
        }

        // - TODO: Implement me
//        private func _shareProviderRecords() -> EventLoopFuture<Void> { ... }

        /// Checks if the peer specified has announced the "/ipfs/kad/1.0.0" protocol in their Indentify packet.
        /// - Parameter pid: The PeerID to check
        /// - Returns: True if this peer is announcing the /ipfs/kad/1.0.0 protocol
        /// - Note: Peers are only supposed to announce the protocol when in server mode.
        private func _isPeerOperatingAsServer(_ pid: PeerID) -> EventLoopFuture<Bool> {
            return self.peerstore.getProtocols(forPeer: pid).map { $0.contains { $0.stringValue.contains(KadDHT.multicodec) } }
        }

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

        public func storeNew(_ key: [UInt8], value: DHTRecord) -> EventLoopFuture<Bool> {
            /// Perform key lookup and request the returned closest k peers to store the value
            let targetID = KadDHT.Key(key)
            let value = value.toProtobuf()
            return self._nearest(self.routingTable.bucketSize, peersToKey: targetID).flatMap { seeds -> EventLoopFuture<Bool> in
                let lookup = KeyLookup(host: self, target: targetID, concurrentRequests: self.maxConcurrentRequest, seeds: seeds, groupProvider: self.network!.eventLoopGroupProvider)
                /// Jump back onto our main event loop to ensure that we're not piggy backing on the lookup's eventloop that's trying to shutdown...
                return lookup.proceedForPeers().hop(to: self.eventLoop).flatMap { nearestPeers -> EventLoopFuture<Bool> in
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
                            self._sendQuery(.putValue(key: key, record: record), to: peer, on: self.eventLoop).flatMapAlways { res -> EventLoopFuture<Bool> in
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
                                        if let rec = rec {
                                            self.logger.info("Response Rec: \(rec)")
                                        } else {
                                            self.logger.info("Response Rec: NIL")
                                        }

                                        return self.eventLoop.makeSucceededFuture(rec != nil && k == key)
                                    case .failure(let error):
                                        self.logger.info("PutValue Error -> \(error)")
                                        return self.eventLoop.makeSucceededFuture(false)
                                }
                            }
                        }.flatten(on: self.eventLoop).flatMap { results -> EventLoopFuture<Bool> in
                            self.logger.warning("\(results.filter({ $0 }).count)/\(results.count) peers were able to store the value \(value)")
                            /// return true if any of the peers we're able to store the value
                            self.logger.warning("\(self.eventLoop.description)")
                            return self.eventLoop.makeSucceededFuture(results.contains { $0 == true })
                        }
                    }
                }
            }
        }

        public func get(_ key: [UInt8]) -> EventLoopFuture<DHTRecord?> {
            self.eventLoop.flatSubmit {
                let kid = KadDHT.Key(key, keySpace: .xor)
                return self.dht.getValue(forKey: kid).flatMap { value in
                    if let val = value {
                        return self.eventLoop.makeSucceededFuture( val )
                    } else {
                        return self._nearestPeerTo(kid).flatMap { peer in
                            self._lookup(key: key, from: peer, depth: 0)
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
        }

        public func getWithTrace(_ key: [UInt8]) -> EventLoopFuture<(DHTRecord?, LookupTrace)> {
            self.eventLoop.flatSubmit {
                let kid = KadDHT.Key(key, keySpace: .xor)
                let trace = LookupTrace()
                return self.dht.getValue(forKey: kid).flatMap { value in
                    if let val = value {
                        return self.eventLoop.makeSucceededFuture( (val, trace) )
                    } else {
                        return self._nearestPeerTo(kid).flatMap { peer in
                            self._lookupWithTrace(key: key, from: peer, trace: trace)
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
        }

        /// TODO: We should update this logic to use providers...
        /// -
        public func getUsingLookupList(_ key: [UInt8]) -> EventLoopFuture<DHTRecord?> {
            self.eventLoop.flatSubmit {
                let kid = KadDHT.Key(key, keySpace: .xor)
                return self.dht.getValue(forKey: kid).flatMap { value in
                    if let val = value {
                        return self.eventLoop.makeSucceededFuture(val)
                    } else {
                        return self._nearest(self.routingTable.bucketSize, peersToKey: kid).flatMap { seeds -> EventLoopFuture<DHTRecord?> in
                            let lookupList = KeyLookup(host: self, target: kid, concurrentRequests: self.maxConcurrentRequest, seeds: seeds, groupProvider: self.network!.eventLoopGroupProvider)
                            return lookupList.proceedForValue().map({ $0 }).hop(to: self.eventLoop)
                        }
                    }
                }
            }
        }

        public func getProvidersUsingLookupList(_ key: [UInt8]) -> EventLoopFuture<[PeerInfo]> {
            self.eventLoop.flatSubmit {
                let kid = KadDHT.Key(key, keySpace: .xor)

                return self._nearest(self.routingTable.bucketSize, peersToKey: kid).flatMap { seeds -> EventLoopFuture<[PeerInfo]> in
                    let lookupList = KeyLookup(host: self, target: kid, concurrentRequests: self.maxConcurrentRequest, seeds: seeds, groupProvider: self.network!.eventLoopGroupProvider)
                    return lookupList.proceedForProvider().map({ $0 }).hop(to: self.eventLoop)
                }
            }
        }

        private func _lookup(key: [UInt8], from peer: PeerInfo, depth: Int) -> EventLoopFuture<Response> {
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

        public class LookupTrace: CustomStringConvertible {
            var events: [(TimeInterval, PeerInfo, Response)]
            var depth: Int

            init() {
                self.events = []
                self.depth = 0
            }

            func add(_ query: Response, from: PeerInfo) {
                self.events.append((Date().timeIntervalSince1970, from, query))
            }

            func incrementDepth() {
                self.depth = self.depth + 1
            }

            func containsPeer(_ pInfo: PeerInfo) -> Bool {
                return self.events.contains(where: { $0.1.peer.b58String == pInfo.peer.b58String })
            }

            public var description: String {
                """
                Lookup Trace:
                \(self.eventsToDescription())
                --------------
                """
            }

            private func eventsToDescription() -> String {
                if self.events.isEmpty { return "Node had the key in their DHT" }
                else {
                    return self.events.map { "Asked: \($0.1.peer) got: \(self.responseToDescription($0.2))" }.joined(separator: "\n")
                }
            }

            private func responseToDescription(_ query: Response) -> String {
                guard case .getValue(let key, let record, let closerPeers) = query else { return "Invalid Response" }
                if let record = record { return "Result for key \(key.asString(base: .base16)): `\(record.value.asString(base: .base16))`" }
                else if !closerPeers.isEmpty { return "Closer peers [\(closerPeers.compactMap { try? PeerID(fromBytesID: $0.id.bytes).b58String }.joined(separator: "\n"))]" }
                return "Lookup Exhausted"
            }
        }

        private func _lookupWithTrace(key: [UInt8], from peer: PeerInfo, trace: LookupTrace) -> EventLoopFuture<(Response, LookupTrace)> {
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

        private var numberOfSearches: Int = 0
        /// Performs a node lookup on our the specified address, or a randomly generated one, to try and find the peers closest to it in the current network
        /// - Note: we can fudge the CPL if we're trying to discover peers for a certain k bucket
        private func _searchForPeersLookupStyle(_ peer: PeerID? = nil) -> EventLoopFuture<[PeerInfo]> {
            self.logger.info("Searching for peers... Lookup Style...")
            let addressToSearchFor: PeerID
            if let specified = peer {
                addressToSearchFor = specified
            } else if self.firstSearch || self.numberOfSearches % 3 == 0 {
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
            self.numberOfSearches += 1
            return self.routingTable.getPeerInfos().flatMap { peers -> EventLoopFuture<[PeerInfo]> in
                self.dhtPeerToPeerInfo(peers).flatMap { seeds in
                    let lookup = Lookup(host: self, target: addressToSearchFor, concurrentRequests: self.maxConcurrentRequest, seeds: seeds, groupProvider: self.network!.eventLoopGroupProvider)
                    return lookup.proceed().hop(to: self.eventLoop).flatMap { closestPeers in
                        closestPeers.compactMap { p in
                            self.logger.info("\(p.peer)")
                            return self.addPeerIfSpaceOrCloser(p)
                        }.flatten(on: self.eventLoop).map { closestPeers }
                    }
                }
            }
        }

        private func dhtPeerToPeerInfo(_ dhtPeers: [DHTPeerInfo]) -> EventLoopFuture<[PeerInfo]> {
            dhtPeers.compactMap {
                self.peerstore.getPeerInfo(byID: $0.id.b58String)
            }.flatten(on: self.eventLoop)
        }

        /// This method adds a peer to our routingTable and peerstore if we either have excess capacity or if the peer is closer to us than the furthest current peer
        private func addPeerIfSpaceOrCloser(_ peer: PeerInfo) -> EventLoopFuture<Void> {
            //guard let pid = try? PeerID(fromBytesID: peer.id.bytes), pid.b58String != self.peerID.b58String else { return self.eventLoop.makeFailedFuture( Errors.unknownPeer ) }
            return self._isPeerOperatingAsServer(peer.peer).flatMap { isQueryPeer in
                guard isQueryPeer else { return self.eventLoop.makeSucceededVoidFuture() }
                return self.routingTable.addPeer(peer.peer, isQueryPeer: true, isReplaceable: true, replacementStrategy: self.replacementStrategy).flatMap { success in
                    self.logger.trace("\(success ? "Added" : "Did not add") \(peer) to routing table")

                    return self.ensurePeerIsInPeerstore(peer: peer).map {
                        if success {
                            _ = self.markPeerAsNecessary(peer: peer.peer)
                            self.metrics.add(event: .addedPeer(peer))
                        } else {
                            self.metrics.add(event: .droppedPeer(peer, .failedToAdd))
                        }
                    }
                }
            }.flatMapAlways({ _ in
                return self.eventLoop.makeSucceededVoidFuture()
            }).hop(to: self.eventLoop)
        }

        private func markPeerAsNecessary(peer: PeerID) -> EventLoopFuture<Void> {
            self.logger.notice("Marking \(peer) as necessary")
            guard let data = try? JSONEncoder().encode(MetadataBook.PrunableMetadata(prunable: .necessary)) else { return self.eventLoop.makeSucceededVoidFuture() }
            return self.peerstore.add(metaKey: MetadataBook.Keys.Prunable.rawValue, data: data.bytes, toPeer: peer, on: self.eventLoop)
            //self.peerstore.update(metaKey: .prunableValue(.necessary), forPeer: peer)
        }

        private func markPeerAsPrunable(peer: PeerID) -> EventLoopFuture<Void> {
            self.logger.notice("Marking \(peer) as prunable")
            guard let data = try? JSONEncoder().encode(MetadataBook.PrunableMetadata(prunable: .prunable)) else { return self.eventLoop.makeSucceededVoidFuture() }
            return self.peerstore.add(metaKey: MetadataBook.Keys.Prunable.rawValue, data: data.bytes, toPeer: peer, on: self.eventLoop)
        }

        private func ensurePeerIsInPeerstore(peer: PeerInfo) -> EventLoopFuture<Void> {
            self.peerstore.getPeerInfo(byID: peer.peer.b58String, on: self.eventLoop).flatMapError { err in
                self.peerstore.add(peerInfo: peer, on: self.eventLoop).map {
                    self.metrics.add(event: .peerDiscovered(peer))
                    return peer
                }
            }.transform(to: ())
        }

        /// Itterates over a collection of peers and attempts to store each one if space or distance permits
        private func addPeersIfSpaceOrCloser(_ peers: [PeerInfo]) -> EventLoopFuture<Void> {
            return peers.map { self.addPeerIfSpaceOrCloser($0) }.flatten(on: self.eventLoop)
        }

        /// This method adds a key:value pair to our dht if we either have excess capacity or if the key is closer to us than the furthest current key in the dht
        private func addKeyIfSpaceOrCloser(key: [UInt8], value: DHT.Record, usingValidator validator: Validator, logger: Logger) -> EventLoopFuture<Response> {
            let kid = KadDHT.Key(key, keySpace: .xor)
            return self.dht.addKeyIfSpaceOrCloser(key: kid, value: value, usingValidator: validator, maxStoreSize: self.dhtSize, targetKey: KadDHT.Key(self.peerID, keySpace: .xor)).map { storedResult in
                switch storedResult {
                    case .excessSpace:
                        logger.notice("We have excess space in DHT, storing `\(key):\(value)`")
                    case .updatedValue:
                        logger.notice("We already have `\(key):\(value)` in our DHT, but this is a newer record, updating it...")
                    case .alreadyExists:
                        logger.notice("We already have `\(key):\(value)` in our DHT")
                    case .storedCloser(let furthestKey, let furthestValue):
                        logger.notice("Replaced `\(String(data: Data(furthestKey.original), encoding: .utf8) ?? "???")`:`\(String(describing: furthestValue))` with `\(key)`:`\(value)`")
                    case .notStoredFurther:
                        logger.notice("New Key Value is further away from all current key value pairs, dropping store request.")
                }
                return .putValue(key: key, record: storedResult.wasAdded ? value : nil)
            }
        }

        private func multiaddressToPeerID(_ ma: Multiaddr) -> PeerID? {
            guard let pidString = ma.getPeerID() else { return nil }
            return try? PeerID(cid: pidString)
        }

        /// Returns the closest peer to the given multiaddress, excluding ourselves
        private func nearestPeerTo(_ ma: Multiaddr) -> EventLoopFuture<Response> {
            guard let peer = self.multiaddressToPeerID(ma) else { return self.eventLoop.makeFailedFuture(Errors.unknownPeer) }

            return self.routingTable.nearest(1, peersTo: peer).flatMap { peerInfos in
                guard let nearest = peerInfos.first, nearest.id.id != self.peerID.id else {
                    return self.eventLoop.makeSucceededFuture(Response.findNode(closerPeers: []))
                }

                return self.peerstore.getPeerInfo(byID: nearest.id.b58String).map { pInfo in
                    var closerPeer: [DHT.Message.Peer] = []
                    if let p = try? DHT.Message.Peer(pInfo) {
                        closerPeer.append(p)
                    }
                    return Response.findNode(closerPeers: closerPeer)
                }
            }
        }

        /// Returns up to the specified number of closest peers to the provided multiaddress, excluding ourselves
        private func nearest(_ num: Int, toPeer peer: PeerID) -> EventLoopFuture<Response> {
            //guard let peer = self.multiaddressToPeerID(ma) else { return self.eventLoop.makeFailedFuture(Errors.unknownPeer) }

            return self.routingTable.nearest(num, peersTo: peer).flatMap { peerInfos in
                peerInfos.filter { $0.id.id != self.peerID.id }.compactMap {
                    //self.peerstore[$0.id.b58String]
                    self.peerstore.getPeerInfo(byID: $0.id.b58String)
                }.flatten(on: self.eventLoop).map { ps in
                    Response.findNode(closerPeers: ps.compactMap { try? DHT.Message.Peer($0) })
                }
            }
        }

        /// Returns the closest peer we know of to the specified key. This hashes the key using SHA256 before xor'ing it.
        private func _nearestPeerTo(_ key: String) -> EventLoopFuture<PeerInfo> {
            return self._nearestPeerTo(KadDHT.Key(key.bytes))
        }

        private func _nearestPeerTo(_ kid: KadDHT.Key) -> EventLoopFuture<PeerInfo> {
            return self.routingTable.nearestPeer(to: kid).flatMap { peer in
                if let peer = peer {
                    return self.peerstore.getPeerInfo(byID: peer.id.b58String)
                } else {
                    return self.eventLoop.makeFailedFuture( Errors.unknownPeer )
                }
            }
        }

        /// Returns up to the specified number of closest peers to the provided multiaddress, excluding ourselves
        private func _nearest(_ num: Int, peersToKey keyID: KadDHT.Key) -> EventLoopFuture<[PeerInfo]> {
            return self.routingTable.nearest(num, peersToKey: keyID).flatMap { peerInfos in
                peerInfos.filter { $0.id.id != self.peerID.id }.compactMap {
                    self.peerstore.getPeerInfo(byID: $0.id.b58String)
                }.flatten(on: self.eventLoop)
            }
        }
    }
}

extension PeerStore {
    func getPeerInfo(byID id: String, on: EventLoop? = nil) -> EventLoopFuture<PeerInfo> {
        self.getKey(forPeer: id, on: on).flatMap { key in
            self.getAddresses(forPeer: key, on: on).map { addresses in
                PeerInfo(peer: key, addresses: addresses)
            }
        }
    }
}

extension KadDHT.Node {
    func dumpPeerstore() {
        self.peerstore.dumpAll()
    }
}

extension PeerInfo: CustomStringConvertible {
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

extension KadDHT {
    static public func createPubKeyRecord(peerID: PeerID) throws -> DHTRecord {
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
