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
import NIOConcurrencyHelpers

public enum KadDHT {
    public static let multicodec: String = "/ipfs/kad/1.0.0"
    public static let multicodecLAN: String = "/ipfs/lan/kad/1.0.0"

    static let CPL_BITS_NOT_BYTES: Bool = true

    public enum Mode: Sendable {
        case client
        case server
    }

    /// A Kad DHT Node
    ///
    /// - Note: As long as we perform all mutating opperation on the specified eventloop we should be thread safe (hence the @unchecked Sendable conformance)
    public class Node: DHTCore, EventLoopService, LifecycleHandler, PeerRouting, ContentRouting, @unchecked Sendable {
        public static let key: String = "KadDHT"

        enum State: Sendable {
            case started
            case stopped
        }

        /// A `TimeAmount` expressed in seconds.
        private static func seconds(_ amount: TimeAmount) -> TimeInterval {
            TimeInterval(amount.nanoseconds) / 1_000_000_000
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

        /// Lookup concurrency (`α`), the number of requests a query path keeps in flight.
        let concurrency: Int

        /// Resiliency (`β`), how many of the closest peers must respond before a lookup is done.
        let resiliency: Int

        /// How many records a value lookup collects before stopping early. `0` searches to convergence.
        let quorum: Int

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

        /// When a given (key, provider-peer-id) provider record was added
        /// to ``providerStore``.
        var providerRecordAddedAt: [Data: Date] = [:]

        /// CIDs (as routing-table keys) for which *we* are the local
        /// provider. The renewal job walks this set during heartbeat
        /// to re-publish records before they expire on remote peers.
        /// Distinct from ``providerStore`` entries we hold on behalf
        /// of other peers.
        var localProviderKeys: Set<KadDHT.Key> = []

        /// Original CID bytes for each entry in ``localProviderKeys``.
        /// The routing-table key is derived (XOR-space hash), so we
        /// preserve the source CID for the renewal job's ADD_PROVIDER
        /// payload.
        var localProviderCIDs: [KadDHT.Key: [UInt8]] = [:]

        /// Provider records older than this are pruned on heartbeat.
        let providerRecordTTL: TimeInterval = Node.seconds(KadDHT.Defaults.provideValidity)

        /// Cadence at which we re-publish our own provider records.
        let providerRecordRepublishInterval: TimeInterval = Node.seconds(KadDHT.Defaults.reprovideInterval)

        /// The longest we hold a value record, measured from its `timeReceived` stamp.
        ///
        /// Enforced on every read and by `_pruneValues()`. Defaults to 48 hours, matching go's
        /// `DefaultMaxRecordAge`.
        let maxRecordAge: TimeInterval

        /// Cadence of the value-store GC sweep. Defaults to 24 hours, matching go's
        /// `DefaultValueGCInterval`.
        let valueGCInterval: TimeInterval

        /// When `_pruneValues()` last swept, so the sweep keeps its own (slow) cadence rather than
        /// walking the whole store on every heartbeat. `distantPast` makes the first heartbeat sweep.
        private var lastValueGC: Date = .distantPast

        /// Whether an `ADD_PROVIDER` whose `providerPeers` don't carry the sender's addresses may
        /// fall back to the address we observed the stream on. Off by default, matching go.
        let acceptObservedProviderAddress: Bool

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

        /// Refresh runs on a slower interval than the maintenance beat, see `start()`.
        private var refreshTask: RepeatedTask?

        public private(set) var state: ServiceLifecycleState = .stopped

        private var handler: LibP2P.ProtocolHandler?

        private var isRunningHeartbeat: Bool = false

        private var isRunningRefresh: Bool = false

        /// [Namespace: Validator]
        ///
        /// - Note: There's deliberately no fallback validator: a PUT for a namespace we don't have a
        ///   validator for is rejected rather than stored unchecked.
        var validators: [[UInt8]: Validator] = [:]

        /// This is why there is a "ipfs/lan/kad/1.0.0" protocol...
        let isRunningLocally: Bool

        init(
            eventLoop: EventLoop,
            network: Application,
            mode: KadDHT.Mode,
            peerID: PeerID,
            bootstrapedPeers: [PeerInfo],
            options: NodeOptions,
            peerstore: PeerStore? = nil
        ) {
            self.eventLoop = eventLoop
            self.network = network
            self.mode = mode
            self.peerID = peerID
            self.peerstore = peerstore ?? network.peers
            self.concurrency = options.concurrency
            self.resiliency = options.resiliency
            self.quorum = options.quorum
            self.connectionTimeout = options.connectionTimeout
            self.dht = EventLoopDictionary(on: eventLoop)
            self.dhtSize = options.maxKeyValueStoreSize
            self.providerStore = EventLoopDictionary(on: eventLoop)
            self.maxProviderStoreSize = options.maxProviderStoreSize
            self.maxPeers = options.maxPeers
            self.maxRecordAge = Self.seconds(options.maxRecordAge)
            self.valueGCInterval = Self.seconds(options.valueGCInterval)
            self.acceptObservedProviderAddress = options.acceptObservedProviderAddress
            self.routingTable = RoutingTable(
                eventloop: eventLoop,
                bucketSize: options.bucketSize,
                localPeerID: peerID,
                latency: options.connectionTimeout,
                peerstoreMetrics: [:],
                usefulnessGracePeriod: .minutes(5)
            )
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
                return self.routingTable.addPeer(pInfo.peer).always { result in
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
                        self.logger.warning(
                            "Failed to add \(bools.filter({ !$0 }).count) of \(bools.count) bootstrap peers"
                        )
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

        convenience init(
            network: Application,
            mode: KadDHT.Mode,
            bootstrapPeers: [PeerInfo],
            options: NodeOptions
        ) throws {
            self.init(
                eventLoop: network.eventLoopGroup.next(),
                network: network,
                mode: mode,
                peerID: network.peerID,
                bootstrapedPeers: bootstrapPeers,
                options: options
            )
        }

        public func didBoot(_ application: Application) throws {
            try self.start()
        }

        public func shutdown(_ application: Application) {
            self.stop()
        }

        public func start() throws {
            guard self.state == .stopped else {
                self.logger.warning("Already Started")
                return
            }

            guard let addy = network!.listenAddresses.first else { throw Errors.noNetwork }
            self.address =
                addy.getPeerIDString() != nil
                ? addy : try! addy.encapsulate(proto: .p2p, address: self.peerID.b58String)
            self.state = .starting

            /// Alert our app of the bootstrapped peers...
            //            for (_, pInfo) in self.peerstore {
            //                self.onPeerDiscovered?(pInfo)
            //            }
            if let opd = self.onPeerDiscovered {
                let _ = self.peerstore.all().map { peers in
                    for peer in peers {
                        opd(PeerInfo(peer: peer.id, addresses: Array(peer.addresses)))
                    }
                }
            }

            /// Two clocks, because the jobs have genuinely different periods.
            ///
            /// Store maintenance (heartbeat), provider expiry, value GC, re-publish, should run often.
            /// A routing-table refresh consists of `1 + non-empty buckets` lookups, and go
            /// runs it every 10 minutes (`DefaultRoutingTableRefreshPeriod`).
            if self.autoUpdate == true {
                self.heartbeatTask = self.eventLoop.scheduleRepeatedAsyncTask(
                    initialDelay: .milliseconds(500),
                    delay: .seconds(120),
                    notifying: nil,
                    self._heartbeat
                )
                self.refreshTask = self.eventLoop.scheduleRepeatedAsyncTask(
                    /// Give the node a few seconds to settle before refreshing
                    initialDelay: .seconds(3),
                    delay: KadDHT.Defaults.refreshInterval,
                    notifying: nil,
                    { _ in self._refreshRoutingTable() }
                )
            }

            self.state = .started

            self.logger.info("Started")
        }

        public func findPeer(peer: PeerID) -> EventLoopFuture<PeerInfo> {
            self.lookupPeer(peer).flatMap { found -> EventLoopFuture<PeerInfo> in
                if let found {
                    return self.eventLoop.makeSucceededFuture(found)
                }
                self.logger.debug("Lookup didn't turn up \(peer), falling back to our peerstore")
                return self.peerstore.getPeerInfo(byID: peer.b58String, on: self.eventLoop)
            }
        }

        /// Announces this node as a provider for `cid` to the K closest
        /// peers in the DHT.
        ///
        /// Mirrors rust-libp2p's `start_providing`:
        ///
        /// 1. Stores a local provider record for `cid` so we know we're
        ///    publishing it. Subsequent calls from this node to
        ///    ``findProviders(cid:count:)`` for the same CID will
        ///    include us in the results (the local lookup short-circuits
        ///    network queries when we already hold a record).
        /// 2. If `announce` is true: runs the iterative-closest-peers
        ///    query to find the K nearest peers to the CID and sends
        ///    each an `ADD_PROVIDER` RPC. `ADD_PROVIDER` is fire-and-forget
        ///    — go answers it with nothing — so the returned future
        ///    resolves once those requests are on the wire, *not* once the
        ///    peers have processed them. Per-peer outcomes are discarded
        ///    either way, so this has never implied anybody stored the
        ///    record; it now also doesn't imply anybody has read it yet.
        ///    Shutting the node down immediately after can therefore drop
        ///    an announce that was still draining.
        /// 3. Records the CID in ``localProviderKeys`` so the heartbeat
        ///    renewal job re-issues the announcement before TTL expiry.
        ///
        /// - Parameters:
        ///   - cid: The content identifier to announce (raw CID bytes).
        ///   - announce: When `false`, only the local provider-record
        ///     store is updated and no network RPCs are sent. Matches
        ///     the rust-libp2p / go-libp2p semantics; useful for
        ///     batched bring-up.
        public func provide(cid: [UInt8], announce: Bool) -> EventLoopFuture<Void> {
            guard let parsedCID = try? CID(cid) else {
                return self.eventLoop.makeFailedFuture(Errors.invalidCID)
            }
            /// Provider records are keyed by *multihash*, not by CID, so that every CID encoding of the
            /// same content converges on one key — this has to match `findProviders(cid:count:)`, which
            /// looks up `cid.multihash.value`. Keying by the raw CID bytes here would make our own
            /// announcements undiscoverable by any lookup, including our own.
            let providerKey = parsedCID.multihash.value
            let kid = KadDHT.Key(providerKey, keySpace: .xor)
            /// `ourAddresses()` guarantees each address carries our PeerID, which is what remote nodes
            /// validate the advertised `providerPeers` entry against before recording it.
            let myPeerInfo = PeerInfo(peer: self.peerID, addresses: self.ourAddresses())
            guard let myProviderPeer = try? DHT.Message.Peer(myPeerInfo) else {
                return self.eventLoop.makeFailedFuture(Errors.encodingError)
            }

            return self.eventLoop.flatSubmit {
                // Step 1: store the provider record locally so we can
                // serve our own findProviders queries without a round
                // trip and so the renewal job can find it.
                self.providerStore.getValue(forKey: kid, default: []).flatMap { existing in
                    let updated = existing.contains(myProviderPeer) ? existing : existing + [myProviderPeer]
                    return self.providerStore.updateValue(updated, forKey: kid)
                }.map { _ -> Void in
                    self.localProviderKeys.insert(kid)
                    self.localProviderCIDs[kid] = cid
                    self.providerRecordAddedAt[Self.providerRecordKey(kid, peerID: self.peerID)] = Date()
                }.flatMap { _ -> EventLoopFuture<Void> in
                    guard announce else { return self.eventLoop.makeSucceededVoidFuture() }
                    return self._announceProviderRecord(cid: cid, key: kid)
                }
            }
        }

        /// Runs the iterative-closest-peers query for `key` and sends
        /// `ADD_PROVIDER` to each of the K closest peers found.
        /// Factored out so the renewal job in `_republishProviderRecords`
        /// can call the same path without duplicating the lookup logic.
        ///
        /// - Note: The `ADD_PROVIDER` wire key is the CID's *multihash*, matching
        ///   ``provide(cid:announce:)`` and ``findProviders(cid:count:)``. We also advertise our own
        ///   `PeerInfo` in `providerPeers`; the receiver validates that entry against the sender's
        ///   PeerID and takes our dialable addresses from it. A receiver following go drops the
        ///   record outright when we advertise nothing, so this isn't optional.
        func _announceProviderRecord(cid: [UInt8], key kid: KadDHT.Key) -> EventLoopFuture<Void> {
            guard let providerKey = (try? CID(cid))?.multihash.value else {
                return self.eventLoop.makeFailedFuture(Errors.invalidCID)
            }
            let myPeerInfo = PeerInfo(peer: self.peerID, addresses: self.ourAddresses())
            guard let myProviderPeer = try? DHT.Message.Peer(myPeerInfo) else {
                return self.eventLoop.makeFailedFuture(Errors.encodingError)
            }

            return self.lookupClosestPeers(to: kid).flatMap { nearestPeers -> EventLoopFuture<Void> in
                let closestPeers = nearestPeers.prefix(self.routingTable.bucketSize)
                self.logger.notice("Announcing provider for cid to \(closestPeers.count) nearest peers")
                return closestPeers.map { peer in
                    self._sendQuery(
                        .addProvider(key: providerKey, providerPeers: [myProviderPeer]),
                        to: peer,
                        on: self.eventLoop
                    )
                    .flatMapAlways { _ -> EventLoopFuture<Void> in
                        // Best-effort: per-peer failures are expected.
                        // The spec only requires some-of-K to succeed for the record to remain discoverable.
                        self.eventLoop.makeSucceededVoidFuture()
                    }
                }.flatten(on: self.eventLoop).map { _ in () }
            }
        }

        /// Composite key for ``providerRecordAddedAt`` lookups —
        /// XOR-space key bytes followed by the provider's peer-id bytes.
        /// Encoded as `Data` so the dictionary can hash/compare cheaply.
        static func providerRecordKey(_ key: KadDHT.Key, peerID: PeerID) -> Data {
            var combined = Data(key.bytes)
            combined.append(contentsOf: peerID.id)
            return combined
        }

        /// Find providers for the content keyed by the given `CID`
        /// - Parameters:
        ///   - cid: The `CID` you're interested in finding providers for
        ///   - count: The number of providers to stop at. Pass `0` to search to convergence.
        /// - Returns: An array of `Multiaddr`'s that are providing the content keyed by the `CID`
        public func findProviders(cid: [UInt8], count: Int) -> EventLoopFuture<[Multiaddr]> {
            guard let cid = try? CID(cid) else { return self.eventLoop.makeFailedFuture(Errors.invalidCID) }
            /// Provider records are keyed by *multihash*, not by CID, so that every CID encoding of the same
            /// content converges on one key. `rawBuffer` would include the v1 version/codec prefix.
            return self.lookupProviders(cid.multihash.value, count: count).map { peers in
                peers.reduce(
                    into: [],
                    { partialResult, pInfo in
                        partialResult.append(contentsOf: pInfo.addresses)
                    }
                )
            }
        }

        public func heartbeat() -> EventLoopFuture<Void> {
            guard self.autoUpdate == false else {
                return self.eventLoop.makeFailedFuture(Errors.cannotCallHeartbeatWhileNodeIsInAutoUpdateMode)
            }
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
                        if let data = try? JSONEncoder().encode(MetadataBook.PrunableMetadata(prunable: .necessary))
                            .byteArray
                        {
                            self.logger.notice(
                                "Necessary Peers<\(peers.filter({ $0.metadata[MetadataBook.Keys.Prunable.rawValue] == data }).count)>"
                            )
                        }
                        self.logger.notice("ProviderStore<\(providerRecordCount)>")
                        self.logger.notice(
                            "DHT Keys<\(dhtValues.count)> [ \n\(dhtValues.map { "\($0.key)" }.joined(separator: ",\n"))]"
                        )
                        self.logger.notice(
                            "PeerStore<\(peers.count)> [ \n\(peers.map { "\($0.id.b58String)" }.joined(separator: ",\n"))]"
                        )
                        return self._pruneProviders().flatMap {
                            self._pruneValues().flatMap {
                                self._republishProviderRecords().flatMap {
                                    self._shareDHTKVs().flatMap {
                                        /// skip the RoutingTable refresh when in autoUpdate (it has it's own task).
                                        guard self.autoUpdate == false else {
                                            return self.eventLoop.makeSucceededVoidFuture()
                                        }
                                        /// When autoUpdate is off, lets perform the refresh because we don't offer
                                        /// a public api for the refresh at the moment.
                                        return self._refreshRoutingTable()
                                    }
                                }
                            }
                        }
                    }.always { _ in
                        self.logger.notice(
                            "Heartbeat Finished after \((DispatchTime.now().uptimeNanoseconds - tic.uptimeNanoseconds) / 1_000_000)ms"
                        )
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

        public var onPeerDiscovered: (@Sendable (PeerInfo) -> Void)?

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

        /// Removes provider records older than ``providerRecordTTL`` and, if the store is still over capacity,
        /// prunes the stalest keys down to ``maxProviderStoreSize``.
        ///
        /// Both expiry and capacity pruning happen in one heartbeat pass. We never prune our own provider records
        /// (entries in ``localProviderKeys``), the renewal job is responsible for their lifecycle.
        func _pruneProviders() -> EventLoopFuture<Void> {
            self.eventLoop.flatSubmit {
                let cutoff = Date().addingTimeInterval(-self.providerRecordTTL)
                self.logger.notice("Pruning expired provider entries (cutoff=\(cutoff))")
                return self._expireOldProviderRecords(before: cutoff).flatMap {
                    self.providerStore.all()
                }.flatMap { snapshot in
                    self.providerStore.prune(
                        toAmount: self.maxProviderStoreSize,
                        protecting: self.localProviderKeys,
                        freshness: self._providerKeyFreshness(snapshot)
                    )
                }
            }
        }

        /// The newest `addedAt` we hold for each provider-store key, so capacity pruning drops the
        /// stalest keys first. Keys we never tracked a timestamp for report as `.distantPast`.
        private func _providerKeyFreshness(
            _ snapshot: [EventLoopDictionary<KadDHT.Key, [DHT.Message.Peer]>.Element]
        ) -> [KadDHT.Key: Date] {
            var freshness: [KadDHT.Key: Date] = [:]
            for entry in snapshot {
                var newest = Date.distantPast
                for provider in entry.value {
                    guard let pid = try? PeerID(fromBytesID: provider.id.byteArray) else { continue }
                    if let added = self.providerRecordAddedAt[Self.providerRecordKey(entry.key, peerID: pid)],
                        added > newest
                    {
                        newest = added
                    }
                }
                freshness[entry.key] = newest
            }
            return freshness
        }

        /// Sweeps value records that have aged past `maxRecordAge`.
        ///
        /// - Note: Reads expire independently (see `getUnexpiredValue`), so a slow periodic sweep can't
        ///   cause us to serve stale records.
        private func _pruneValues() -> EventLoopFuture<Void> {
            self.eventLoop.flatSubmit {
                let now = Date()
                guard now.timeIntervalSince(self.lastValueGC) >= self.valueGCInterval else {
                    return self.eventLoop.makeSucceededVoidFuture()
                }
                self.lastValueGC = now
                self.logger.notice("pruning expired records (maxAge=\(self.maxRecordAge)s)")
                return self.dht.removeExpiredValues(maxAge: self.maxRecordAge, now: now).map { expired in
                    if expired > 0 {
                        self.logger.notice("Expired \(expired) record(s)")
                    }
                }
            }
        }

        /// Removes (key, provider-peer) entries whose `addedAt` is older
        /// than `cutoff`. Iterates the timestamp dictionary, builds a
        /// list of entries to remove, then applies them to the
        /// `providerStore`.
        func _expireOldProviderRecords(before cutoff: Date) -> EventLoopFuture<Void> {
            self.eventLoop.flatSubmit {
                // Snapshot the timestamp map so we can mutate it without
                // iterating-while-mutating. Skip records that belong to
                // us — those are managed by the renewal job.
                let staleKeys = self.providerRecordAddedAt.compactMap {
                    (compositeKey, addedAt) -> Data? in
                    guard addedAt < cutoff else { return nil }
                    return compositeKey
                }
                guard !staleKeys.isEmpty else {
                    return self.eventLoop.makeSucceededVoidFuture()
                }
                self.logger.notice("Expiring \(staleKeys.count) stale provider record entries")

                // Walk the store entry-by-entry and drop the matching
                // providers. We don't have a direct (key, peer) → drop
                // primitive, so do it via getValue/updateValue.
                return self.providerStore.all().flatMap {
                    snapshot -> EventLoopFuture<Void> in
                    var updates: [EventLoopFuture<Void>] = []
                    for entry in snapshot {
                        let kid = entry.key
                        let providers = entry.value
                        // A provider record entry is fresh if its
                        // composite-keyed addedAt entry is missing
                        // (never tracked; conservatively kept) or is
                        // newer than cutoff. Self-published entries
                        // bypass this check — the renewal job is
                        // authoritative for our own records.
                        let kept = providers.filter { provider in
                            let providerIsSelf = provider.id == Data(self.peerID.id)
                            if self.localProviderKeys.contains(kid) && providerIsSelf {
                                return true
                            }
                            /// `DHT.Message.Peer.id` holds the peer's *ID bytes* (see
                            /// `DHT.Message.Peer.init(_:)`), so it has to be decoded with
                            /// `fromBytesID`. `PeerID(marshaledPublicKey:)` expects a marshaled
                            /// protobuf public key and throws on ID bytes — which made this guard
                            /// drop every remotely-supplied provider on the first heartbeat that
                            /// saw any stale record.
                            guard let pid = try? PeerID(fromBytesID: provider.id.byteArray) else {
                                // Malformed provider id — drop it.
                                return false
                            }
                            let composite = Self.providerRecordKey(kid, peerID: pid)
                            if let added = self.providerRecordAddedAt[composite], added < cutoff {
                                return false
                            }
                            return true
                        }
                        if kept.isEmpty {
                            updates.append(self.providerStore.removeValue(forKey: kid).map { _ in () })
                        } else if kept.count != providers.count {
                            updates.append(self.providerStore.updateValue(kept, forKey: kid).map { _ in () })
                        }
                    }
                    // Drop the timestamp entries we just acted on so the
                    // map doesn't grow without bound.
                    for staleKey in staleKeys {
                        self.providerRecordAddedAt.removeValue(forKey: staleKey)
                    }
                    return EventLoopFuture.andAllSucceed(updates, on: self.eventLoop)
                }
            }
        }

        /// Re-issues `ADD_PROVIDER` to the network for every CID in
        /// ``localProviderKeys`` whose last announcement is older than
        /// ``providerRecordRepublishInterval``. The local provider
        /// record's `addedAt` is refreshed on success.
        ///
        /// Best-effort: a single failed announce doesn't stop the
        /// others. Per-CID failures will be retried on the next
        /// heartbeat.
        func _republishProviderRecords() -> EventLoopFuture<Void> {
            self.eventLoop.flatSubmit {
                let cutoff = Date().addingTimeInterval(-self.providerRecordRepublishInterval)
                let due = self.localProviderKeys.compactMap { kid -> (KadDHT.Key, [UInt8])? in
                    let composite = Self.providerRecordKey(kid, peerID: self.peerID)
                    let lastAnnounced = self.providerRecordAddedAt[composite] ?? .distantPast
                    guard lastAnnounced < cutoff else { return nil }
                    guard let cid = self.localProviderCIDs[kid] else { return nil }
                    return (kid, cid)
                }
                guard !due.isEmpty else { return self.eventLoop.makeSucceededVoidFuture() }
                self.logger.notice("Re-publishing \(due.count) local provider records")

                let announcements = due.map { (kid, cid) -> EventLoopFuture<Void> in
                    self._announceProviderRecord(cid: cid, key: kid).flatMapAlways {
                        _ -> EventLoopFuture<Void> in
                        self.providerRecordAddedAt[Self.providerRecordKey(kid, peerID: self.peerID)] = Date()
                        return self.eventLoop.makeSucceededVoidFuture()
                    }
                }
                return EventLoopFuture.andAllSucceed(announcements, on: self.eventLoop)
            }
        }

        func processGetRequest(_ req: Request) -> EventLoopFuture<LibP2P.Response<ByteBuffer>> {
            self.processRequest(req)
        }

        func processPutRequest(_ req: Request) -> EventLoopFuture<LibP2P.Response<ByteBuffer>> {
            self.processRequest(req)
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
                    /// The spec has us reset a stream we failed to serve, rather than closing it
                    /// cleanly: a clean close reads as "nothing more to say", so the peer would take
                    /// our silence for a legitimate empty answer instead of a failed request. This
                    /// matches the `.reset` the decode/auth guards in `onData` already return.
                    self.logger.warning("KadDHT::OnData::Error -> \(error)")
                    return req.eventLoop.makeSucceededFuture(.reset(error))
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
            guard let query = try? Query.decode([UInt8](request.payload.readableBytesView)) else {
                request.logger.warning("Failed to decode inbound data...")
                //return stream.reset().transform(to: nil)
                //let _ = stream.reset()
                //return request.eventLoop.makeFailedFuture(Errors.unknownPeer)
                return request.eventLoop.makeSucceededFuture(.reset(Errors.DecodingErrorInvalidType))
            }

            /// Handle the query
            ///
            /// - Important: We need to return the Response on the `request.eventLoop`.
            return request.eventLoop.flatSubmit {  //.flatScheduleTask(in: self.connection.responseTime) {
                self.trustedAddresses(for: from, observedOn: request.addr).flatMap {
                    pInfo -> EventLoopFuture<Response> in

                    /// Do we know this peer?
                    ///
                    /// Nodes, both those operating in client and server mode, add another node to their routing table if and only if that node operates in server mode.
                    /// This distinction allows restricted nodes to utilize the DHT, i.e. query the DHT, without decreasing the quality of the distributed hash table, i.e. without polluting the routing tables.
                    /// `addPeerIfSpaceOrCloser` makes that server-mode check itself.
                    _ = self.addPeerIfSpaceOrCloser(pInfo)

                    return self._handleQuery(query, from: pInfo, request: request).always { result in
                        switch result {
                        case .success(let res):
                            self.metrics.add(event: .queryResponse(pInfo, res))
                        case .failure(let error):
                            request.logger.error(
                                "Error encountered while responding to query \(query) from peer \(from) -> \(error)"
                            )
                        }
                    }
                }
            }.flatMapThrowing { resp in
                request.logger.info("---")
                request.logger.info("Responding to query \(query) with:")
                request.logger.info("\(resp)")
                request.logger.info("---")

                guard query.fireAndForgetResponse == nil else {
                    /// none of the other implementations respond to ADD_PROVIDER queries.
                    return LibP2P.Response.close
                }
                return try LibP2P.Response.respondThenClose(request.allocator.buffer(bytes: resp.encode()))
            }.hop(to: request.eventLoop)
        }

        /// The addresses we're willing to attribute to `peer` when it dials us.
        ///
        /// A requester-supplied, or it's observed, address is unverified: for an inbound stream
        /// `request.addr` is wherever we happened to see the peer, which is usually an ephemeral
        /// source port nothing can dial back. So we lead with what identify already put in the
        /// peerstore and keep the observed address only as a trailing fallback, rather than letting
        /// it be the only thing we record.
        func trustedAddresses(for peer: PeerID, observedOn observed: Multiaddr) -> EventLoopFuture<PeerInfo> {
            self.peerstore.getPeerInfo(byID: peer.b58String, on: self.eventLoop).map { known -> PeerInfo in
                guard !known.addresses.isEmpty else { return PeerInfo(peer: peer, addresses: [observed]) }
                guard !known.addresses.contains(observed) else { return known }
                return PeerInfo(peer: peer, addresses: known.addresses + [observed])
            }.flatMapErrorThrowing { _ in
                /// Nothing on file — the observed address is all we have to go on.
                PeerInfo(peer: peer, addresses: [observed])
            }
        }

        /// Switches over the Query Type and Handles each appropriately
        func _handleQuery(_ query: Query, from: PeerInfo, request: Request) -> EventLoopFuture<Response> {
            request.logger.notice("Query::Handling Query \(query) from peer \(from.peer)")
            switch query {
            case .ping:
                return self.eventLoop.makeSucceededFuture(Response.ping)

            case .findNode(let key):
                /// If they're looking for us, tell them about us.
                ///
                /// - Note: go's `handleFindPeer` answers `[self]` in this case. Returning our *neighbours*
                ///   instead (which is what we used to do, since `_nearest` filters us out) means a peer
                ///   asking us directly for our own address never learns it.
                if key == self.peerID.id {
                    return self.eventLoop.submit {
                        guard
                            let us = try? DHT.Message.Peer(PeerInfo(peer: self.peerID, addresses: self.ourAddresses()))
                        else { return Response.findNode(closerPeers: []) }
                        return Response.findNode(closerPeers: [us])
                    }
                } else {
                    /// Otherwise return the k closest peers we know of to `key` in XOR space.
                    ///
                    /// - Note: `key` is an arbitrary Kademlia key (canonical FIND_NODE), so we hash the raw
                    ///   bytes into key space via `KadDHT.Key(_:)` rather than requiring a PeerID. When the
                    ///   key is a PeerId's bytes this is identical to the old `KadDHT.Key(peerID)` path, so
                    ///   swift↔swift lookups are unchanged.
                    return self.nearest(
                        self.routingTable.bucketSize,
                        toKey: KadDHT.Key(key, keySpace: .xor),
                        excluding: from.peer
                    )
                }

            case .putValue(let key, let value):
                request.logger.notice("🚨🚨🚨 PutValue Request 🚨🚨🚨")
                request.logger.notice("DHTRecordKey(HEX)::\(key.toHexString())")
                request.logger.notice("DHTRecordValue(HEX)::\((try? value.serializedData().toHexString()) ?? "NIL")")
                guard let namespace = KadDHT.extractNamespace(key) else {
                    request.logger.warning("Failed to extract namespace for DHT PUT request")
                    request.logger.warning("DHTRecordKey(HEX)::\(key.toHexString())")
                    request.logger.warning(
                        "DHTRecordValue(HEX)::\((try? value.serializedData().toHexString()) ?? "NIL")"
                    )
                    return self.eventLoop.makeSucceededFuture(.putValue(key: key, record: nil))
                }

                guard let validator = self.validators[namespace] else {
                    request.logger.warning(
                        "Query::PutValue::No Validator Set For Namespace '\(String(data: Data(namespace), encoding: .utf8) ?? "???")'"
                    )
                    return self.eventLoop.makeSucceededFuture(.putValue(key: key, record: nil))
                }

                /// Validators see the record's value bytes
                guard (try? validator.validate(key: key, value: value.value.byteArray)) != nil else {
                    request.logger.warning(
                        "Query::PutValue::KeyVal failed validation for namespace '\(String(data: Data(namespace), encoding: .utf8) ?? "???")'"
                    )
                    return self.eventLoop.makeSucceededFuture(.putValue(key: key, record: nil))
                }

                request.logger.notice(
                    "Query::PutValue::KeyVal passed validation for namespace '\(String(data: Data(namespace), encoding: .utf8) ?? "???")'"
                )
                request.logger.notice(
                    "Query::PutValue::Attempting to store value for key: \(KadDHT.keyToHumanReadableString(key))"
                )
                return self.addKeyIfSpaceOrCloser(
                    key: key,
                    value: value,
                    usingValidator: validator,
                    logger: request.logger
                )
            //return self.addKeyIfSpaceOrCloser2(key: key, value: value, from: from)

            case .getValue(let key):
                /// If we have the value, send it back!
                let kid = KadDHT.Key(key, keySpace: .xor)
                request.logger.notice("Query::GetValue::\(KadDHT.keyToHumanReadableString(key))")
                /// - Note: We attach the k closest peers whether or not we hold the record. go's
                ///   `handleGetValue` sets `Record` and `CloserPeers` unconditionally; only sending closer
                ///   peers on a miss truncates the requester's lookup at the first hop that has a copy.
                return self.dht.getUnexpiredValue(forKey: kid, maxAge: self.maxRecordAge).and(
                    self._nearest(self.routingTable.bucketSize, peersToKey: kid, excluding: from.peer)
                ).map { value, peers in
                    request.logger.notice(
                        "Query::GetValue::Returning \(value == nil ? "no value" : "value") and \(peers.count) closer peers for key: \(KadDHT.keyToHumanReadableString(key))"
                    )
                    return Response.getValue(
                        key: key,
                        record: value,
                        closerPeers: peers.compactMap { try? DHT.Message.Peer($0) }
                    )
                }

            case .getProviders(let key):
                /// - Note: The key here is a multihash, not a CID, so we don't try to parse it. go's
                ///   `handleGetProviders` only bounds the length, which `Query.decode` has already done.
                ///   Rejecting anything that isn't a valid CID broke every non-sha2-256 multihash.
                let kid = KadDHT.Key(key, keySpace: .xor)

                /// Return the providers we know of *and* the k closest peers, matching go.
                return self.providerStore.getValue(forKey: kid).and(
                    self._nearest(self.routingTable.bucketSize, peersToKey: kid, excluding: from.peer)
                ).map { providers, peers in
                    let providers = providers ?? []
                    request.logger.notice(
                        "Query::GetProviders::Returning \(providers.count) providers and \(peers.count) closer peers for key: \(KadDHT.keyToHumanReadableString(key))"
                    )
                    return Response.getProviders(
                        cid: key,
                        providerPeers: providers,
                        closerPeers: peers.compactMap { try? DHT.Message.Peer($0) }
                    )
                }

            case .addProvider(let key, let advertised):
                let kid = KadDHT.Key(key, keySpace: .xor)

                /// Only record the advertised `providerPeers` entries that match the sender's PeerID, per the
                /// spec ("validate that the received PeerInfo matches the sender's peerID"). Those entries
                /// are where the provider's dialable addresses come from — we used to discard them entirely
                /// and store only the address we happened to observe this connection on.
                let provider: DHT.Message.Peer?
                if let matching = advertised.first(where: { $0.id == Data(from.peer.id) }),
                    let matchingInfo = try? matching.toPeerInfo(),
                    !matchingInfo.addresses.isEmpty
                {
                    /// Union the addresses they advertised with the one we observed.
                    let addresses =
                        matchingInfo.addresses + from.addresses.filter { !matchingInfo.addresses.contains($0) }
                    provider = try? DHT.Message.Peer(PeerInfo(peer: from.peer, addresses: addresses))
                } else if self.acceptObservedProviderAddress {
                    request.logger.warning(
                        "Query::AddProvider::No providerPeers entry matched sender \(from.peer), falling back to the observed address"
                    )
                    provider = try? DHT.Message.Peer(from)
                } else {
                    /// go's `handleAddProvider` drops a record whose matching `providerPeers` entry
                    /// carries no addresses. Substituting the address we observed would publish an
                    /// ephemeral source port as a provider address, so every peer that later asked us
                    /// for providers would get an answer it can't dial.
                    request.logger.warning(
                        "Query::AddProvider::Dropping record from \(from.peer) — no providerPeers entry advertised the sender's addresses"
                    )
                    return self.eventLoop.makeSucceededFuture(Response.addProvider(cid: key, providerPeers: []))
                }

                guard let provider else {
                    return self.eventLoop.makeSucceededFuture(Response.addProvider(cid: key, providerPeers: []))
                }

                let timestampKey = Self.providerRecordKey(kid, peerID: from.peer)

                return self.providerStore.getValue(forKey: kid).flatMap { existingProviders in
                    let existingProviders = existingProviders ?? []
                    /// - Note: This condition used to be inverted, so a provider we'd never seen was
                    ///   reported as "already a provider" and dropped, while a provider we already had was
                    ///   appended a second time. Since the store always started empty, nothing was ever
                    ///   recorded and GET_PROVIDERS could never return anybody.
                    guard !existingProviders.contains(provider) else {
                        /// Refresh the record's `addedAt` so the re-publish we just received keeps the entry
                        /// alive past `providerRecordTTL` — otherwise a provider that faithfully renews every
                        /// republish interval would still be expired by `_pruneProviders`.
                        self.providerRecordAddedAt[timestampKey] = Date()
                        request.logger.notice(
                            "Query::AddProvider::\(from.peer) refreshed as a provider for key: \(KadDHT.keyToHumanReadableString(key))"
                        )
                        return self.eventLoop.makeSucceededFuture(
                            Response.addProvider(cid: key, providerPeers: [provider])
                        )
                    }
                    return self.providerStore.updateValue(existingProviders + [provider], forKey: kid).map { _ in
                        self.providerRecordAddedAt[timestampKey] = Date()
                        request.logger.notice(
                            "Query::AddProvider::Added \(from.peer) as a provider for key: \(KadDHT.keyToHumanReadableString(key))"
                        )
                        return Response.addProvider(cid: key, providerPeers: [provider])
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
                queryPromise.fail(Errors.connectionTimedOut)
            }

            //self.logger.info("Scanning \(to) for dialable addresses...")
            queryPromise.completeWith(
                /// We should loop through the addresses and determine which one to dial
                /// - Any already open?
                /// - If not, any preferred transports?
                network.dialableAddress(
                    to.addresses,
                    externalAddressesOnly: !self.isRunningLocally,
                    on: on ?? self.eventLoop
                ).flatMap { dialableAddresses in
                    guard !dialableAddresses.isEmpty else {
                        return (on ?? self.eventLoop).makeFailedFuture(Errors.noDialableAddressesForPeer)
                    }
                    guard let addy = dialableAddresses.first else {
                        return (on ?? self.eventLoop).makeFailedFuture(Errors.peerIDMultiaddrEncapsulationFailed)
                    }
                    do {
                        /// We encapsulate the multiaddr with the peers expected public key so we can verify the responder is who we're expecting.
                        let ma =
                            addy.getPeerIDString() != nil
                            ? addy : try addy.encapsulate(proto: .p2p, address: to.peer.b58String)
                        //let ma = addy.addresses.contains(where: { $0.codec == .p2p }) ? addy : try addy.encapsulate(proto: .p2p, address: to.peer.cidString)
                        self.logger.info(
                            "Dialable Addresses For \(to.peer): [\(dialableAddresses.map { $0.description }.joined(separator: ","))]"
                        )
                        /// An RPC the peer doesn't answer resolves as soon as our write is out
                        let fireAndForget = query.fireAndForgetResponse
                        return network.newRequest(
                            to: ma,
                            forProtocol: KadDHT.multicodec,
                            withRequest: Data(payload),
                            style: fireAndForget == nil ? .responseExpected : .noResponseExpected,
                            withTimeout: self.connectionTimeout
                        ).flatMapThrowing { resp -> Response in
                            if let fireAndForget { return fireAndForget }
                            return try Response.decode(resp.byteArray)
                        }
                    } catch {
                        return (on ?? self.eventLoop).makeFailedFuture(Errors.peerIDMultiaddrEncapsulationFailed)
                    }
                }
            )

            return queryPromise.futureResult
        }

        /// For each KV in our DHT, we send it to the closest two peers we know of (excluding us)
        private func _shareDHTKVs() -> EventLoopFuture<Void> {
            self.dht.count().flatMap { count in
                guard count <= 2 else { return self._shareDHTKVsSequentially() }
                return self.dht.unexpiredValues(maxAge: self.maxRecordAge).flatMap { elements in
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
                guard workers >= 1 else {
                    self.logger.warning("Invalid Worker Count")
                    return self.eventLoop.makeSucceededVoidFuture()
                }
                guard self.kvsToShare.isEmpty else {
                    self.logger.warning("Already Sharing KVs, skipping...")
                    return self.eventLoop.makeSucceededVoidFuture()
                }
                return self.dht.unexpiredValues(maxAge: self.maxRecordAge).flatMap { elements in
                    self.kvsToShare = elements

                    /// Launch concurrent recursive share routines...
                    self.logger.notice(
                        "Launching \(workers) workers in order to share \(self.kvsToShare.count) KV pairs!"
                    )
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
                    return self._shareDHTKVWithNearestPeers(key: kv.key, value: kv.value, nearestPeers: 3).flatMap {
                        _ in
                        self._recursiveShare()
                    }
                } else {
                    return self.eventLoop.makeSucceededVoidFuture()
                }
            }
        }

        /// Given a KV pair, this method will find the nearest X peers and attempt to share the KV with them.
        private func _shareDHTKVWithNearestPeers(
            key: KadDHT.Key,
            value: DHT.Record,
            nearestPeers peerCount: Int
        ) -> EventLoopFuture<Bool> {
            self.logger.notice(
                "Sharing \(KadDHT.keyToHumanReadableString(key.original)) with the \(peerCount) closest peers"
            )
            let successfulPuts: NIOLockedValueBox<[PeerID]> = .init([])
            return self._nearest(peerCount, peersToKey: key).flatMap { nearestPeers -> EventLoopFuture<Bool> in
                nearestPeers.compactMap { peer -> EventLoopFuture<Bool> in
                    self._sendQuery(.putValue(key: key.original, record: value), to: peer).flatMapAlways {
                        result -> EventLoopFuture<Bool> in
                        switch result {
                        case .success(let res):
                            self.logger.debug("Shared key:value with \(peer.peer)")
                            guard case .putValue(let k, let v) = res else {
                                self.logger.warning("Failed to share key:value with \(peer.peer)")
                                return self.eventLoop.makeSucceededFuture(false)
                            }
                            guard k == key.original, v != nil else {
                                self.logger.warning("Failed to share key:value with \(peer.peer)")
                                return self.eventLoop.makeSucceededFuture(false)
                            }
                            self.logger.debug("They Stored It!")
                            successfulPuts.withLockedValue { $0.append(peer.peer) }
                            return self.eventLoop.makeSucceededFuture(true)

                        case .failure(let error):
                            self.logger.warning("Failed to share key:value with \(peer.peer) -> \(error)")
                            return self.eventLoop.makeSucceededFuture(false)
                        }
                    }
                }.flatten(on: self.eventLoop).map({ $0.contains(true) }).always { results in
                    self.logger.notice(
                        "Done Sharing Key:\(KadDHT.keyToHumanReadableString(key.original)) with \(successfulPuts.withLockedValue({$0}).count)/\(nearestPeers.count) peers"
                    )
                }
            }
        }

        /// The DHT protocols we treat as evidence that a peer is operating as a server.
        private var dhtProtocols: [String] {
            self.isRunningLocally ? [KadDHT.multicodec, KadDHT.multicodecLAN] : [KadDHT.multicodec]
        }

        /// Checks if the peer specified has announced the "/ipfs/kad/1.0.0" protocol in their Indentify packet.
        /// - Parameter pid: The PeerID to check
        /// - Returns: True if this peer is announcing the "/ipfs/kad/1.0.0" protocol (or lan version)
        /// - Note: Peers are only supposed to announce the protocol when in server mode.
        private func _isPeerOperatingAsServer(_ pid: PeerID) -> EventLoopFuture<Bool> {
            let known = self.dhtProtocols
            return self.peerstore.getProtocols(forPeer: pid).map { announced in
                announced.contains { proto in known.contains { proto == $0 } }
            }
        }

        public func stop() {
            guard self.state == .started || self.state == .starting else {
                self.logger.warning("Already stopped")
                return
            }
            self.state = .stopping
            /// - Note: not waited on, unlike the heartbeat below. `stop()` already blocks on one
            ///   promise and waiting on a refresh mid-fan-out would hold
            ///   shutdown for up to `refreshQueryTimeout` per outstanding lookup.
            self.refreshTask?.cancel()
            self.refreshTask = nil
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
            /// Perform key lookup and request the returned closest k peers to store the value.
            ///
            /// Local-first: store on our own kv before fanning the
            /// value out to closest peers. Mirrors ``provide(cid:announce:)``,
            /// which writes its provider record to the local
            /// ``providerStore`` before announcing. Two consequences:
            ///
            /// 1. The publisher's own ``getUsingLookupList`` calls
            ///    for this key succeed immediately, without a round
            ///    trip.
            /// 2. Remote peers performing iterative GET_VALUE on
            ///    this key can resolve it against the publisher
            ///    once the publisher is in their routing table —
            ///    even if the publisher's own routing table was
            ///    empty at storeNew time (e.g. bootstrap addition
            ///    hadn't completed yet).
            ///
            /// `storeNew` therefore returns `true` whenever the
            /// local write succeeds, even if no closest-peer
            /// putValue accepts. Best-effort announce semantics —
            /// the heartbeat's ``_shareDHTKVs`` will continue
            /// pushing the value out to closer peers on its own
            /// cadence.
            let targetID = KadDHT.Key(key)
            let value = value.toProtobuf()

            /// Stamp the local copy only, the outbound `putValue` below is built separately and
            /// deliberately leaves `timeReceived` empty, matching go's `MakePutRecord`.
            return self.dht.updateValue(KadDHT.timeStamped(value), forKey: targetID).flatMap {
                _ -> EventLoopFuture<Bool> in
                self.logger.notice(
                    "storeNew: stored locally key=\(KadDHT.keyToHumanReadableString(key))"
                )
                return self.lookupClosestPeers(to: targetID).flatMap {
                    nearestPeers -> EventLoopFuture<Bool> in
                    /// We have the closest peers to this key that the network knows of, so ask each
                    /// of the k closest to store the value.
                    let closestPeers = nearestPeers.prefix(self.routingTable.bucketSize)
                    self.logger.notice("Asking the closest \(closestPeers.count) peers to store our value")

                    // Don't set timeReceived on the way out...
                    var record = DHT.Record()
                    record.key = value.key
                    record.value = value.value

                    return closestPeers.map { peer in
                        self._sendQuery(.putValue(key: key, record: record), to: peer, on: self.eventLoop)
                            .flatMapAlways { res -> EventLoopFuture<Bool> in
                                switch res {
                                case .success(let response):
                                    guard case .putValue(let k, let rec) = response else {
                                        return self.eventLoop.makeSucceededFuture(false)
                                    }
                                    return self.eventLoop.makeSucceededFuture(rec != nil && k == key)
                                case .failure(let error):
                                    self.logger.debug("PutValue to \(peer.peer) failed: \(error)")
                                    return self.eventLoop.makeSucceededFuture(false)
                                }
                            }
                    }.flatten(on: self.eventLoop).flatMap { results -> EventLoopFuture<Bool> in
                        self.logger.notice(
                            "storeNew: \(results.filter({ $0 }).count)/\(results.count) peers accepted the value (local copy stored regardless)"
                        )
                        /// Local-first semantics: the value is already stored locally, return true.
                        /// Remote acceptance is optional, the heartbeat's ``_shareDHTKVs`` will continue propagating the value.
                        return self.eventLoop.makeSucceededFuture(true)
                    }
                }
            }
        }

        /// The value stored under `key`, ours if we hold it, otherwise the best the network has.
        public func get(_ key: [UInt8]) -> EventLoopFuture<DHTRecord?> {
            self.getLocalOrLookup(key).map { $0 }
        }

        /// ``get(_:)`` with a record of every peer the lookup asked and what they answered.
        public func getWithTrace(_ key: [UInt8]) -> EventLoopFuture<(DHTRecord?, LookupTrace)> {
            let trace = LookupTrace()
            return self.getLocalOrLookup(key, trace: trace).map { ($0, trace) }
        }

        @available(*, deprecated, renamed: "get(_:)")
        public func getUsingLookupList(_ key: [UInt8]) -> EventLoopFuture<DHTRecord?> {
            self.get(key)
        }

        @available(*, deprecated, message: "Use findProviders(cid:count:), or lookupProviders(_:count:)")
        public func getProvidersUsingLookupList(_ key: [UInt8]) -> EventLoopFuture<[PeerInfo]> {
            self.lookupProviders(key, count: 0)
        }

        /// Local-first read: our own store, then an iterative GET_VALUE across the network.
        private func getLocalOrLookup(_ key: [UInt8], trace: LookupTrace? = nil) -> EventLoopFuture<DHT.Record?> {
            self.eventLoop.flatSubmit {
                let kid = KadDHT.Key(key, keySpace: .xor)
                return self.dht.getUnexpiredValue(forKey: kid, maxAge: self.maxRecordAge).flatMap { value in
                    if let value {
                        return self.eventLoop.makeSucceededFuture(value)
                    }
                    return self.lookupValue(key, quorum: self.quorum, trace: trace)
                }
            }
        }

        /// Every response a lookup collected, in arrival order.
        public final class LookupTrace: CustomStringConvertible, Sendable {
            struct Event: Sendable {
                let time: TimeInterval
                let peer: PeerInfo
                let response: Response
            }

            let events: NIOLockedValueBox<[Event]>

            var depth: Int {
                get { _depth.withLockedValue { $0 } }
                set { _depth.withLockedValue { $0 = newValue } }
            }
            let _depth: NIOLockedValueBox<Int>

            init() {
                self.events = .init([])
                self._depth = .init(0)
            }

            func add(_ query: Response, from: PeerInfo) {
                self.events.withLockedValue { e in
                    e.append(.init(time: Date().timeIntervalSince1970, peer: from, response: query))
                }
            }

            func incrementDepth() {
                self.depth = self.depth + 1
            }

            func containsPeer(_ pInfo: PeerInfo) -> Bool {
                self.events.withLockedValue { e in
                    // TODO: Should we be comparing the b58Strings or the PeerIDs directly
                    e.contains(where: { $0.peer.peer.b58String == pInfo.peer.b58String })
                }
            }

            public var description: String {
                """
                Lookup Trace:
                \(self.eventsToDescription())
                --------------
                """
            }

            private func eventsToDescription() -> String {
                self.events.withLockedValue { e in
                    if e.isEmpty {
                        return "Node had the key in their DHT"
                    } else {
                        return e.map {
                            "Asked: \($0.peer.peer) got: \(self.responseToDescription($0.response))"
                        }.joined(separator: "\n")
                    }
                }
            }

            private func responseToDescription(_ query: Response) -> String {
                guard case .getValue(let key, let record, let closerPeers) = query else { return "Invalid Response" }
                if let record = record {
                    return "Result for key \(key.asString(base: .base16)): `\(record.value.asString(base: .base16))`"
                } else if !closerPeers.isEmpty {
                    return
                        "Closer peers [\(closerPeers.compactMap { try? PeerID(fromBytesID: $0.id.byteArray).b58String }.joined(separator: "\n"))]"
                }
                return "Lookup Exhausted"
            }
        }

        /// Walks the key space we care about, so the routing table stays populated and current.
        ///
        /// One self-lookup, which refreshes our closest peers, the part of the table that matters
        /// most and the part `maxRefreshPrefixLength` wont reach, followed by one targeted
        /// lookup per each non-empty bucket.
        ///
        /// Lookups run one at a time. A refresh is background maintenance, so it yields to whatever
        /// else the node is doing rather than opening `k * α` streams at once.
        func _refreshRoutingTable() -> EventLoopFuture<Void> {
            self.eventLoop.flatSubmit {
                guard self.isRunningRefresh == false else {
                    self.logger.debug("Refresh already in flight, skipping this cycle")
                    return self.eventLoop.makeSucceededVoidFuture()
                }
                self.isRunningRefresh = true
                let tic = DispatchTime.now()

                return self.routingTable.nonEmptyBucketPrefixLengths().flatMap {
                    prefixLengths -> EventLoopFuture<Void> in
                    let refreshable = prefixLengths.filter { $0 <= KadDHT.Defaults.maxRefreshPrefixLength }
                    if refreshable.count < prefixLengths.count {
                        self.logger.debug(
                            "Leaving \(prefixLengths.count - refreshable.count) deep bucket(s) to the self-lookup"
                        )
                    }
                    self.logger.notice("Refreshing self + \(refreshable.count) bucket(s)")

                    /// Our own key first, it's the one lookup we always run.
                    let targets = [KadDHT.Key(self.peerID, keySpace: .xor)] + refreshable.compactMap { cpl in
                        self.refreshTarget(forBucketAtPrefixLength: cpl)
                    }

                    /// Perform a lookup for each non-empty bucket (one at a time, so we dont overwhelm the node with concurrent requests)
                    return targets.reduce(self.eventLoop.makeSucceededVoidFuture()) { chain, target in
                        chain.flatMap { _ in
                            self.lookupClosestPeers(
                                to: target,
                                timeout: KadDHT.Defaults.refreshQueryTimeout
                            ).flatMapAlways { result -> EventLoopFuture<Void> in
                                /// Dont fail the rest of our lookups when one of them fails.
                                if case .failure(let error) = result {
                                    self.logger.debug("Refresh lookup failed: \(error)")
                                }
                                return self.eventLoop.makeSucceededVoidFuture()
                            }
                        }
                    }
                }.always { _ in
                    self.logger.notice(
                        "Refresh finished after \((DispatchTime.now().uptimeNanoseconds - tic.uptimeNanoseconds) / 1_000_000)ms"
                    )
                    self.isRunningRefresh = false
                }
            }
        }

        /// A lookup target that lands in the bucket `cpl` bits away from us.
        private func refreshTarget(forBucketAtPrefixLength cpl: Int) -> KadDHT.Key? {
            let target = KadDHT.Key.random(
                commonPrefixLength: cpl,
                with: KadDHT.Key(self.peerID, keySpace: .xor)
            )
            if target == nil {
                self.logger.debug("Couldn't find a refresh target for bucket \(cpl), skipping it")
            }
            return target
        }

        /// This method adds a peer to our routingTable and peerstore if we either have excess capacity or if the peer is closer to us than the furthest current peer
        private func addPeerIfSpaceOrCloser(_ peer: PeerInfo) -> EventLoopFuture<Void> {
            //guard let pid = try? PeerID(fromBytesID: peer.id.bytes), pid.b58String != self.peerID.b58String else { return self.eventLoop.makeFailedFuture( Errors.unknownPeer ) }
            self._isPeerOperatingAsServer(peer.peer).flatMap { isQueryPeer in
                guard isQueryPeer else { return self.eventLoop.makeSucceededVoidFuture() }
                return self.routingTable.addPeer(
                    peer.peer,
                    isQueryPeer: true,
                    isReplaceable: true,
                    replacementStrategy: self.replacementStrategy
                ).flatMap { success in
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
                self.eventLoop.makeSucceededVoidFuture()
            }).hop(to: self.eventLoop)
        }

        private func markPeerAsNecessary(peer: PeerID) -> EventLoopFuture<Void> {
            self.logger.notice("Marking \(peer) as necessary")
            guard let data = try? JSONEncoder().encode(MetadataBook.PrunableMetadata(prunable: .necessary)) else {
                return self.eventLoop.makeSucceededVoidFuture()
            }
            return self.peerstore.add(
                metaKey: MetadataBook.Keys.Prunable.rawValue,
                data: data.byteArray,
                toPeer: peer,
                on: self.eventLoop
            )
            //self.peerstore.update(metaKey: .prunableValue(.necessary), forPeer: peer)
        }

        private func markPeerAsPrunable(peer: PeerID) -> EventLoopFuture<Void> {
            self.logger.notice("Marking \(peer) as prunable")
            guard let data = try? JSONEncoder().encode(MetadataBook.PrunableMetadata(prunable: .prunable)) else {
                return self.eventLoop.makeSucceededVoidFuture()
            }
            return self.peerstore.add(
                metaKey: MetadataBook.Keys.Prunable.rawValue,
                data: data.byteArray,
                toPeer: peer,
                on: self.eventLoop
            )
        }

        private func ensurePeerIsInPeerstore(peer: PeerInfo) -> EventLoopFuture<Void> {
            self.peerstore.getPeerInfo(byID: peer.peer.b58String, on: self.eventLoop).flatMapError { err in
                self.peerstore.add(peerInfo: peer, on: self.eventLoop).map {
                    self.metrics.add(event: .peerDiscovered(peer))
                    return peer
                }
            }.transform(to: ())
        }

        /// Iterates over a collection of peers and attempts to store each one if space or distance permits
        func addPeersIfSpaceOrCloser(_ peers: [PeerInfo]) -> EventLoopFuture<Void> {
            peers.map { self.addPeerIfSpaceOrCloser($0) }.flatten(on: self.eventLoop)
        }

        /// This method adds a key:value pair to our dht if we either have excess capacity or if the key is closer to us than the furthest current key in the dht
        private func addKeyIfSpaceOrCloser(
            key: [UInt8],
            value: DHT.Record,
            usingValidator validator: Validator,
            logger: Logger
        ) -> EventLoopFuture<Response> {
            let kid = KadDHT.Key(key, keySpace: .xor)
            return self.dht.addKeyIfSpaceOrCloser(
                key: kid,
                value: KadDHT.timeStamped(value),
                usingValidator: validator,
                maxStoreSize: self.dhtSize,
                targetKey: KadDHT.Key(self.peerID, keySpace: .xor)
            ).map { storedResult in
                switch storedResult {
                case .excessSpace:
                    logger.notice("We have excess space in DHT, storing `\(key):\(value)`")
                case .updatedValue:
                    logger.notice(
                        "We already have `\(key):\(value)` in our DHT, but this is a newer record, updating it..."
                    )
                case .alreadyExists:
                    logger.notice("We already have `\(key):\(value)` in our DHT")
                case .storedCloser(let furthestKey, let furthestValue):
                    logger.notice(
                        "Replaced `\(String(data: Data(furthestKey.original), encoding: .utf8) ?? "???")`:`\(String(describing: furthestValue))` with `\(key)`:`\(value)`"
                    )
                case .notStoredFurther:
                    logger.notice(
                        "New Key Value is further away from all current key value pairs, dropping store request."
                    )
                }
                return .putValue(key: key, record: storedResult.wasAdded ? value : nil)
            }
        }

        private func multiaddressToPeerID(_ ma: Multiaddr) -> PeerID? {
            try? ma.getPeerID()
        }

        /// Returns the closest peer to the given multiaddress, excluding ourselves
        private func nearestPeerTo(_ ma: Multiaddr) -> EventLoopFuture<Response> {
            guard let peer = self.multiaddressToPeerID(ma) else {
                return self.eventLoop.makeFailedFuture(Errors.unknownPeer)
            }

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

        /// Returns a `findNode` response containing up to `num` of the closest peers we know of to `key`,
        /// excluding ourselves and (optionally) the peer that asked.
        private func nearest(
            _ num: Int,
            toKey key: KadDHT.Key,
            excluding requester: PeerID? = nil
        ) -> EventLoopFuture<Response> {
            self._nearest(num, peersToKey: key, excluding: requester).map { ps in
                Response.findNode(closerPeers: ps.compactMap { try? DHT.Message.Peer($0) })
            }
        }

        /// Returns the closest peer we know of to the specified key. This hashes the key using SHA256 before xor'ing it.
        private func _nearestPeerTo(_ key: String) -> EventLoopFuture<PeerInfo> {
            self._nearestPeerTo(KadDHT.Key(key.bytes))
        }

        private func _nearestPeerTo(_ kid: KadDHT.Key) -> EventLoopFuture<PeerInfo> {
            self.routingTable.nearestPeer(to: kid).flatMap { peer in
                if let peer = peer {
                    return self.peerstore.getPeerInfo(byID: peer.id.b58String)
                } else {
                    return self.eventLoop.makeFailedFuture(Errors.unknownPeer)
                }
            }
        }

        /// Returns up to the specified number of closest peers to the provided key, excluding ourselves and
        /// (optionally) the peer that asked us.
        ///
        /// - Parameter requester: When answering a query, pass the requesting peer. go's
        ///   `betterPeersToQuery` never tells a peer about itself, and echoing the requester back wastes a
        ///   `closerPeers` slot and makes them re-query themselves.
        func _nearest(
            _ num: Int,
            peersToKey keyID: KadDHT.Key,
            excluding requester: PeerID? = nil
        ) -> EventLoopFuture<[PeerInfo]> {
            self.routingTable.nearest(num, peersToKey: keyID).flatMap { peerInfos in
                peerInfos.filter { peer in
                    peer.id != self.peerID && peer.id != requester
                }.compactMap {
                    self.peerstore.getPeerInfo(byID: $0.id.b58String)
                }.flatten(on: self.eventLoop)
            }
        }

        /// Our own dialable addresses, each guaranteed to carry our PeerID.
        ///
        /// Used when answering a FIND_NODE for our own ID.
        private func ourAddresses() -> [Multiaddr] {
            let candidates = self.network?.listenAddresses ?? []
            let addresses = candidates.isEmpty ? [self.address].compactMap { $0 } : candidates
            return addresses.compactMap { addy in
                addy.getPeerIDString() != nil
                    ? addy : try? addy.encapsulate(proto: .p2p, address: self.peerID.b58String)
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

extension KadDHT {
    /// Returns `record` with `timeReceived` set to now.
    ///
    /// `timeReceived` is the receiver's note of when it took delivery, not a publisher-supplied
    /// field. go's `record.MakePutRecord` builds outbound records with only `Key` and `Value` set and
    /// deliberately leaves `TimeReceived` empty. The field exists so whoever holds a record can age it
    /// out against `MaxRecordAge`. So it gets stamped whenever a record enters our store and left blank on
    /// the way out.
    static func timeStamped(_ record: DHT.Record) -> DHT.Record {
        var stamped = record
        stamped.timeReceived = RFC3339Date().string
        return stamped
    }

    /// Whether a value record has aged past `maxAge`, measured from the `timeReceived` we stamped on
    /// it when it entered the store.
    ///
    /// A record whose `timeReceived` is absent or unparseable counts as expired. That's go's
    /// behaviour in `checkLocalDatastore` — "either no receive time set on record, or it was invalid"
    /// marks the record bad — and it's the safe default: a record we can't age is a record we can
    /// never be trusted to expire. Everything that enters our store goes through ``timeStamped``, so
    /// a missing stamp means the entry didn't arrive by a path we control.
    static func isExpired(_ record: DHT.Record, maxAge: TimeInterval, now: Date = Date()) -> Bool {
        guard record.hasTimeReceived, let received = try? RFC3339Date(string: record.timeReceived) else {
            return true
        }
        return now.timeIntervalSince(received.date) > maxAge
    }

    /// Builds the `/pk/` record that publishes `peerID`'s public key to the DHT.
    ///
    /// The record's key is `"/pk/"` followed by the raw bytes of the peer's multihash id, and its
    /// value is the protobuf-marshalled public key.
    ///
    /// ```swift
    /// let record = try KadDHT.createPubKeyRecord(peerID: myPeerID)
    /// let stored = try app.dht.kadDHT.storeNew("/pk/".bytes + myPeerID.id, value: record).wait()
    /// ```
    ///
    /// - Parameter peerID: The peer whose public key should be published. Only the id and the public
    ///   key are read, so a `PeerID` without a private key is fine.
    /// - Returns: A `DHTRecord` keyed under `/pk/<multihash>` holding the marshalled public key.
    /// - Throws: An error if the peer's public key cannot be marshalled — for example a `PeerID`
    ///   constructed from a multihash alone, which carries no key material.
    static public func createPubKeyRecord(peerID: PeerID) throws -> DHTRecord {
        let key = "/pk/".bytes + peerID.id
        let record = try DHT.Record.with { rec in
            rec.key = Data(key)
            rec.value = try Data(peerID.marshalPublicKey())
        }

        return record
    }
}
