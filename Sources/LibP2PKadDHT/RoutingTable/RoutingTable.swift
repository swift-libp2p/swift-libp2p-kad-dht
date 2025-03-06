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

/// The routing table is responsible for maintaining a set of kBuckets each containing a group of DHTPeers that are sorted by distance with respect to our local ID.
///
/// - Note: These buckets needs to be accessed in a thread safe manner using the eventloop.
class RoutingTable: EventLoopService {
    public typealias Filter = (DHTPeerInfo) -> Bool
    public typealias DiversityFilter = (DHTPeerInfo) -> DiversityFilterResult

    public enum Errors: Error {
        case PeerExceededMaxAcceptableLatency
        case NoCapacityForNewPeer
    }

    public struct PeerGroupInfo {
        let id: DHTPeerInfo
        let cpl: Int
        let ipGroupKey: String
    }

    enum DiversityFilterResult {
        case allow(PeerGroupInfo, Bool)
        case increment(PeerGroupInfo)
        case decrement(PeerGroupInfo)
        case peerAddresses(DHTPeerInfo, [Multiaddr])
    }

    /// The Event Loop that we're constrained tos
    public let eventLoop: EventLoop

    /// The services state
    var state: ServiceLifecycleState { self._state }

    private var _state: ServiceLifecycleState
    private var logger: Logger
    public var logLevel: Logger.Level {
        get { self.logger.logLevel }
        set { self.logger.logLevel = newValue }
    }

    /// ID of the Local Peer
    private let localPeerID: PeerID
    private let localDHTID: KadDHT.Key

    /// Letency Metrics for peers in this cluster
    private let metrics: [String: Any]

    /// Maximum acceptable latency for peers in this cluster
    private let maxLatency: TimeAmount

    /// The buckets that our ID/Key space is segmented into
    private var buckets: [Bucket]
    public var bucketCount: EventLoopFuture<Int> {
        self.eventLoop.submit {
            self.buckets.count
        }
    }

    /// The maximum number of peers that can fit into any given bucket
    public let bucketSize: Int

    private let commonPrefixLengthRefreshLink: Bool

    private let commonPrefixLengthRefresehdAt: Date

    public var peerAddedHandler: ((PeerID) -> Void)?
    public var peerRemovedHandler: ((PeerID) -> Void)?

    public var defaultReplacementStrategy: ReplacementStrategy = .furtherThanReplacement

    private let usefulnessGracePeriod: TimeAmount

    private let diversityFilter: DiversityFilter?

    init(eventloop: EventLoop, bucketSize: Int, localPeerID: PeerID, latency: TimeAmount, peerstoreMetrics: [String: Any], usefulnessGracePeriod: TimeAmount) {
        self.eventLoop = eventloop
        self._state = .stopped
        self.logger = Logger(label: "RoutingTable[\(localPeerID.b58String.prefix(8))]")
        /// Default to our global LOG_LEVEL
        self.logger.logLevel = .info //LOG_LEVEL

        self.bucketSize = bucketSize

        self.localPeerID = localPeerID
        self.localDHTID = KadDHT.Key(localPeerID)

        self.maxLatency = latency
        self.metrics = peerstoreMetrics
        self.usefulnessGracePeriod = usefulnessGracePeriod

        self.buckets = [Bucket()]

        // TODO:
        self.commonPrefixLengthRefreshLink = false
        self.commonPrefixLengthRefresehdAt = Date()
        self.diversityFilter = nil
    }

    func start() throws {
        guard self._state == .stopped else { return }
        self._state = .started
    }

    func stop() throws {
        guard self._state == .started else { return }
        self._state = .stopped
        try self.eventLoop.close()
    }

    func numberOfPeers(withCommonPrefixLength cpl: Int) -> EventLoopFuture<Int> {
        self.eventLoop.submit {
            if cpl >= self.buckets.count - 1 {
                guard let last = self.buckets.last else { return 0 }
                return last.filter { $0.dhtID.commonPrefixLength(with: self.localDHTID) == cpl }.count
            } else {
                return self.buckets[cpl].count
            }
        }
    }

    public func addPeer(_ peer: PeerID, isQueryPeer: Bool = false, isReplaceable: Bool = true, replacementStrategy: ReplacementStrategy? = nil) -> EventLoopFuture<Bool> {
        self.eventLoop.submit {
            try self._addPeer(peer, isQueryPeer: isQueryPeer, isReplaceable: isReplaceable, replacementStrategy: replacementStrategy)
        }
    }

    private func _addPeer(_ peer: PeerID, isQueryPeer: Bool, isReplaceable: Bool, replacementStrategy: ReplacementStrategy? = nil) throws -> Bool {

        let bucketID = self._bucketIDFor(peer: peer)

        let now = Date().timeIntervalSince1970
        var lastUsefulAt: TimeInterval?
        if isQueryPeer {
            lastUsefulAt = now
        }

        /// If the peer already exists in the Routing Table
        if var peer = self.buckets[bucketID].getPeer(peer) {
            /// if we're querying the peer first time after adding it, let's give it a usefulness bump.
            /// - Note: This will ONLY happen once.
            if peer.lastUsefulAt == nil && isQueryPeer {
                peer.lastUsefulAt = lastUsefulAt
            }
            return false
        }

        /// Check peers latency metrics
        //if self.metrics["peer"]["LatencyEWMA"] > self.maxLatency {
        /// Connection doesn't meet our latency requirements, don't add peer to DHT
        /// TODO: Throw error instead??
        //    return false
        //}

        /// If we have a diversity filter, add the peer to the filter
        //if let df = self.diversityFilter {
        //    try df.addPeer(peer)
        //}

        /// If we have room in the correct bucket, add the peer...
        if self.buckets[bucketID].count < self.bucketSize {
            self.buckets[bucketID].pushFront(
                DHTPeerInfo(
                    id: peer,
                    lastUsefulAt: lastUsefulAt,
                    lastSuccessfulOutboundQueryAt: now,
                    addedAt: now,
                    dhtID: KadDHT.Key(peer),
                    replaceable: isReplaceable
                )
            )
            /// Invoke the peerAddedHandler if one is registered
            self.peerAddedHandler?(peer)
            return true
        }

        /// If the bucket is full and it's the last bucket (the wildcard bucket) unfold it.
        if bucketID == self.buckets.count - 1 {
            self._nextBucket()

            let bucketID = self._bucketIDFor(peer: peer)

            /// If there's room for the peer after splitting, add the peer to the bucket...
            if self.buckets[bucketID].count < self.bucketSize {
                self.buckets[bucketID].pushFront(
                    DHTPeerInfo(
                        id: peer,
                        lastUsefulAt: lastUsefulAt,
                        lastSuccessfulOutboundQueryAt: now,
                        addedAt: now,
                        dhtID: KadDHT.Key(peer),
                        replaceable: isReplaceable
                    )
                )
                /// Invoke the peerAddedHandler if one is registered
                self.peerAddedHandler?(peer)
                return true
            }
        }

        switch replacementStrategy ?? self.defaultReplacementStrategy {
            case .anyReplaceable:
                /// The bucket to which the peer belongs is full. Let's try and find a peer in that bucket which is replaceable
                /// We don't really need a stable sort here as it doesn't matter which peer we evict as long as it's a replaceable peer
                if let replaceablePeer = self.buckets[bucketID].shuffled().last(where: { $0.replaceable }) {
                    if self._removePeer(replaceablePeer) {
                        self.buckets[bucketID].pushFront(
                            DHTPeerInfo(
                                id: peer,
                                lastUsefulAt: lastUsefulAt,
                                lastSuccessfulOutboundQueryAt: now,
                                addedAt: now,
                                dhtID: KadDHT.Key(peer),
                                replaceable: isReplaceable
                            )
                        )
                        /// Invoke the peerAddedHandler if one is registered
                        self.peerAddedHandler?(peer)
                        return true
                    }
                } else {
                    self.logger.debug("Failed to find a peer to evict in order to make room for this peer...")
                }

            case .furthestReplaceable:
                /// Replaces the furthest replaceable peer without checking if the new peer is closer of farther then the replaceable peer
                /// - Note: This results is a little more random movement throughout the network leading to more peer discovery and better kv lookup results
                if let replaceablePeer = self.buckets[bucketID].filter({ $0.replaceable }).sorted(by: { lhs, rhs in
                    self.localDHTID.compareDistancesFromSelf(to: lhs.dhtID, and: rhs.dhtID) == .firstKey
                }).last {
                    self.logger.debug("Found the furthest replaceable peer in bucket[\(bucketID)], attempting to replace it with this peer")
                    if self._removePeer(replaceablePeer) {
                        self.buckets[bucketID].pushFront(
                            DHTPeerInfo(
                                id: peer,
                                lastUsefulAt: lastUsefulAt,
                                lastSuccessfulOutboundQueryAt: now,
                                addedAt: now,
                                dhtID: KadDHT.Key(peer),
                                replaceable: isReplaceable
                            )
                        )
                        /// Invoke the peerAddedHandler if one is registered
                        self.peerAddedHandler?(peer)
                        return true
                    }
                } else {
                    self.logger.debug("Failed to find a peer to evict in order to make room for this peer...")
                }

            case .oldestReplaceable:
                /// The bucket to which the peer belongs is full. Let's try and find a peer in that bucket which is replaceable
                /// We don't really need a stable sort here as it doesn't matter which peer we evict as long as it's a replaceable peer
                if let replaceablePeer = self.buckets[bucketID].last(where: { $0.replaceable }) {
                    if self._removePeer(replaceablePeer) {
                        self.buckets[bucketID].pushFront(
                            DHTPeerInfo(
                                id: peer,
                                lastUsefulAt: lastUsefulAt,
                                lastSuccessfulOutboundQueryAt: now,
                                addedAt: now,
                                dhtID: KadDHT.Key(peer),
                                replaceable: isReplaceable
                            )
                        )
                        /// Invoke the peerAddedHandler if one is registered
                        self.peerAddedHandler?(peer)
                        return true
                    }
                } else {
                    self.logger.debug("Failed to find a peer to evict in order to make room for this peer...")
                }

            case .furtherThanReplacement:
                /// Adds the peer if they are closer to us than the furthest replaceable peer in the current bucket
                /// - Note: This results in the network converging fairly quickly and accurately, but can lead to network segragation and results in poor kv lookup results
                if let replaceablePeer = self.buckets[bucketID].filter({ $0.replaceable }).sorted(by: { lhs, rhs in
                    self.localDHTID.compareDistancesFromSelf(to: lhs.dhtID, and: rhs.dhtID) == .firstKey
                }).last, self.localPeerID.compareDistancesFromSelf(to: peer, and: replaceablePeer.id) == .firstKey {
                    self.logger.debug("Found a peer in bucket[\(bucketID)] that's replaceable and further away from us than the new peer, attempting to replace it with this peer")
                    if self._removePeer(replaceablePeer) {
                        self.buckets[bucketID].pushFront(
                            DHTPeerInfo(
                                id: peer,
                                lastUsefulAt: lastUsefulAt,
                                lastSuccessfulOutboundQueryAt: now,
                                addedAt: now,
                                dhtID: KadDHT.Key(peer),
                                replaceable: isReplaceable
                            )
                        )
                        /// Invoke the peerAddedHandler if one is registered
                        self.peerAddedHandler?(peer)
                        return true
                    }
                } else {
                    self.logger.debug("Failed to find a peer to evict in order to make room for this peer...")
                }
        }

        /// We weren't able to find a place for the new peer...
        //if let df = self.diversityFilter {
        //    df.removePeer(peer)
        //}

        return false
    }

    public enum ReplacementStrategy {
        /// Picks a random replaceable peer
        case anyReplaceable
        /// Picks the furthest replacable peer from us
        case furthestReplaceable
        /// Picks the oldest replaceable peer
        case oldestReplaceable
        /// Only replaces the furthest replacable peer if they're further away from us then the replacement
        case furtherThanReplacement
    }

    public func addPeer(_ peer: DHTPeerInfo, isQueryPeer: Bool, replacementStrategy: ReplacementStrategy? = nil) throws -> EventLoopFuture<Bool> {
        self.eventLoop.submit {
            let bucketID = self._bucketIDFor(peer: peer)
            self.logger.debug("Attempting to add peer to bucket[\(bucketID)]")

            let now = Date().timeIntervalSince1970
            var lastUsefulAt: TimeInterval?
            if isQueryPeer {
                lastUsefulAt = now
            }

            /// If the peer already exists in the Routing Table
            if var peer = self.buckets[bucketID].getPeer(peer) {
                /// if we're querying the peer first time after adding it, let's give it a usefulness bump.
                /// - Note: This will ONLY happen once.
                if peer.lastUsefulAt == nil && isQueryPeer {
                    peer.lastUsefulAt = lastUsefulAt
                }
                self.logger.debug("Peer Already Exists. Returning Without Adding")
                return false
            }

            /// Check peers latency metrics
            //if self.metrics["peer"]["LatencyEWMA"] > self.maxLatency {
            /// Connection doesn't meet our latency requirements, don't add peer to DHT
            /// TODO: Throw error instead??
            //    return false
            //}

            /// If we have a diversity filter, add the peer to the filter
            //if let df = self.diversityFilter {
            //    try df.addPeer(peer)
            //}

            /// If we have room in the correct bucket, add the peer...
            if self.buckets[bucketID].count < self.bucketSize {
                self.buckets[bucketID].pushFront(
                    DHTPeerInfo(
                        id: peer.id,
                        lastUsefulAt: lastUsefulAt,
                        lastSuccessfulOutboundQueryAt: now,
                        addedAt: now,
                        dhtID: peer.dhtID,
                        replaceable: peer.replaceable
                    )
                )
                /// Invoke the peerAddedHandler if one is registered
                self.peerAddedHandler?(peer.id)
                self.logger.debug("Peer Added To Current Bucket Due To Excess Capacity")
                return true
            }

            /// If the bucket is full and it's the last bucket (the wildcard bucket) unfold it.
            if bucketID == self.buckets.count - 1 {
                self.logger.debug("Attempting to unfold the last wildcard bucket")
                self._nextBucket()

                let bucketID = self._bucketIDFor(peer: peer)
                self.logger.debug("Now attempting to add peer to bucket[\(bucketID)] after split")

                /// If there's room for the peer after splitting, add the peer to the bucket...
                if self.buckets[bucketID].count < self.bucketSize {
                    self.buckets[bucketID].pushFront(
                        DHTPeerInfo(
                            id: peer.id,
                            lastUsefulAt: lastUsefulAt,
                            lastSuccessfulOutboundQueryAt: now,
                            addedAt: now,
                            dhtID: peer.dhtID,
                            replaceable: peer.replaceable
                        )
                    )
                    /// Invoke the peerAddedHandler if one is registered
                    self.peerAddedHandler?(peer.id)
                    self.logger.debug("Peer Added To Bucket[\(bucketID)] post split due to excess capacity")
                    return true
                }
            }

            /// Begin Peer Replacement Logic
            switch replacementStrategy ?? self.defaultReplacementStrategy {
                case .anyReplaceable:
                    /// The bucket to which the peer belongs is full. Let's try and find a peer in that bucket which is replaceable
                    /// We don't really need a stable sort here as it doesn't matter which peer we evict as long as it's a replaceable peer
                    if let replaceablePeer = self.buckets[bucketID].shuffled().last(where: { $0.replaceable }) {
                        self.logger.debug("Found a peer in bucket[\(bucketID)] that's replaceable, attempting to replace it with this peer")
                        if self._removePeer(replaceablePeer) {
                            self.buckets[bucketID].pushFront(
                                DHTPeerInfo(
                                    id: peer.id,
                                    lastUsefulAt: lastUsefulAt,
                                    lastSuccessfulOutboundQueryAt: now,
                                    addedAt: now,
                                    dhtID: peer.dhtID,
                                    replaceable: peer.replaceable
                                )
                            )
                            /// Invoke the peerAddedHandler if one is registered
                            self.peerAddedHandler?(peer.id)
                            return true
                        }
                    } else {
                        self.logger.debug("Failed to find a peer to evict in order to make room for this peer...")
                    }

                case .furthestReplaceable:
                    /// Replaces the furthest replaceable peer without checking if the new peer is closer of farther then the replaceable peer
                    /// - Note: This results is a little more random movement throughout the network leading to more peer discovery and better kv lookup results
                    if let replaceablePeer = self.buckets[bucketID].filter({ $0.replaceable }).sorted(by: { lhs, rhs in
                        self.localDHTID.compareDistancesFromSelf(to: lhs.dhtID, and: rhs.dhtID) == .firstKey
                    }).last {
                        self.logger.debug("Found the furthest reaplaceable peer in bucket[\(bucketID)], attempting to replace it with this peer")
                        if self._removePeer(replaceablePeer) {
                            self.buckets[bucketID].pushFront(
                                DHTPeerInfo(
                                    id: peer.id,
                                    lastUsefulAt: lastUsefulAt,
                                    lastSuccessfulOutboundQueryAt: now,
                                    addedAt: now,
                                    dhtID: peer.dhtID,
                                    replaceable: peer.replaceable
                                )
                            )
                            /// Invoke the peerAddedHandler if one is registered
                            self.peerAddedHandler?(peer.id)
                            return true
                        }
                    } else {
                        self.logger.debug("Failed to find a peer to evict in order to make room for this peer...")
                    }

                case .oldestReplaceable:
                    /// The bucket to which the peer belongs is full. Let's try and find a peer in that bucket which is replaceable
                    /// We don't really need a stable sort here as it doesn't matter which peer we evict as long as it's a replaceable peer
                    if let replaceablePeer = self.buckets[bucketID].last(where: { $0.replaceable }) {
                        self.logger.debug("Found a peer in bucket[\(bucketID)] that's replaceable, attempting to replace it with this peer")
                        if self._removePeer(replaceablePeer) {
                            self.buckets[bucketID].pushFront(
                                DHTPeerInfo(
                                    id: peer.id,
                                    lastUsefulAt: lastUsefulAt,
                                    lastSuccessfulOutboundQueryAt: now,
                                    addedAt: now,
                                    dhtID: peer.dhtID,
                                    replaceable: peer.replaceable
                                )
                            )
                            /// Invoke the peerAddedHandler if one is registered
                            self.peerAddedHandler?(peer.id)
                            return true
                        }
                    } else {
                        self.logger.debug("Failed to find a peer to evict in order to make room for this peer...")
                    }

                case .furtherThanReplacement:
                    /// Adds the peer if they are closer to us than the furthest replaceable peer in the current bucket
                    /// - Note: This results in the network converging fairly quickly and accurately, but can lead to network segragation and results in poor kv lookup results
                    if let replaceablePeer = self.buckets[bucketID].filter({ $0.replaceable }).sorted(by: { lhs, rhs in
                        self.localDHTID.compareDistancesFromSelf(to: lhs.dhtID, and: rhs.dhtID) == .firstKey
                    }).last, self.localDHTID.compareDistancesFromSelf(to: peer.dhtID, and: replaceablePeer.dhtID) == .firstKey {
                        self.logger.debug("Found a peer in bucket[\(bucketID)] that's replaceable and further away from us than the new peer, attempting to replace it with this peer")
                        if self._removePeer(replaceablePeer) {
                            self.buckets[bucketID].pushFront(
                                DHTPeerInfo(
                                    id: peer.id,
                                    lastUsefulAt: lastUsefulAt,
                                    lastSuccessfulOutboundQueryAt: now,
                                    addedAt: now,
                                    dhtID: peer.dhtID,
                                    replaceable: peer.replaceable
                                )
                            )
                            /// Invoke the peerAddedHandler if one is registered
                            self.peerAddedHandler?(peer.id)
                            return true
                        }
                    } else {
                        self.logger.debug("Failed to find a peer to evict in order to make room for this peer...")
                    }
            }

            /// We weren't able to find a place for the new peer...
            //if let df = self.diversityFilter {
            //    df.removePeer(peer)
            //}

            return false
        }
    }

    public func removePeer(_ peer: PeerID) -> EventLoopFuture<Bool> {
        self.eventLoop.submit {
            self._removePeer(peer)
        }
    }

    /// Removes a peer from our Routing Table
    ///
    /// TODO: Check to make sure our bucket compaction is actually correct. We're deviating a little from the GO implementation...
    private func _removePeer(_ peer: PeerID) -> Bool {
        let bucketID = self._bucketIDFor(peer: peer)
        if self.buckets[bucketID].remove(peer) {

            /// If we have a diversityFilter installed, remove the peer from it as well...
            //if var df = self.diversityFilter {
            //    df.remove(peer)
            //}

            /// Compact the buckets array, making sure we don't have empty buckets laying around...
            while !self.buckets.isEmpty {
                let lastBucketIndex = self.buckets.count - 1
                if self.buckets.count > 1, self.buckets[lastBucketIndex].isEmpty {
                    // If the last bucket is empty remove it...
                    self.buckets.remove(at: lastBucketIndex)
                } else if self.buckets.count >= 2 && self.buckets[lastBucketIndex - 1].isEmpty {
                    // If the second to last bucket became empty, remove it...
                    self.buckets.remove(at: lastBucketIndex - 1)
                } else {
                    break
                }
            }
            if self.buckets.isEmpty { self.buckets.append(Bucket()) }

            /// Invoke the peerRemovedHandler if one is set...
            self.peerRemovedHandler?(peer)

            return true
        }
        return false
    }

    public func removePeer(_ peer: DHTPeerInfo) -> EventLoopFuture<Bool> {
        self.eventLoop.submit {
            self._removePeer(peer)
        }
    }

    /// Removes a peer from our Routing Table
    ///
    /// TODO: Check to make sure our bucket compaction is actually correct. We're deviating a little from the GO implementation...
    private func _removePeer(_ peer: DHTPeerInfo) -> Bool {
        let bucketID = self._bucketIDFor(peer: peer)
        if self.buckets[bucketID].remove(peer.id) {

            /// If we have a diversityFilter installed, remove the peer from it as well...
            //if var df = self.diversityFilter {
            //    df.remove(peer)
            //}

            /// Compact the buckets array, making sure we don't have empty buckets laying around...
            while !self.buckets.isEmpty {
                let lastBucketIndex = self.buckets.count - 1
                if self.buckets.count > 1, self.buckets[lastBucketIndex].isEmpty {
                    // If the last bucket is empty remove it...
                    self.buckets.remove(at: lastBucketIndex)
                } else if self.buckets.count >= 2 && self.buckets[lastBucketIndex - 1].isEmpty {
                    // If the second to last bucket became empty, remove it...
                    self.buckets.remove(at: lastBucketIndex - 1)
                } else {
                    break
                }
            }
            if self.buckets.isEmpty { self.buckets.append(Bucket()) }

            /// Invoke the peerRemovedHandler if one is set...
            self.peerRemovedHandler?(peer.id)

            return true
        } else {
            for (idx, bucket) in self.buckets.enumerated() {
                if bucket.contains(where: { peer.id == $0.id }) && idx != bucketID {
                    self.logger.debug("The peer we're trying to remove is in bucket[\(idx)] rather then the bucket we looked in at bucket[\(bucketID)]")
                }
            }
        }
        return false
    }

    public func markPeerReplaceable(_ peer: DHTPeerInfo) -> EventLoopFuture<Bool> {
        self.eventLoop.submit { self._markPeerReplaceable(peer) }
    }

    private func _markPeerReplaceable(_ peer: DHTPeerInfo) -> Bool {
        return self._modifyPeer(peer, modifier: { $0.replaceable = true })
    }

    public func markPeerIrreplaceable(_ peer: DHTPeerInfo) -> EventLoopFuture<Bool> {
        self.eventLoop.submit { self._markPeerReplaceable(peer) }
    }

    private func _markPeerIrreplaceable(_ peer: DHTPeerInfo) -> Bool {
        return self._modifyPeer(peer, modifier: { $0.replaceable = false })
    }

    public func markAllPeersIrreplaceable() -> EventLoopFuture<Void> {
        self.eventLoop.submit { self._markAllPeersIrreplaceable() }
    }

    private func _markAllPeersIrreplaceable() {
        for i in 0..<self.buckets.count {
            self.buckets[i].updateAllWith { dhtPeer in
                dhtPeer.replaceable = false
            }
        }
    }

    public func markAllPeersReplaceable() -> EventLoopFuture<Void> {
        self.eventLoop.submit { self._markAllPeersReplaceable() }
    }

    private func _markAllPeersReplaceable() {
        for i in 0..<self.buckets.count {
            self.buckets[i].updateAllWith { dhtPeer in
                dhtPeer.replaceable = true
            }
        }
    }

    public func getPeerInfos() -> EventLoopFuture<[DHTPeerInfo]> {
        self.eventLoop.submit {
            self._getPeerInfos()
        }
    }

    private func _getPeerInfos() -> [DHTPeerInfo] {
        return self.buckets.reduce(into: Array<DHTPeerInfo>()) { partialResult, bucket in
            partialResult += bucket.peers()
        }
    }

    public func updateLastSuccessfulOutboundQuery(at: TimeInterval, for peer: PeerID) -> EventLoopFuture<Bool> {
        self.eventLoop.submit { self._updateLastSuccessfulOutboundQuery(at: at, for: peer) }
    }

    private func _updateLastSuccessfulOutboundQuery(at: TimeInterval, for peer: PeerID) -> Bool {
        self._modifyPeer(peer, modifier: { $0.lastSuccessfulOutboundQueryAt = at })
    }

    public func updateLastSuccessfulOutboundQuery(at: TimeInterval, for peer: DHTPeerInfo) -> EventLoopFuture<Bool> {
        self.eventLoop.submit { self._updateLastSuccessfulOutboundQuery(at: at, for: peer) }
    }

    private func _updateLastSuccessfulOutboundQuery(at: TimeInterval, for peer: DHTPeerInfo) -> Bool {
        return self._modifyPeer(peer, modifier: { $0.lastSuccessfulOutboundQueryAt = at })
    }

    public func updateLastUseful(at: TimeInterval, for peer: PeerID) -> EventLoopFuture<Bool> {
        self.eventLoop.submit { self._updateLastUseful(at: at, for: peer) }
    }

    private func _updateLastUseful(at: TimeInterval, for peer: PeerID) -> Bool {
        self._modifyPeer(peer, modifier: { $0.lastUsefulAt = at })
    }

    public func updateLastUseful(at: TimeInterval, for peer: DHTPeerInfo) -> EventLoopFuture<Bool> {
        self.eventLoop.submit { self._updateLastUseful(at: at, for: peer) }
    }

    private func _updateLastUseful(at: TimeInterval, for peer: DHTPeerInfo) -> Bool {
        self._modifyPeer(peer, modifier: { $0.lastUsefulAt = at })
    }

    private func _nextBucket() {
        /// This is the last bucket, which allegedly is a mixed bag containing peers not belonging in dedicated (unfolded) buckets.
        /// _allegedly_ is used here to denote that *all* peers in the last bucket might feasibly belong to another bucket.
        /// This could happen if e.g. we've unfolded 4 buckets, and all peers in folded bucket 5 really belong in bucket 8.
        guard !self.buckets.isEmpty else { self.buckets.append(Bucket()); return }
        let newBucket = self.buckets[self.buckets.count - 1].split(commonPrefixLength: self.buckets.count - 1, targetID: self.localDHTID.bytes)
        self.buckets.append(newBucket)

        if newBucket.count > self.bucketSize {
            self._nextBucket()
        }
    }

    /// Find a specific peer by ID or return nil
    public func find(id: PeerID) -> EventLoopFuture<DHTPeerInfo?> {
        self.eventLoop.submit { self._find(id: id) }
    }

    private func _find(id: PeerID) -> DHTPeerInfo? {
        if let nearest = self._nearestPeer(to: id), nearest.id == id {
            return nearest
        }
        return nil
    }

    public func find(id: DHTPeerInfo) -> EventLoopFuture<DHTPeerInfo?> {
        self.eventLoop.submit {
            self._find(id: id)
        }
    }

    private func _find(id: DHTPeerInfo) -> DHTPeerInfo? {
        if let nearest = self._nearestPeer(to: id), nearest.id == id.id {
            return nearest
        }
        return nil
    }

    /// NearestPeer returns a single peer that is the closest one that we know of to the given ID
    public func nearestPeer(to: PeerID) -> EventLoopFuture<DHTPeerInfo?> {
        self.eventLoop.submit { self._nearestPeer(to: to) }
    }

    private func _nearestPeer(to: PeerID) -> DHTPeerInfo? {
        self._nearest(1, peersTo: to).first
    }

    public func nearestPeer(to: DHTPeerInfo) -> EventLoopFuture<DHTPeerInfo?> {
        self.eventLoop.submit { self._nearestPeer(to: to) }
    }

    private func _nearestPeer(to: DHTPeerInfo) -> DHTPeerInfo? {
        self._nearest(1, peersTo: to.dhtID).first
    }

    public func nearestPeer(to: KadDHT.Key) -> EventLoopFuture<DHTPeerInfo?> {
        self.eventLoop.submit { self._nearestPeer(to: to) }
    }

    private func _nearestPeer(to: KadDHT.Key) -> DHTPeerInfo? {
        self._nearest(1, peersTo: to).first
    }

    /// NearestPeers returns a list of the 'count' closest peers to the given ID
    public func nearest(_ count: Int, peersTo peer: PeerID) -> EventLoopFuture<[DHTPeerInfo]> {
        self.eventLoop.submit { self._nearest(count, peersTo: KadDHT.Key(peer)) }
    }

    private func _nearest(_ count: Int, peersTo peer: PeerID) -> [DHTPeerInfo] {
        self._nearest(count, peersTo: KadDHT.Key(peer))
    }

    public func nearest(_ count: Int, peersTo peer: DHTPeerInfo) -> EventLoopFuture<[DHTPeerInfo]> {
        self.eventLoop.submit { self._nearest(count, peersTo: peer.dhtID) }
    }

    private func _nearest(_ count: Int, peersTo peer: DHTPeerInfo) -> [DHTPeerInfo] {
        self._nearest(count, peersTo: peer.dhtID)
    }

    public func nearest(_ count: Int, peersToKey keyID: KadDHT.Key) -> EventLoopFuture<[DHTPeerInfo]> {
        self.eventLoop.submit { self._nearest(count, peersTo: keyID) }
    }

    private func _nearest(_ count: Int, peersTo peer: KadDHT.Key) -> [DHTPeerInfo] {
        self.logger.debug("Attempting to find the \(count) nearest peers to \(peer.bytes)")
        let cpl = min(peer.commonPrefixLength(with: self.localDHTID), self.buckets.count - 1)
        //let cpl = self._bucketIDFor(peer: peer)

        var peersToSort = self.buckets[cpl].peers()

        /// If we're short, add the peers from the buckets to the right
        if peersToSort.count < count {
            for i in (cpl + 1)..<self.buckets.count {
                self.logger.debug("Searching for additional peers with higher CPLs in bucket[\(i)]")
                peersToSort += self.buckets[i].peers()
                if peersToSort.count >= count { break }
            }
        }

        /// If we're still short, add peers from the buckets to left
        if peersToSort.count < count {
            for i in (0..<cpl).reversed() {
                self.logger.debug("Searching for additional peers with lower CPLs in bucket[\(i)]")
                peersToSort += self.buckets[i].peers()
                if peersToSort.count >= count { break }
            }
        }

        /// Sort the peers by their distance to our local key and return the requested number of closest peers
        return Array<DHTPeerInfo>(peersToSort.sorted(by: { lhs, rhs in
            peer.compareDistancesFromSelf(to: lhs.dhtID, and: rhs.dhtID).rawValue >= 0
        }).prefix( count ))
    }

    /// Returns the total count of all peers in the RoutingTable
    public func totalPeers() -> EventLoopFuture<Int> {
        self.eventLoop.submit { self._size() }
    }

    /// Returns the total count of all peers in the RoutingTable
    ///
    /// Caller is responsible for ensuring this is called on our eventloop
    private func _size() -> Int {
        return self.buckets.reduce(into: 0) { partialResult, bucket in
            partialResult += bucket.count
        }
    }

    /// ListPeers returns a list of all peers from all buckets in the routing table.
    ///
    /// Caller is responsible for ensuring this is called on our eventloop
    private func _listPeers() -> [PeerID] {
        return self.buckets.reduce(into: Array<PeerID>()) { partialResult, bucket in
            partialResult += bucket.peerIDs()
        }
    }

    func prettyPrint() {
        print(self.description)
    }

    /// - TODO: Implement me...
    func getDiversityStats() -> [String: Any] {
        return [:]
    }

    private func _modifyPeer(_ peer: DHTPeerInfo, modifier: (inout DHTPeerInfo) -> Void) -> Bool {
        let bucketID = self._bucketIDFor(peer: peer)
        return self.buckets[bucketID].getPeer(peer, modifier: modifier)
    }

    private func _modifyPeer(_ peer: PeerID, modifier: (inout DHTPeerInfo) -> Void) -> Bool {
        let bucketID = self._bucketIDFor(peer: peer)
        return self.buckets[bucketID].getPeer(peer, modifier: modifier)
    }

    /// Caller is responsible for ensuring this is called on our eventloop
    private func _bucketIDFor(peer: PeerID) -> Int {
        self._bucketIDFor(peer: KadDHT.Key(peer))
    }

    private func _bucketIDFor(peer: DHTPeerInfo) -> Int {
        self._bucketIDFor(peer: peer.dhtID)
    }

    private func _bucketIDFor(peer: KadDHT.Key) -> Int {
        let cpl = self.localDHTID.commonPrefixLength(with: peer)
        if cpl >= self.buckets.count {
            return self.buckets.count - 1
        }
        return cpl
    }

    /// Caller is responsible for ensuring this is called on our eventloop
    private func _maxCommonPrefixLength() -> Int {
        return self.buckets.last(where: { !$0.isEmpty })?.maxCommonPrefixLength(target: self.localDHTID.bytes) ?? 0
    }
}

extension RoutingTable: CustomStringConvertible {
    var description: String {
        """
        📒 --------------------------------- 📒
        Routing Table [\(self.localPeerID)]
        Bucket Count: \(self.buckets.count) buckets of size: \(self.bucketSize)
        Total Peers: \(self._listPeers().count)
        \(self.buckets.enumerated().map { idx, bucket -> String in
            "b[\(idx)] = [\(bucket.map { String($0.dhtID.commonPrefixLength(with: self.localDHTID)) }.joined(separator: ", "))]"
        }.joined(separator: "\n"))
        ---------------------------------------
        """
    }
}
