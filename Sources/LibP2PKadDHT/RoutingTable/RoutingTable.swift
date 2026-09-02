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
/// - Note: Because we perform all mutating operations on a specific event loop we should be thread safe (hence the @unchecked Sendable)
class RoutingTable: EventLoopService, @unchecked Sendable {
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

    init(
        eventloop: EventLoop,
        bucketSize: Int,
        localPeerID: PeerID,
        latency: TimeAmount,
        peerstoreMetrics: [String: Any],
        usefulnessGracePeriod: TimeAmount
    ) {
        self.eventLoop = eventloop
        self._state = .stopped
        self.logger = Logger(label: "RoutingTable[\(localPeerID.b58String.prefix(8))]")
        /// Default to our global LOG_LEVEL
        self.logger.logLevel = .info  //LOG_LEVEL

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
        /// - Note: We deliberately don't close our `eventLoop` here. It's shared with the `KadDHT.Node`.
    }

    /// The common-prefix lengths of the buckets that currently hold at least one peer.
    ///
    /// Bucket `i` holds the peers sharing exactly `i` leading bits with us, so the index *is* the
    /// prefix length
    func nonEmptyBucketPrefixLengths() -> EventLoopFuture<[Int]> {
        self.eventLoop.submit {
            self.buckets.enumerated().compactMap { index, bucket in
                bucket.count > 0 ? index : nil
            }
        }
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

    public func addPeer(
        _ peer: PeerID,
        isQueryPeer: Bool = false,
        isReplaceable: Bool = true,
        replacementStrategy: ReplacementStrategy? = nil
    ) -> EventLoopFuture<Bool> {
        self.eventLoop.submit {
            let now = Date().timeIntervalSince1970
            return self._addPeer(
                DHTPeerInfo(
                    id: peer,
                    lastUsefulAt: nil,
                    lastSuccessfulOutboundQueryAt: now,
                    addedAt: now,
                    dhtID: KadDHT.Key(peer),
                    replaceable: isReplaceable
                ),
                isQueryPeer: isQueryPeer,
                replacementStrategy: replacementStrategy
            )
        }
    }

    public func addPeer(
        _ peer: DHTPeerInfo,
        isQueryPeer: Bool,
        replacementStrategy: ReplacementStrategy? = nil
    ) throws -> EventLoopFuture<Bool> {
        self.eventLoop.submit {
            self._addPeer(peer, isQueryPeer: isQueryPeer, replacementStrategy: replacementStrategy)
        }
    }

    /// Adds a `peer` into our `RoutingTable` if room or the `replacementStrategy` allows for it.
    ///
    /// Both entry points funnel through here. It works on a fully-formed ``DHTPeerInfo`` rather than
    /// deriving one, because `dhtID` is not always `KadDHT.Key(id)`, test helpers build peers
    /// with a synthetic key so they can place one in a chosen bucket.
    private func _addPeer(
        _ peer: DHTPeerInfo,
        isQueryPeer: Bool,
        replacementStrategy: ReplacementStrategy? = nil
    ) -> Bool {

        var bucketID = self._bucketIDFor(peer: peer)
        self.logger.debug("Attempting to add peer to bucket[\(bucketID)]")

        let now = Date().timeIntervalSince1970
        let lastUsefulAt: TimeInterval? = isQueryPeer ? now : nil

        /// If the peer already exists in the Routing Table
        if self.buckets[bucketID].getPeer(
            peer,
            modifier: { existing in
                /// if we're querying the peer first time after adding it, let's give it a usefulness bump.
                if existing.lastUsefulAt == nil && isQueryPeer {
                    existing.lastUsefulAt = lastUsefulAt
                }
            }
        ) {
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
            self._insert(peer, lastUsefulAt: lastUsefulAt, at: bucketID, now: now)
            self.logger.debug("Peer Added To Current Bucket Due To Excess Capacity")
            return true
        }

        /// If the bucket is full and it's the last bucket (the wildcard bucket) unfold it.
        if bucketID == self.buckets.count - 1 {
            self.logger.debug("Attempting to unfold the last wildcard bucket")
            self._nextBucket()

            /// The split may have moved this peer into a different bucket
            bucketID = self._bucketIDFor(peer: peer)
            self.logger.debug("Now attempting to add peer to bucket[\(bucketID)] after split")

            /// If there's room for the peer after splitting, add the peer to the bucket...
            if self.buckets[bucketID].count < self.bucketSize {
                self._insert(peer, lastUsefulAt: lastUsefulAt, at: bucketID, now: now)
                self.logger.debug("Peer Added To Bucket[\(bucketID)] post split due to excess capacity")
                return true
            }
        }

        /// The bucket is full, so refer to our replacement strategy to determine how to proceed
        guard
            let evictee = self._evictionCandidate(
                in: bucketID,
                makingRoomFor: peer,
                strategy: replacementStrategy ?? self.defaultReplacementStrategy,
                now: now
            )
        else {
            self.logger.debug("Failed to find a peer to evict in order to make room for this peer...")
            /// We weren't able to find a place for the new peer...
            //if let df = self.diversityFilter {
            //    df.removePeer(peer)
            //}
            return false
        }

        /// Remove the unlucky peer
        guard self._removePeer(evictee.id, in: self._bucketIDFor(peer: evictee)) else { return false }

        /// Recomputing the bucketID shouldn't yeild a different id but recompute it just in case...
        /// - Note: Should we guard / throw if the recomputed bucketID is different than the removal / evictee?
        self._insert(peer, lastUsefulAt: lastUsefulAt, at: self._bucketIDFor(peer: peer), now: now)
        return true
    }

    /// Puts `peer` at the front of `bucketID` and notifies whoever is watching the table.
    ///
    /// - Note: `dhtID` and `replaceable` are carried over from `peer` rather than recomputed.
    private func _insert(
        _ peer: DHTPeerInfo,
        lastUsefulAt: TimeInterval?,
        at bucketID: Int,
        now: TimeInterval
    ) {
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
        self.peerAddedHandler?(peer.id)
    }

    /// Who to evict from a full `bucketID` to make room for `incoming`, or `nil` to discard the
    /// new peer.
    ///
    /// Only replaceable peers are ever candidates. Among those, peers that have had their grace
    /// period to be useful and weren't go first (see ``_hasntProvedUseful(_:now:)``). The
    /// strategy then chooses between whatever is left.
    private func _evictionCandidate(
        in bucketID: Int,
        makingRoomFor incoming: DHTPeerInfo,
        strategy: ReplacementStrategy,
        now: TimeInterval
    ) -> DHTPeerInfo? {
        let replaceable = self.buckets[bucketID].filter { $0.replaceable }
        guard !replaceable.isEmpty else { return nil }

        /// Filter out the recently useful peers
        let stale = replaceable.filter { self._hasntProvedUseful($0, now: now) }
        /// If stale is empty, then fall back to all replaceable peers
        let candidates = stale.isEmpty ? replaceable : stale
        if !stale.isEmpty {
            self.logger.debug(
                "\(stale.count) of \(replaceable.count) replaceable peers in bucket[\(bucketID)] have outstayed their usefulness grace period"
            )
        }

        switch strategy {
        case .anyReplaceable:
            /// It doesn't matter which one goes, so no need for a stable order.
            return candidates.shuffled().last

        case .oldestReplaceable:
            /// Peers are pushed onto the front of a bucket, so the tail is the oldest.
            return candidates.last

        case .furthestReplaceable:
            /// Evicts the furthest peer regardless of where the newcomer sits.
            /// - Note: this keeps peers moving through the table, which turns up more of the network
            ///   and gives better kv lookup results than converging hard on our own neighbourhood.
            return self._furthestFromUs(candidates)

        case .furtherThanReplacement:
            /// Only worth the churn if the newcomer is actually an improvement.
            /// - Note: converges quickly and accurately, but can segregate the network and gives
            ///   poorer kv lookup results.
            guard let furthest = self._furthestFromUs(candidates),
                self.localDHTID.compareDistancesFromSelf(to: incoming.dhtID, and: furthest.dhtID) == .firstKey
            else { return nil }
            return furthest
        }
    }

    /// The candidate furthest from us in XOR space.
    private func _furthestFromUs(_ candidates: [DHTPeerInfo]) -> DHTPeerInfo? {
        candidates.sorted { lhs, rhs in
            self.localDHTID.compareDistancesFromSelf(to: lhs.dhtID, and: rhs.dhtID) == .firstKey
        }.last
    }

    /// Whether a peer has been marked useful in the last `usefulnessGracePeriod`.
    ///
    /// Measured from `lastUsefulAt`, falling back to `addedAt` for a peer that has never been useful.
    private func _hasntProvedUseful(_ peer: DHTPeerInfo, now: TimeInterval) -> Bool {
        let grace = TimeInterval(self.usefulnessGracePeriod.nanoseconds) / 1_000_000_000
        return now - (peer.lastUsefulAt ?? peer.addedAt) > grace
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

    public func removePeer(_ peer: PeerID) -> EventLoopFuture<Bool> {
        self.eventLoop.submit {
            self._removePeer(peer, in: self._bucketIDFor(peer: peer))
        }
    }

    public func removePeer(_ peer: DHTPeerInfo) -> EventLoopFuture<Bool> {
        self.eventLoop.submit {
            self._removePeer(peer.id, in: self._bucketIDFor(peer: peer))
        }
    }

    /// Removes a peer from our Routing Table
    ///
    /// The single removal routine. There was a second, near-identical copy taking a `DHTPeerInfo`,
    /// which existed because the two disagreed on *where to look*: one hashed the `PeerID`, the other
    /// used the peer's own `dhtID`. Those differ for a peer whose key wasn't derived from its id, so
    /// the bucket is now passed in by whoever knows it.
    ///
    /// TODO: Check to make sure our bucket compaction is actually correct. We're deviating a little from the GO implementation...
    private func _removePeer(_ peer: PeerID, in bucketID: Int) -> Bool {
        if self.buckets[bucketID].remove(peer) {

            /// If we have a diversityFilter installed, remove the peer from it as well...
            //if var df = self.diversityFilter {
            //    df.remove(peer)
            //}

            /// Compact the buckets array by trimming any empty buckets off of the tail...
            /// - Note: We only trim the tail. A bucket's index is the common prefix length of the
            ///   peers it holds, so removing an interior bucket would shift every higher bucket down and
            ///   permanently break the CPL index
            /// - Note: We always keep at least one bucket around.
            while self.buckets.count > 1, self.buckets[self.buckets.count - 1].isEmpty {
                self.buckets.removeLast()
            }

            /// Invoke the peerRemovedHandler if one is set...
            self.peerRemovedHandler?(peer)

            return true
        }

        /// A peer sitting in a bucket other than the one its CPL points at means an index has
        /// drifted, which silently breaks eviction and lookups.
        /// - TODO: Should we drop the peer anyway??
        for (idx, bucket) in self.buckets.enumerated() where idx != bucketID {
            if bucket.contains(where: { peer == $0.id }) {
                self.logger.warning(
                    "The peer we're trying to remove is in bucket[\(idx)] rather then the bucket we looked in at bucket[\(bucketID)]"
                )
            }
        }
        return false
    }

    public func markPeerReplaceable(_ peer: DHTPeerInfo) -> EventLoopFuture<Bool> {
        self.eventLoop.submit { self._markPeerReplaceable(peer) }
    }

    private func _markPeerReplaceable(_ peer: DHTPeerInfo) -> Bool {
        self._modifyPeer(peer, modifier: { $0.replaceable = true })
    }

    public func markPeerIrreplaceable(_ peer: DHTPeerInfo) -> EventLoopFuture<Bool> {
        self.eventLoop.submit { self._markPeerIrreplaceable(peer) }
    }

    private func _markPeerIrreplaceable(_ peer: DHTPeerInfo) -> Bool {
        self._modifyPeer(peer, modifier: { $0.replaceable = false })
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
        self.buckets.reduce(into: [DHTPeerInfo]()) { partialResult, bucket in
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
        self._modifyPeer(peer, modifier: { $0.lastSuccessfulOutboundQueryAt = at })
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
        guard !self.buckets.isEmpty else {
            self.buckets.append(Bucket())
            return
        }

        /// Keep unfolding until the tail bucket actually has room for a new peer.
        ///
        /// A bucket never holds more than `bucketSize` peers, so `newBucket.count >= self.bucketSize`
        /// means the split moved every peer out of the old tail, leaving the new tail just as full as
        /// the bucket we started with. Stopping there would send `_addPeer` down the eviction path even
        /// though another unfold would have made room for free.
        /// - Note: see `testUnfoldsUntilTailBucketHasRoom` for more info
        /// - Note: Each pass splits at a common prefix length one greater than the last, so this
        ///   terminates once the split CPL exceeds the highest CPL in the bucket, at which point the
        ///   new bucket comes back empty.
        while true {
            let newBucket = self.buckets[self.buckets.count - 1].split(
                commonPrefixLength: self.buckets.count - 1,
                targetID: self.localDHTID.bytes
            )
            self.buckets.append(newBucket)

            if newBucket.count < self.bucketSize { return }
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
        return [DHTPeerInfo](
            peersToSort.sorted(by: { lhs, rhs in
                peer.compareDistancesFromSelf(to: lhs.dhtID, and: rhs.dhtID) == .firstKey
            }).prefix(count)
        )
    }

    /// Returns the total count of all peers in the RoutingTable
    public func totalPeers() -> EventLoopFuture<Int> {
        self.eventLoop.submit { self._size() }
    }

    /// Returns the total count of all peers in the RoutingTable
    ///
    /// Caller is responsible for ensuring this is called on our eventloop
    private func _size() -> Int {
        self.buckets.reduce(into: 0) { partialResult, bucket in
            partialResult += bucket.count
        }
    }

    /// ListPeers returns a list of all peers from all buckets in the routing table.
    ///
    /// Caller is responsible for ensuring this is called on our eventloop
    private func _listPeers() -> [PeerID] {
        self.buckets.reduce(into: [PeerID]()) { partialResult, bucket in
            partialResult += bucket.peerIDs()
        }
    }

    func prettyPrint() {
        print(self.description)
    }

    /// - TODO: Implement me...
    func getDiversityStats() -> [String: Any] {
        [:]
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
        self.buckets.last(where: { !$0.isEmpty })?.maxCommonPrefixLength(target: self.localDHTID.bytes) ?? 0
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
