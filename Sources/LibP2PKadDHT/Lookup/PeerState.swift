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

extension KadDHT.QueryEngine {

    /// Where a peer stands in one lookup, following go's `qpeerset`.
    enum PeerState: Sendable {
        /// Heard of, not yet queried.
        case heard
        /// Query in flight.
        case waiting
        /// Responded.
        case queried
        /// Failed to respond.
        case unreachable
    }

    /// The peers one lookup has heard of, kept sorted by XOR distance to the target.
    ///
    /// Replaces the old capacity-k `LookupList`: every peer heard of is retained, so a peer that
    /// gets bumped past k by closer candidates is still available when those candidates turn out to
    /// be unreachable, and the result set can be filled to k without a separate overflow list.
    struct PeerSet {
        private struct Entry {
            let peer: PeerInfo
            let key: KadDHT.Key
            var state: PeerState
        }

        let target: KadDHT.Key
        private var entries: [Entry] = []
        private var known: Set<String> = []

        init(target: KadDHT.Key, seeds: [PeerInfo] = []) {
            self.target = target
            self.insert(seeds)
        }

        /// How many peers we've heard of.
        var count: Int { self.entries.count }

        /// Queries in flight.
        var inFlight: Int { self.entries.reduce(0) { $0 + ($1.state == .waiting ? 1 : 0) } }

        /// Nothing left to query, and nothing in flight.
        var isStarved: Bool {
            !self.entries.contains { $0.state == .heard || $0.state == .waiting }
        }

        /// Adds `peer` as a candidate, unless we've heard of it already.
        @discardableResult
        mutating func insert(_ peer: PeerInfo) -> Bool {
            let id = peer.peer.b58String
            guard !self.known.contains(id) else { return false }
            self.known.insert(id)

            let entry = Entry(peer: peer, key: KadDHT.Key(peer.peer, keySpace: .xor), state: .heard)
            let index =
                self.entries.firstIndex {
                    self.target.compareDistancesFromSelf(to: entry.key, and: $0.key) == .firstKey
                } ?? self.entries.count
            self.entries.insert(entry, at: index)
            return true
        }

        /// Adds every peer we haven't heard of yet, returning how many were new.
        @discardableResult
        mutating func insert(_ peers: [PeerInfo]) -> Int {
            var added = 0
            for peer in peers where self.insert(peer) { added += 1 }
            return added
        }

        /// The closest peer we haven't queried yet, moved to `.waiting`.
        mutating func nextToQuery() -> PeerInfo? {
            guard let index = self.entries.firstIndex(where: { $0.state == .heard }) else { return nil }
            self.entries[index].state = .waiting
            return self.entries[index].peer
        }

        mutating func mark(_ peer: PeerID, as state: PeerState) {
            guard let index = self.entries.firstIndex(where: { $0.peer.peer == peer }) else { return }
            self.entries[index].state = state
        }

        /// the β closest peers we know of have all responded, so nothing closer is going to turn up.
        func isComplete(resiliency: Int) -> Bool {
            let closest = self.entries.lazy.filter { $0.state != .unreachable }.prefix(resiliency)
            guard !closest.isEmpty else { return false }
            return closest.allSatisfy { $0.state == .queried }
        }

        /// The `n` closest peers that responded.
        func responded(_ n: Int) -> [PeerInfo] {
            self.entries.lazy.filter { $0.state == .queried }.prefix(n).map { $0.peer }
        }

        /// The peers in `state`, closest first.
        func peers(in state: PeerState) -> [PeerInfo] {
            self.entries.lazy.filter { $0.state == state }.map { $0.peer }
        }
    }
}
