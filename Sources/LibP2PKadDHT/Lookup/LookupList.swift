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

/// An always sorted list of Contacts with respect to the ID (sorted from closest to furthers using the .xor keyspace)
class LookupList {
    struct Contact {
        let peer: PeerInfo
        var processed: Bool = false

        mutating func markProcessed() {
            self.processed = true
        }
    }

    /// The target key that we're sorting with respect to
    let id: KadDHT.Key
    /// The max capacity of this list
    let capacity: Int
    /// Sorted list of Multiaddr / Peers discovered so far in this Lookup
    private var slots: [Contact]
    /// A list of peers that have been removed due to Errors encountered while dialing them
    private var removed: [Contact]
    /// An overflow list of good peers, that just happen to be further away then our current list...
    private var further: [Contact] = []
    private var furtherUnqueried: [Contact] = []

    private var canceled: Bool = false

    init(id: PeerID, capacity: Int, seeds: [PeerInfo] = []) {
        self.id = KadDHT.Key(id, keySpace: .xor)
        self.capacity = capacity
        self.slots = []
        self.removed = []
        //self.eventLoop = on
        self.insertMany(seeds)
    }

    init(id: KadDHT.Key, capacity: Int, seeds: [PeerInfo] = []) {
        self.id = id
        self.capacity = capacity
        self.slots = []
        self.removed = []
        //self.eventLoop = on
        self.insertMany(seeds)
    }

    /// Update the list with the discovered contact
    /// - If the contact is already in the list, nothing happens
    /// - If the contact is further away from the target then the furthest contact and the list is at capacity it's discarded
    /// - Otherwise the new contact is added to the list in it's correct order and the furthest peer is dropped
    @discardableResult
    public func insert(_ peer: PeerInfo) -> Bool {
        self._insert(peer)
    }

    private func _insert(_ peer: PeerInfo) -> Bool {
        guard !self.canceled else { return false }
        //guard let cid = ma.getPeerID(), let newPeer = try? PeerID(cid: cid) else { return false }
        guard !self.removed.contains(where: { $0.peer.peer.b58String == peer.peer.b58String }) else { return false }
        for i in 0..<self.slots.count {
            let contact = self.slots[i]
            switch self.id.compareDistancesFromSelf(to: KadDHT.Key(peer.peer), and: KadDHT.Key(contact.peer.peer)) {
            case .sameDistance:
                return true

            case .secondKey:
                continue

            case .firstKey:
                self.slots.insert(Contact(peer: peer), at: i)

                /// If our list exceeds max capacity, drop the last element
                if self.slots.count > self.capacity {
                    let removedPeer = self.slots.removeLast()
                    /// If we processed this peer and they didn't
                    if removedPeer.processed == true {
                        self.further.append(removedPeer)
                    } else {
                        self.furtherUnqueried.append(removedPeer)
                    }
                }

                return true
            }
        }
        if self.slots.count < self.capacity {
            self.slots.append(Contact(peer: peer))
            return true
        }
        return false
    }

    @discardableResult
    public func insertMany(_ peers: [PeerInfo]) -> [Bool] {
        self._insertMany(peers)
    }

    private func _insertMany(_ peers: [PeerInfo]) -> [Bool] {
        guard !self.canceled else { return [] }
        //print("LookupList::Attempting to insert \(peers.count) new peers")
        return peers.map { self._insert($0) }
    }

    @discardableResult
    public func remove(_ peerID: PeerID) -> Bool {
        self._remove(peerID)
    }

    private func _remove(_ peerID: PeerID) -> Bool {
        var didRemove = false
        for i in 0..<self.slots.count {
            if self.slots[i].peer.peer == peerID {
                self.removed.append(self.slots.remove(at: i))
                didRemove = true
                break
            }
        }
        /// Make sure we remove them from our overflow list as well...
        for i in 0..<self.further.count {
            if self.further[i].peer.peer == peerID {
                self.removed.append(self.further.remove(at: i))
                //return true
                break
            }
        }

        /// If we have furtherUnqueried Peers and space in our slots add them??
        //        while self.slots.count < self.capacity && !self.furtherUnqueried.isEmpty {
        //            print("LookupList::Inserting Peer From Unqueried Overflow List")
        //            let _ = self._insert(self.furtherUnqueried.removeFirst().peer)
        //        }

        return didRemove
    }

    /// Returns the first unprocessed contact in the list, or nil if all contacts have already been processed...
    public func next() -> PeerInfo? {
        self._next()
    }

    private func _next() -> PeerInfo? {
        guard let idx = self.slots.firstIndex(where: { $0.processed == false }) else { return nil }
        self.slots[idx].markProcessed()
        return self.slots[idx].peer
    }

    /// Returns all of the current Multiaddr in our LookupList
    public func all() -> [PeerInfo] {
        self._all()
    }

    private func _all() -> [PeerInfo] {
        var results: [PeerInfo] = []
        results.append(contentsOf: self.slots.map { $0.peer })
        /// If our final list is shy of our capcity/target then attempt to fill it with further peers that we're bumped out of our list with what apparently turned out to be unreachable / bad peers
        //        if results.count < self.capacity {
        //            print("LookupList::Adding an additional \(self.further.count) further peers")
        //            results.append(contentsOf: self.further.sorted(by: { lhs, rhs in
        //                self.id.compareDistancesFromSelf(to: KadDHT.Key(lhs.peer.id), and: KadDHT.Key(rhs.peer.id)) >= 0
        //            }).prefix(self.capacity - results.count).map { $0.peer })
        //        }

        return results
    }

    public func cancel() {
        self._cancel()
    }

    private func _cancel() {
        self.canceled = true
        for i in 0..<self.slots.count {
            self.slots[i].markProcessed()
        }
    }

    public func dumpMetrics() {
        print("*** Lookup List Metrics ***")
        print("Searching for key: \(self.id.bytes.asString(base: .base58btc))")
        print("Returned List: [\(self.all().map { $0.peer.description }.joined(separator: ", "))]")
        print("Failed Queries (removed peers): \(self.removed.count)")
        print("Further Queried Peers: \(self.further.count)")
        print("Further Unqueried Peers: \(self.furtherUnqueried.count)")
        print("----------------------------")
    }
}
