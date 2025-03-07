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

import LibP2PCore

extension PeerID {
    internal func commonPrefixLength(with peer: PeerID) -> Int {
        KadDHT.Key(self).bytes.commonPrefixLength(with: KadDHT.Key(peer).bytes)
    }

    /// Measures the distance between two peers
    internal static func distanceBetween(p0: PeerID, p1: PeerID, using keyspace: KadDHT.Key.KeySpace = .xor) -> [UInt8]
    {
        KadDHT.Key.distanceBetween(k0: KadDHT.Key(p0, keySpace: keyspace), k1: KadDHT.Key(p1, keySpace: keyspace))
    }

    /// Measures the distance between us and the specified peer
    internal func distanceTo(peer: PeerID, using keyspace: KadDHT.Key.KeySpace = .xor) -> [UInt8] {
        KadDHT.Key.distanceBetween(k0: KadDHT.Key(self, keySpace: keyspace), k1: KadDHT.Key(peer, keySpace: keyspace))
    }

    /// Compares the distances between two peers from a certain peer
    ///
    /// Returns  1 if the first peer is closer
    /// Returns -1 if the second peer is closer
    /// Returns  0 if the peers are the same distance apart
    internal static func compareDistances(
        from: PeerID,
        to key1: PeerID,
        and key2: PeerID,
        using keyspace: KadDHT.Key.KeySpace = .xor
    ) -> KadDHT.Key.ComparativeDistance {
        KadDHT.Key.compareDistances(
            from: KadDHT.Key(from, keySpace: keyspace),
            to: KadDHT.Key(key1, keySpace: keyspace),
            and: KadDHT.Key(key2, keySpace: keyspace)
        )
    }

    /// Compares the distances between two peers from us
    internal func compareDistancesFromSelf(
        to key1: PeerID,
        and key2: PeerID,
        using keyspace: KadDHT.Key.KeySpace = .xor
    ) -> KadDHT.Key.ComparativeDistance {
        KadDHT.Key.compareDistances(
            from: KadDHT.Key(self, keySpace: keyspace),
            to: KadDHT.Key(key1, keySpace: keyspace),
            and: KadDHT.Key(key2, keySpace: keyspace)
        )
    }

    internal func absoluteDistance() -> [UInt8] {
        KadDHT.Key(self).bytes
    }
}

extension Array where Element == PeerID {
    /// Sorts an array of `PeerID`s, in place, based on their `Distance` from the specified peer, ordered closest to furthest.
    /// - Note: The notion of `Distance` is derived using the specified `KeySpace` which defaults to Kademlia XOR.
    /// - Note: This is an expensive, unoptimized, sorting method, it performs redundant SHA256 hashes of our PeerIDs for comparison sake.
    mutating func sort(byDistanceToPeer target: PeerID, usingKeySpace: KadDHT.Key.KeySpace = .xor) {
        let targetKey = KadDHT.Key(target, keySpace: usingKeySpace)
        self.sort { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(
                to: KadDHT.Key(lhs, keySpace: usingKeySpace),
                and: KadDHT.Key(rhs, keySpace: usingKeySpace)
            )
            return comp == .firstKey
        }
    }

    /// Returns an array of sorted `PeerID`s based on their `Distance` from the specified peer, ordered closest to furthest.
    /// - Note: The notion of `Distance` is derived using the specified `KeySpace` which defaults to Kademlia XOR.
    /// - Note: This is an expensive, unoptimized, sorting method, it performs redundant SHA256 hashes of our PeerIDs for comparison sake.
    func sorted(byDistanceToPeer target: PeerID, usingKeySpace: KadDHT.Key.KeySpace = .xor) -> [PeerID] {
        let targetKey = KadDHT.Key(target, keySpace: usingKeySpace)
        return self.sorted { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(
                to: KadDHT.Key(lhs, keySpace: usingKeySpace),
                and: KadDHT.Key(rhs, keySpace: usingKeySpace)
            )
            return comp == .firstKey
        }
    }

    /// Returns an array of sorted `PeerID`s based on their `Distance` from the specified peer, ordered closest to furthest.
    /// - Note: The notion of `Distance` is derived using the specified `KeySpace` which defaults to Kademlia XOR.
    /// - Note: This is an expensive, unoptimized, sorting method, it performs redundant SHA256 hashes of our PeerIDs for comparison sake.
    func sortedByAbsoluteDistance(usingKeySpace: KadDHT.Key.KeySpace = .xor) -> [PeerID] {
        let targetKey = KadDHT.Key(preHashedBytes: [UInt8](repeating: 0, count: 32))
        return self.sorted { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(
                to: KadDHT.Key(lhs, keySpace: usingKeySpace),
                and: KadDHT.Key(rhs, keySpace: usingKeySpace)
            )
            return comp == .firstKey
        }
    }
}

extension Array where Element == PeerID {
    func sortedAbsolutely(using keyspace: KadDHT.Key.KeySpace = .xor) -> [PeerID] {
        let targetKey = KadDHT.Key(preHashedBytes: [UInt8](repeating: 0, count: 32))
        return self.sorted { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(
                to: KadDHT.Key(lhs, keySpace: keyspace),
                and: KadDHT.Key(rhs, keySpace: keyspace)
            )
            return comp == .firstKey
        }
    }

    func sorted(closestTo: PeerID, using keyspace: KadDHT.Key.KeySpace = .xor) -> [PeerID] {
        let targetKey = KadDHT.Key(closestTo, keySpace: keyspace)
        return self.sorted { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(
                to: KadDHT.Key(lhs, keySpace: keyspace),
                and: KadDHT.Key(rhs, keySpace: keyspace)
            )
            return comp == .firstKey
        }
    }
}
