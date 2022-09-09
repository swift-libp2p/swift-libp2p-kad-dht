//
//  PeerID+Ext.swift
//  
//
//  Created by Brandon Toms on 9/9/22.
//

import LibP2PCore

extension PeerID {
    internal func commonPrefixLength(with peer:PeerID) -> Int {
        return KadDHT.Key(self).bytes.commonPrefixLength(with: KadDHT.Key(peer).bytes)
    }
    
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
