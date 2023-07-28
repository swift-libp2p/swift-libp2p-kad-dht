//
//  Array+Ext.swift
//  
//
//  Created by Brandon Toms on 9/9/22.
//

import LibP2PCore

extension Array where Element == KadDHT.Node {
    func sortedAbsolutely(using keyspace:KadDHT.Key.KeySpace = .xor) -> [KadDHT.Node] {
        let targetKey = KadDHT.Key(preHashedBytes: Array<UInt8>(repeating: 0, count: 32))
        return self.sorted { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(to: KadDHT.Key(lhs.peerID, keySpace: keyspace), and: KadDHT.Key(rhs.peerID, keySpace: keyspace))
            return comp == .firstKey
        }
    }
    
    func sorted(closestTo:KadDHT.Node, using keyspace:KadDHT.Key.KeySpace = .xor) -> [KadDHT.Node] {
        let targetKey = KadDHT.Key(closestTo.peerID, keySpace: keyspace)
        return self.sorted { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(to: KadDHT.Key(lhs.peerID, keySpace: keyspace), and: KadDHT.Key(rhs.peerID, keySpace: keyspace))
            return comp == .firstKey
        }
    }
}

extension Array where Element == UInt8 {
    func zeroPrefixLength() -> Int {
        var zeros = 0
        for byte in self {
            zeros += byte.leadingZeroBitCount
            if byte.leadingZeroBitCount != 8 { break }
        }
        return zeros
    }
}
