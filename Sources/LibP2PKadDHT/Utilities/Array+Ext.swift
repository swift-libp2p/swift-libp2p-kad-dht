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

extension Array where Element == KadDHT.Node {
    func sortedAbsolutely(using keyspace: KadDHT.Key.KeySpace = .xor) -> [KadDHT.Node] {
        let targetKey = KadDHT.Key(preHashedBytes: [UInt8](repeating: 0, count: 32))
        return self.sorted { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(
                to: KadDHT.Key(lhs.peerID, keySpace: keyspace),
                and: KadDHT.Key(rhs.peerID, keySpace: keyspace)
            )
            return comp == .firstKey
        }
    }

    func sorted(closestTo: KadDHT.Node, using keyspace: KadDHT.Key.KeySpace = .xor) -> [KadDHT.Node] {
        let targetKey = KadDHT.Key(closestTo.peerID, keySpace: keyspace)
        return self.sorted { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(
                to: KadDHT.Key(lhs.peerID, keySpace: keyspace),
                and: KadDHT.Key(rhs.peerID, keySpace: keyspace)
            )
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
