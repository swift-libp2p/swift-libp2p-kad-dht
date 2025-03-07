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

typealias ID = [UInt8]

extension ID {
    func commonPrefixLength(with: ID) -> Int {
        if KadDHT.CPL_BITS_NOT_BYTES {
            return self.commonPrefixLengthBits(with: with)
        } else {
            return self.commonPrefixLengthBytes(with: with)
        }
    }

    func commonPrefixLengthBytes(with: ID) -> Int {
        self.enumerated().first { elem in
            elem.element != with[elem.offset]
        }?.offset ?? 0
    }

    func commonPrefixLengthBits(with key: ID) -> Int {
        self.distanceBetween(key: self, and: key).zeroPrefixLength()
    }

    private func distanceBetween(key key1: [UInt8], and key2: [UInt8]) -> [UInt8] {
        guard key1.count == key2.count else {
            print("Error: Keys must be the same length")
            return []
        }
        return key1.enumerated().map { idx, byte in key2[idx] ^ byte }
    }
}
