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
import CryptoSwift
import LibP2PCore
import Multihash

extension KadDHT {
    struct Key: Equatable, Hashable {
        /// - TODO: Move Distance calculations onto KeySpace
        enum KeySpace {
            case xor
        }

        let keySpace: KeySpace
        let original: [UInt8]
        let bytes: [UInt8]

        init(_ peer: PeerID, keySpace: KeySpace = .xor) {
            self.init(peer.id, keySpace: keySpace)
        }

        init(_ bytes: [UInt8], keySpace: KeySpace = .xor) {
            self.keySpace = keySpace
            /// Store the original ID
            self.original = bytes
            /// Hash the ID for DHT Key conformance
            self.bytes = Digest.sha2(bytes, variant: .sha256) //SHA2(variant: .sha256).calculate(for: bytes)
        }

        /// Used for testing purposes only
        internal init(preHashedBytes: [UInt8], keySpace: KeySpace = .xor) {
            self.keySpace = keySpace
            self.original = []
            self.bytes = preHashedBytes
        }

        /// Creates a key of specified length with a predictable prefix, used for testing purposes
        internal init(prefix: [UInt8], length: Int = 20, keySpace: KeySpace = .xor) {
            self.keySpace = keySpace
            self.original = []
            var prehashedBytes = Array<UInt8>(repeating: 0, count: length)
            for i in 0..<prefix.count {
                prehashedBytes[i] = prefix[i]
            }
            self.bytes = prehashedBytes
        }
    }
}

extension KadDHT.Key: Comparable {
    /// Measures the distance between two keys
    internal static func distanceBetween(k0: KadDHT.Key, k1: KadDHT.Key) -> [UInt8] {
        let k0Bytes = k0.bytes
        let k1Bytes = k1.bytes
        guard k0Bytes.count == k1Bytes.count else { print("Error: Keys must be the same length"); return [] }
        return k0Bytes.enumerated().map { idx, byte in
            k1Bytes[idx] ^ byte
        }
    }

    /// Measures the distance between us and the specified key
    internal func distanceTo(key: KadDHT.Key) -> [UInt8] {
        return KadDHT.Key.distanceBetween(k0: self, k1: key)
    }

    internal enum ComparativeDistance: Int8 {
        case secondKey = -1
        case sameDistance = 0
        case firstKey = 1
    }

    /// Compares the distances between two peers/keys from a certain peer/key
    ///
    /// Returns  1 if the first key is closer
    /// Returns -1 if the second key is closer
    /// Returns  0 if the keys are the same distance apart (aka equal)
    internal static func compareDistances(from: KadDHT.Key, to key1: KadDHT.Key, and key2: KadDHT.Key) -> ComparativeDistance {
        let p0Bytes = from.bytes
        let p1Bytes = key1.bytes
        let p2Bytes = key2.bytes
        guard p0Bytes.count == p1Bytes.count, p0Bytes.count == p2Bytes.count else { print("Error: Keys must be the same length"); return .sameDistance }
        for (idx, byte) in p0Bytes.enumerated() {
            let bit1 = p1Bytes[idx] ^ byte
            let bit2 = p2Bytes[idx] ^ byte
            if bit1 > bit2 { return .secondKey }
            if bit1 < bit2 { return .firstKey }
        }
        return .sameDistance
    }

//    enum ComparativeDistance:Int8 {
//        case firstKeyCloser  =  1
//        case secondKeyCloser = -1
//        case equal           =  0
//    }
//
    /// Determines which of the two keys is closer to the this key
    internal func compareDistancesFromSelf(to key1: KadDHT.Key, and key2: KadDHT.Key) -> ComparativeDistance {
        return KadDHT.Key.compareDistances(from: self, to: key1, and: key2)
    }

    static let ZeroKey = {
        KadDHT.Key(preHashedBytes: Array<UInt8>(repeating: 0, count: 32))
    }()

    static func < (lhs: KadDHT.Key, rhs: KadDHT.Key) -> Bool {
        return self.ZeroKey.compareDistancesFromSelf(to: lhs, and: rhs).rawValue > 0
    }

    /// Returns the hex string representation of the keys underlying bytes
    func toHex() -> String { self.bytes.toHexString() }
    /// Returns the hex string representation of the keys underlying bytes
    func toString() -> String { self.toHex() }
    /// Returns the hex string representation of the keys underlying bytes
    func toBinary() -> String { self.bytes.asString(base: .base2) }
}

extension Array where Element == KadDHT.Key {
    /// Sorts an array of `KadDHT.Keys`, in place, based on their `Distance` from the specified key, ordered closest to furthest.
    /// - Note: The notion of `Distance` is derived using the specified `KeySpace` which defaults to Kademlia XOR.
    mutating func sort(byDistanceToKey target: KadDHT.Key) {
        self.sort { lhs, rhs in
            let comp = target.compareDistancesFromSelf(to: lhs, and: rhs)
            return comp.rawValue > 0
        }
    }

    /// Returns an array of sorted `KadDHT.Keys` based on their `Distance` from the specified key, ordered closest to furthest.
    /// - Note: The notion of `Distance` is derived using the specified `KeySpace` which defaults to Kademlia XOR.
    func sorted(byDistanceToKey target: KadDHT.Key) -> [KadDHT.Key] {
        self.sorted { lhs, rhs in
            let comp = target.compareDistancesFromSelf(to: lhs, and: rhs)
            return comp.rawValue > 0
        }
    }
}

extension KadDHT.Key {
    internal func commonPrefixLength(with peer: KadDHT.Key) -> Int {
        return self.bytes.commonPrefixLength(with: peer.bytes)
    }
}

extension KadDHT {
    /// This method attempts to take a key in the form of bytes and convert it into a human readable "/<namespace>/<multihash>" string for debugging
    /// - Parameter key: The key in bytes that you'd like to log
    /// - Returns: The most human readable string we can make
    static func keyToHumanReadableString(_ key: [UInt8]) -> String {
        if let namespaceBytes = KadDHT.extractNamespace(key), let namespace = String(data: Data(namespaceBytes), encoding: .utf8) {
            if let mh = try? Multihash(Array(key.dropFirst(namespace.count + 2))) {
                return "/\(namespace)/\(mh.b58String)"
            } else if let cid = try? CID(Array(key.dropFirst(namespace.count + 2))) {
                return "/\(namespace)/\(cid.multihash.b58String)"
            } else {
                return "/\(namespace)/\(key.dropFirst(namespaceBytes.count + 2))"
            }
        } else {
            if let mh = try? Multihash(key) {
                return "\(mh.b58String)"
            } else if let cid = try? CID(key) {
                return "\(cid.multihash.b58String)"
            } else {
                return "\(key)"
            }
        }
    }

    /// This method attempts to extract a namespace prefixed key of the form "/namespace/<multihash>"
    /// - Parameter key: The key to extract the prefixed namespace from
    /// - Returns: The namespace bytes if they exist (excluding the forward slashes), or nil if the key isn't prefixed with a namespace
    /// - Note: "/" in utf8 == 47
    static func extractNamespace(_ key: [UInt8]) -> [UInt8]? {
        guard key.first == UInt8(47) else { return nil }
        guard let idx = key.dropFirst().firstIndex(of: UInt8(47)) else { return nil }
        return Array(key[1..<idx])
    }
}
