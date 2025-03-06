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

import CryptoSwift
import LibP2P
import LibP2PCrypto
@testable import LibP2PKadDHT

func RandomPeerID() -> PeerID {
    try! PeerID(.Ed25519)
}

//func RandomPeerID(withCPL:Int, wrt:PeerID) -> PeerID {
//    let temp = try! PeerID(.Ed25519)
//
//    var bytes = temp.id
//    for i in 0..<withCPL {
//        bytes[i] = wrt.id[i]
//    }
//
//    return try! PeerID(fromBytesID: [UInt8](bytes))
//}

func RandomDHTKey() -> KadDHT.Key {
    try! KadDHT.Key(preHashedBytes: LibP2PCrypto.randomBytes(length: 32))
}

func RandomDHTKey(withCPL cpl: Int, wrt key: KadDHT.Key) -> KadDHT.Key {
    if KadDHT.CPL_BITS_NOT_BYTES {
        return RandomDHTKey(withCPLBits: cpl, wrt: key)
    } else {
        return RandomDHTKey(withCPLBytes: cpl, wrt: key)
    }
}

/// Bytes
private func RandomDHTKey(withCPLBytes cpl: Int, wrt key: KadDHT.Key) -> KadDHT.Key {
    var bytes = try! LibP2PCrypto.randomBytes(length: 32)
    if cpl == 0 {
        while bytes[0] == key.bytes[0] { bytes[0] = UInt8.random(in: 0..<255) }
    } else {
        bytes.removeFirst(cpl)
        bytes.insert(contentsOf: key.bytes[..<cpl], at: 0)
        while bytes[cpl] == key.bytes[cpl] { bytes[cpl] = UInt8.random(in: 0..<255) }
    }
    return KadDHT.Key(preHashedBytes: bytes)
}

/// Bits
private func RandomDHTKey(withCPLBits cpl: Int, wrt key: KadDHT.Key) -> KadDHT.Key {
    var bytes = try! LibP2PCrypto.randomBytes(length: 32)
    if cpl == 0 {
        bytes[0] = ~key.bytes[0]
    } else {
        let cplBytes = cpl / 8
        if cplBytes > 0 {
            bytes.removeFirst(cplBytes)
            bytes.insert(contentsOf: key.bytes[..<cplBytes], at: 0)
        }
        bytes[cplBytes] = ~key.bytes[cplBytes] ^ (255 << (8 - (cpl % 8)))
    }
    return KadDHT.Key(preHashedBytes: bytes)
}

func RandomDHTPeer(isReplaceable: Bool = false) -> DHTPeerInfo {
    let pid = try! PeerID(.Ed25519)
    let now = Date().timeIntervalSince1970
    return DHTPeerInfo(
        id: pid,
        lastUsefulAt: now,
        lastSuccessfulOutboundQueryAt: now,
        addedAt: now,
        dhtID: KadDHT.Key(pid),
        replaceable: isReplaceable
    )
}

/// - Warning: This is a dummy DHTPeer (the key is not derived from the id)!
func RandomDHTPeer(withCPL cpl: Int, wrt key: KadDHT.Key, isReplaceable: Bool = false) -> DHTPeerInfo {
    let pid = try! PeerID(.Ed25519)
    let now = Date().timeIntervalSince1970
    return DHTPeerInfo(
        id: pid,
        lastUsefulAt: now,
        lastSuccessfulOutboundQueryAt: now,
        addedAt: now,
        dhtID: RandomDHTKey(withCPL: cpl, wrt: key),
        replaceable: isReplaceable
    )
}

func generateRandomPeerInfo() throws -> PeerInfo {
    let pid = try PeerID(.Ed25519)
    return try PeerInfo(peer: pid, addresses: [Multiaddr("/ip4/127.0.0.1/tcp/1000/p2p/\(pid.b58String)")])
}

func distanceBetween(key key1: [UInt8], and key2: [UInt8]) -> [UInt8] {
    guard key1.count == key2.count else { print("Error: Keys must be the same length"); return [] }
    return key1.enumerated().map { idx, byte in key2[idx] ^ byte }
}

func compareDistances(from: [UInt8], to key1: [UInt8], and key2: [UInt8]) -> Int8 {
    guard from.count == key1.count, from.count == key2.count else { print("Error: Keys must be the same length"); return 0 }
    for (idx, byte) in from.enumerated() {
        let bit1 = key1[idx] ^ byte
        let bit2 = key2[idx] ^ byte
        if bit1 > bit2 { return -1 }
        if bit1 < bit2 { return 1 }
    }
    return 0
}

func bytesToInt(_ bytes: [UInt8]) -> UInt64 {
    let b = Array<UInt8>(bytes.prefix(8))
    var value: UInt64 = 0
    let data = NSData(bytes: b, length: 8)
    data.getBytes(&value, length: 8)
    value = UInt64(bigEndian: value)

    if value == 0 {
        let b = Array<UInt8>(bytes.suffix(8))
        let data = NSData(bytes: b, length: 8)
        data.getBytes(&value, length: 8)
        value = UInt64(bigEndian: value)
    }

    return value
}

func distanceBetween(n0: UInt8, n1: UInt8) -> UInt8 {
    n0 ^ n1
}

func closer(to: UInt8, than: UInt8, from: UInt8) -> Bool {
    return distanceBetween(n0: to, n1: from) < distanceBetween(n0: than, n1: from)
}

/// Adds two UInt8 Byte arrays together
func sumByteArrays(_ bytes1: [UInt8], bytes2: [UInt8]) -> [UInt8] {
    /// Ensure the arrays are of equal length
    var b1: [UInt8] = []
    var b2: [UInt8] = []
    if bytes1.count > bytes2.count {
        b1 = bytes1
        b2 = Array<UInt8>(repeating: 0, count: bytes1.count - bytes2.count) + bytes2
    } else if bytes2.count > bytes1.count {
        b1 = Array<UInt8>(repeating: 0, count: bytes2.count - bytes1.count) + bytes1
        b2 = bytes2
    } else {
        b1 = bytes1
        b2 = bytes2
    }
    var summation: [UInt8] = []
    var carry: Bool = false
    b1.enumerated().reversed().forEach { i, byte in
        let temp: UInt16 = UInt16(byte) + UInt16(b2[i]) + (carry ? 1 : 0)
        summation.insert(UInt8(temp % 256), at: 0)
        if temp > 255 { carry = true }
        else { carry = false }
    }
    if carry { summation.insert(1, at: 0) }
    return summation
}
