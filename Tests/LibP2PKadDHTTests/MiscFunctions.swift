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

/// Decodes a `Query` the way the receive path actually sees it.
///
/// On the wire a query is `uvarint(len) + protobuf`, which is what `Query.encode()` produces. Inbound,
/// the route's `VarintFrameDecoder` (see `registerDHTRoute`) strips that length prefix before
/// `Query.decode` ever runs, so `Query.decode` deliberately expects a *bare* protobuf.
///
/// That makes `encode` and `decode` asymmetric by design — feeding `encode()` output straight into
/// `decode()` fails with `malformedProtobuf`, because the leading length byte parses as a bogus field
/// tag. Tests that want a round trip have to unframe first, which is what this helper does.
func decodeQueryFrame(_ encoded: [UInt8]) throws -> KadDHT.Query {
    try KadDHT.Query.decode(Array(encoded.dropFirst(uVarInt(encoded).bytesRead)))
}

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
    guard key1.count == key2.count else {
        print("Error: Keys must be the same length")
        return []
    }
    return key1.enumerated().map { idx, byte in key2[idx] ^ byte }
}

func compareDistances(from: [UInt8], to key1: [UInt8], and key2: [UInt8]) -> Int8 {
    guard from.count == key1.count, from.count == key2.count else {
        print("Error: Keys must be the same length")
        return 0
    }
    for (idx, byte) in from.enumerated() {
        let bit1 = key1[idx] ^ byte
        let bit2 = key2[idx] ^ byte
        if bit1 > bit2 { return -1 }
        if bit1 < bit2 { return 1 }
    }
    return 0
}

func bytesToInt(_ bytes: [UInt8]) -> UInt64 {
    let b = [UInt8](bytes.prefix(8))
    var value: UInt64 = 0
    let data = NSData(bytes: b, length: 8)
    data.getBytes(&value, length: 8)
    value = UInt64(bigEndian: value)

    if value == 0 {
        let b = [UInt8](bytes.suffix(8))
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
    distanceBetween(n0: to, n1: from) < distanceBetween(n0: than, n1: from)
}

/// Adds two UInt8 Byte arrays together
func sumByteArrays(_ bytes1: [UInt8], bytes2: [UInt8]) -> [UInt8] {
    /// Ensure the arrays are of equal length
    var b1: [UInt8] = []
    var b2: [UInt8] = []
    if bytes1.count > bytes2.count {
        b1 = bytes1
        b2 = [UInt8](repeating: 0, count: bytes1.count - bytes2.count) + bytes2
    } else if bytes2.count > bytes1.count {
        b1 = [UInt8](repeating: 0, count: bytes2.count - bytes1.count) + bytes1
        b2 = bytes2
    } else {
        b1 = bytes1
        b2 = bytes2
    }
    var summation: [UInt8] = []
    var carry: Bool = false
    for (i, byte) in b1.enumerated().reversed() {
        let temp: UInt16 = UInt16(byte) + UInt16(b2[i]) + (carry ? 1 : 0)
        summation.insert(UInt8(temp % 256), at: 0)
        if temp > 255 { carry = true } else { carry = false }
    }
    if carry { summation.insert(1, at: 0) }
    return summation
}

// MARK: - k-closest completeness

/// Grades a whole simulated network against the routing tables it *should* have converged on.
///
/// Kademlia doesn't impose a global ordering on nodes, so there's no meaningful "is the network
/// sorted?" question to ask. The invariant it actually guarantees is local: every node should know
/// the `k` peers nearest to itself in XOR keyspace. That's what this measures.
///
/// For each node we compute the theoretically optimal bucket — the `k` nearest of all the other
/// nodes in the network, which we can determine exactly because the test owns every node — and
/// check how many of them are actually in that node's routing table. Unlike a positional/ordering
/// score, this is sign-stable, has a fixed 0–100% scale, and degrades meaningfully: a node missing
/// 2 of its 8 nearest peers scores 75% whether the network has 20 nodes or 20,000.
struct KClosestCompleteness {
    /// One node's grade.
    struct NodeGrade {
        let peer: PeerID
        /// The `k` nearest other nodes in the network — the bucket this node *should* hold.
        let expected: [PeerID]
        /// Of `expected`, the ones actually present in the node's routing table.
        let found: [PeerID]
        /// Of `expected`, the ones absent from the node's routing table.
        let missing: [PeerID]
        /// Total peers in the routing table, including ones outside the k-closest set.
        let tableSize: Int

        /// `1.0` when the node holds every one of its k nearest peers.
        ///
        /// A node in a network of one has nothing to know, which we treat as trivially complete
        /// rather than as a divide-by-zero.
        var score: Double {
            guard !expected.isEmpty else { return 1.0 }
            return Double(found.count) / Double(expected.count)
        }
    }

    let k: Int
    let grades: [NodeGrade]

    /// Mean per-node completeness across the network, `0.0...1.0`.
    var score: Double {
        guard !grades.isEmpty else { return 1.0 }
        return grades.reduce(0.0) { $0 + $1.score } / Double(grades.count)
    }

    /// Nodes holding all `k` of their nearest peers.
    var perfectNodes: Int { grades.filter { $0.score >= 1.0 }.count }

    /// The single least-converged node, useful for spotting one straggler dragging the mean down.
    var worst: NodeGrade? { grades.min { $0.score < $1.score } }
}

/// Grades `nodes` on k-closest completeness.
///
/// - Parameters:
///   - nodes: Every node in the simulated network. The grade is only meaningful if this is the
///     *complete* set, since the optimal bucket is derived from it.
///   - k: Bucket size to grade against. Defaults to each node's own `routingTable.bucketSize`,
///     which is what that node is actually trying to fill. Capped at `nodes.count - 1` so a
///     network smaller than `k + 1` can still reach 100%.
/// - Note: Throws rather than force-unwrapping the routing-table futures so a transport hiccup
///   surfaces as a test failure instead of a crash in the diagnostic.
func kClosestCompleteness(_ nodes: [KadDHT.Node], k: Int? = nil) throws -> KClosestCompleteness {
    /// Precompute each node's keyspace position once — `KadDHT.Key` re-hashes the PeerID on every
    /// init, and the comparator below runs O(n log n) times per node.
    let positions = nodes.map { (peer: $0.peerID, key: KadDHT.Key($0.peerID, keySpace: .xor)) }

    var grades: [KClosestCompleteness.NodeGrade] = []
    var effectiveK = 0

    for (index, node) in nodes.enumerated() {
        let selfKey = positions[index].key
        let bucketSize = min(k ?? node.routingTable.bucketSize, max(nodes.count - 1, 0))
        effectiveK = max(effectiveK, bucketSize)

        /// The optimal bucket: every other node, sorted by XOR distance from this one, truncated
        /// to `bucketSize`.
        let expected = positions.enumerated()
            .filter { $0.offset != index }
            .map { $0.element }
            .sorted { selfKey.compareDistancesFromSelf(to: $0.key, and: $1.key) == .firstKey }
            .prefix(bucketSize)
            .map { $0.peer }

        let table = try node.routingTable.getPeerInfos().wait()
        /// Match on `b58String` — `PeerID` equality is fine, but a string set keeps this O(n)
        /// instead of O(n²) across the containment checks below.
        let held = Set(table.map { $0.id.b58String })

        grades.append(
            KClosestCompleteness.NodeGrade(
                peer: node.peerID,
                expected: expected,
                found: expected.filter { held.contains($0.b58String) },
                missing: expected.filter { !held.contains($0.b58String) },
                tableSize: table.count
            )
        )
    }

    return KClosestCompleteness(k: effectiveK, grades: grades)
}

/// Prints a k-closest completeness report for `nodes`.
///
/// ```
/// k-closest completeness: 92.5% (k=8) — 15/20 nodes optimal
///   <peer.ID RGAUcD>  6/8  (table: 14)  missing: <peer.ID DcbL2i>, <peer.ID MkPNVp>
///   <peer.ID 9wCzih>  7/8  (table: 11)  missing: <peer.ID MGALkM>
/// ```
///
/// Only imperfect nodes are listed, so a fully converged network prints a single summary line.
internal func printKClosestCompleteness(_ nodes: [KadDHT.Node], k: Int? = nil) {
    guard let report = try? kClosestCompleteness(nodes, k: k) else {
        print("k-closest completeness: <unavailable — routing table read failed>")
        return
    }

    let percent = String(format: "%.1f", report.score * 100)
    print(
        "k-closest completeness: \(percent)% (k=\(report.k)) — \(report.perfectNodes)/\(report.grades.count) nodes optimal"
    )

    for grade in report.grades.sorted(by: { $0.score < $1.score }) where grade.score < 1.0 {
        let missing = grade.missing.map { $0.description }.joined(separator: ", ")
        print(
            "  \(grade.peer) \(grade.found.count)/\(grade.expected.count) (table: \(grade.tableSize)) missing: \(missing)"
        )
    }
}
