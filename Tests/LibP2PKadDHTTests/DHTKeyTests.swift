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
import XCTest

@testable import LibP2PKadDHT

class DHTKeyTests: XCTestCase {

    override func setUpWithError() throws {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDownWithError() throws {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testStuff() {
        let node: UInt8 = 0b00001110

        let nodeA: UInt8 = 0b00001111
        let nodeB: UInt8 = 0b00011110
        let nodeC: UInt8 = 0b01010101

        var nodes = Array([nodeA, nodeB, nodeC].reversed())
        //nodes.shuffle()
        print(nodes)

        /// Lets sort the nodes
        nodes.sort(by: { closer(to: $0, than: $1, from: node) })

        print(nodes)
    }

    func testZeroPrefixLength() {
        let byteArrays: [[UInt8]] = [
            [0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00],
            [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            [0x00, 0x58, 0xFF, 0x80, 0x00, 0x00, 0xF0],
        ]
        let zeroLengths = [24, 56, 9]

        for byteArray in byteArrays {
            print(byteArray.commonPrefixLength(with: [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]))
        }

        for bytes in byteArrays.enumerated() {
            print("\(bytes.element.zeroPrefixLength()) == \(zeroLengths[bytes.offset])")
            XCTAssertEqual(bytes.element.zeroPrefixLength(), zeroLengths[bytes.offset])
        }
    }

    func testXORKeySpace() {
        let ids: [[UInt8]] = [
            [0xFF, 0xFF, 0xFF, 0xFF],
            [0x00, 0x00, 0x00, 0x00],
            [0xFF, 0xFF, 0xFF, 0xF0],
        ]

        let ks: [(KadDHT.Key, KadDHT.Key)] = [
            (KadDHT.Key(ids[0]), KadDHT.Key(ids[0])),
            (KadDHT.Key(ids[1]), KadDHT.Key(ids[1])),
            (KadDHT.Key(ids[2]), KadDHT.Key(ids[2])),
        ]

        for (i, s) in ks.enumerated() {
            /// Assert that the same bytes results in the same DHTKey each time
            XCTAssertEqual(s.0, s.1)

            /// Assert that the original bytes we're modified
            XCTAssertEqual(s.0.original, ids[i])

            /// Assert that our SHA256 DHTKey is 32 bytes
            XCTAssertEqual(s.0.bytes.count, 32)
        }

        for (i, s) in ks.enumerated() {
            if i == 0 { continue }

            /// Ensure distance calc is symmetrical
            print(
                "Symmetrical Distance Calc: \(bytesToInt(s.0.distanceTo(key: ks[i-1].0))) | \(bytesToInt(ks[i-1].0.distanceTo(key: s.0)))"
            )
            XCTAssertEqual(s.0.distanceTo(key: ks[i - 1].0), ks[i - 1].0.distanceTo(key: s.0))

            /// Ensure neighboring keys aren't equal to one another
            XCTAssertNotEqual(s.0, ks[i - 1].0)

        }

    }

    func testDistanceAndCenterSorting() {
        let byteArrays: [[UInt8]] = [
            [
                173, 149, 19, 27, 192, 183, 153, 192, 177, 175, 71, 127, 177, 79, 207, 38, 166, 169, 247, 96, 121, 228,
                139, 240, 144, 172, 183, 232, 54, 123, 253, 14,
            ],
            [
                223, 63, 97, 152, 4, 169, 47, 219, 64, 87, 25, 45, 196, 61, 215, 72, 234, 119, 138, 220, 82, 188, 73,
                140, 232, 5, 36, 192, 20, 184, 17, 25,
            ],
            [
                73, 176, 221, 176, 149, 143, 22, 42, 129, 124, 213, 114, 232, 95, 189, 154, 18, 3, 122, 132, 32, 199,
                53, 185, 58, 157, 117, 78, 52, 146, 157, 127,
            ],
            [
                73, 176, 221, 176, 149, 143, 22, 42, 129, 124, 213, 114, 232, 95, 189, 154, 18, 3, 122, 132, 32, 199,
                53, 185, 58, 157, 117, 78, 52, 146, 157, 127,
            ],
            [
                73, 176, 221, 176, 149, 143, 22, 42, 129, 124, 213, 114, 232, 95, 189, 154, 18, 3, 122, 132, 32, 199,
                53, 185, 58, 157, 117, 78, 52, 146, 157, 126,
            ],
            [
                73, 0, 221, 176, 149, 143, 22, 42, 129, 124, 213, 114, 232, 95, 189, 154, 18, 3, 122, 132, 32, 199, 53,
                185, 58, 157, 117, 78, 52, 146, 157, 127,
            ],
        ]

        let keys = byteArrays.map { KadDHT.Key(preHashedBytes: $0, keySpace: .xor) }

        print(keys[2].bytes)
        print(keys[3].bytes)
        print(keys[2].distanceTo(key: keys[3]))
        XCTAssertEqual(0, bytesToInt(keys[2].distanceTo(key: keys[3])))

        print(keys[2].distanceTo(key: keys[4]))
        XCTAssertEqual(1, bytesToInt(keys[2].distanceTo(key: keys[4])))

        print(keys[2].distanceTo(key: keys[5]))

        /// Sort the keys by distance with respect to keys[2]
        let sorted = keys.sorted(
            byDistanceToKey: KadDHT.Key(
                preHashedBytes: [
                    73, 176, 221, 176, 149, 143, 22, 42, 129, 124, 213, 114, 232, 95, 189, 154, 18, 3, 122, 132, 32,
                    199, 53, 185, 58, 157, 117, 78, 52, 146, 157, 127,
                ],
                keySpace: .xor
            )
        )
        let expectedOrder = [2, 3, 4, 5, 1, 0]
        sorted.forEach { print($0) }

        for idx in expectedOrder.enumerated() {
            XCTAssertEqual(keys[idx.element], sorted[idx.offset])
        }
    }

    func testSumByteArray() throws {
        let arr1 = try KadDHT.Key(PeerID(.Ed25519))
        let arr2 = try KadDHT.Key(PeerID(.Ed25519))

        print(arr1.bytes)
        print("+")
        print(arr2.bytes)
        print(
            "------------------------------------------------------------------------------------------------------------------------------"
        )
        var summation: [UInt8] = []
        var carry: Bool = false
        arr1.bytes.enumerated().reversed().forEach { i, byte in
            let temp: UInt16 = UInt16(byte) + UInt16(arr2.bytes[i]) + (carry ? 1 : 0)
            summation.insert(UInt8(temp % 256), at: 0)
            if temp > 255 { carry = true } else { carry = false }
        }
        if carry { summation.insert(1, at: 0) }
        print(summation)
    }

    func testByteAddition() {
        let five: UInt8 = 255
        let four: UInt8 = 1

        let sum: UInt16 = UInt16(five) + UInt16(four)
        print(sum)
        print(UInt8(sum % 256))
    }

    /* The following KadDHT tests were lifted from https://github.com/jeanlauliac/kademlia-dht/blob/master/test/id.test.js */
    func testKadDHTZeroKey() throws {
        /// Defaults to 32 bytes
        let id0 = KadDHT.Key.ZeroKey
        let stringRep = "0000000000000000000000000000000000000000000000000000000000000000"
        let arrayRep: [UInt8] = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]

        XCTAssertEqual(arrayRep.count, 32)
        XCTAssertEqual(id0.bytes, arrayRep)
        XCTAssertEqual(stringRep.count, 64)
        XCTAssertEqual(id0.toString(), stringRep)

        XCTAssertEqual(id0.bytes.asString(base: .base16), id0.bytes.toHexString())
    }

    func testKadDHTZeroKeyBits() throws {
        /// Defaults to 32 bytes
        let id0 = KadDHT.Key.ZeroKey
        let stringRep = "0000000000000000000000000000000000000000000000000000000000000000"
        let arrayRep: [UInt8] = [
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]

        XCTAssertEqual(arrayRep.count, 32)
        XCTAssertEqual(id0.bytes, arrayRep)
        XCTAssertEqual(stringRep.count, 64)
        XCTAssertEqual(id0.toString(), stringRep)

        XCTAssertEqual(arrayRep.commonPrefixLengthBits(with: id0.bytes), 256)

        XCTAssertEqual(id0.bytes.asString(base: .base16), id0.bytes.toHexString())
    }

    /// Our KadDHT Implementation defaults to Sha256, in this test we use Sha1...
    func testkadDHTKeyFromString() throws {
        let id0 = KadDHT.Key("the cake is a lie".bytes)

        var hasher = SHA1()
        let _ = try hasher.update(withBytes: "the cake is a lie".bytes)
        let digest = try hasher.finish()

        print(digest.toHexString())

        XCTAssertEqual(digest.toHexString(), "4e13b0c28e17087366ac4d67801ae0835bf9e9a1")

        print(id0.toHex())
        print(id0.original.toHexString())
    }

    func testkadDHTKeyDistance() throws {

        var hasher = SHA1()
        let _ = try hasher.update(withBytes: "the cake is a lie".bytes)
        let digest = try hasher.finish()

        let id1 = KadDHT.Key(preHashedBytes: digest)

        /// Ensure distance to self yeilds zero
        id1.distanceTo(key: id1).forEach {
            XCTAssertEqual($0, 0)
        }

        var hasher2 = SHA1()
        let _ = try hasher2.update(withBytes: "fubar".bytes)
        let digest2 = try hasher2.finish()

        let id2 = KadDHT.Key(preHashedBytes: digest2)

        let distance = "d4a6bfe55a3715cad428cedd03de6e39a04b43b6"

        XCTAssertEqual(id1.distanceTo(key: id2).toHexString(), distance)
    }

    func testCompareDistance() throws {
        let id0 = KadDHT.Key(prefix: [0b001100101])
        let id1 = KadDHT.Key(prefix: [0b011110011])
        let id2 = KadDHT.Key(prefix: [0b001010101])

        XCTAssertEqual(id0.compareDistancesFromSelf(to: id1, and: id2), .secondKey)
        XCTAssertEqual(id0.compareDistancesFromSelf(to: id2, and: id1), .firstKey)
        XCTAssertEqual(id0.compareDistancesFromSelf(to: id1, and: id1), .sameDistance)
    }

    func testkadDHTKeyEquality() throws {

        var hasher = SHA1()
        let _ = try hasher.update(withBytes: "the cake is a lie".bytes)
        let digest = try hasher.finish()

        let id1 = KadDHT.Key(preHashedBytes: digest)

        /// Ensure distance to self yeilds zero
        XCTAssertTrue(id1 == id1)

        var hasher2 = SHA1()
        let _ = try hasher2.update(withBytes: "fubar".bytes)
        let digest2 = try hasher2.finish()

        let id2 = KadDHT.Key(preHashedBytes: digest2)

        XCTAssertFalse(id1 == id2)
    }

    func testPeerIDDistance() throws {
        let peerID = try PeerID()
        print(peerID.hexString)

        //var peers:[PeerID] = (0..<256).map { _ in try! PeerID(.Ed25519) }
        //print(peers.map { $0.hexString })

        let peers: [String] = (0..<8).map { SHA1().calculate(for: [UInt8](arrayLiteral: $0)).asString(base: .base16) }
        print(peers)

        //peers.forEach {
        //    print( distanceBetween(p0: peerID, p1: $0) )
        //}
    }

    /// SHA1 Distance Test
    ///
    /// This test...
    /// 1) Generates 2048 Random SHA1 hashes,
    /// 2) Sorts the set of hashes by distance to our main hash,
    /// 3) Then calculates and prints the total distance for the 3 closest and 3 furthest hashes from our main hash
    /// - Note: Generating and sorting 2048 SHA1 Hashes takes about 0.264 seconds
    /// - Note: We use SHA1 here because the entropy is small enough we can notice distance results with a relatively low sample count.
    /// PeerID's byte size is just too large to get any meaningful data without generating massive sample lists...
    ///
    /// An example of printing the KBucket Distance (20,000 SHA1 Hashes)
    /// ```
    /// 5ba93c9db0cff93f52b521d7420e43f6eda2784f // Our ID
    /// Sorted Peer List (Closest to Furthest)
    /// 5ba91b3606ad0d94e793ee7f37fc31a69664194d (43618452829355)       // Closest Match
    /// 5bad77b85f86c014b329bcfa0f46d4b98e46f6d1 (1208526207269163)
    /// 5ba3d2a92b7c68070570741471cca46462cefeeb (3076659485053240)
    /// .
    /// .
    /// .
    /// a447683cdd3e6b141098f4ec5a08eb42d69a065a (18441770576439775787)
    /// a45a2afa9c482c5a66644b2ddc6b563fdd1ea1bf (18443109531396855141)
    /// a453828f7fbc614d8ff260c84f9ecafd5cda44b5 (18445264211848435826) // Furthest Match
    /// ^^^^^^^^^^^^^^^^^^
    /// 43618452829355
    /// 0
    /// ,----------------------------------------,
    /// | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 71 | 19928 |
    /// '----------------------------------------'
    /// ```
    func testSHA1_ID_Distance() throws {
        typealias Bytes = [UInt8]
        typealias KBucket = [Int]

        let ourID = SHA1().calculate(for: [0])
        print(ourID.asString(base: .base58btc))

        var peers: [Bytes] = (1...2000).map { _ in
            SHA1().calculate(for: [UInt8](withUnsafeBytes(of: Int64.random(in: 1...Int64.max), { $0 })))
        }
        ///peers.shuffle()
        //print("Unsorted Peer List")
        //peers.forEach { print($0.asString(base: .base16)) }
        //print("^^^^^^^^^^^^^^^^^^")

        peers.sort { lhs, rhs in
            compareDistances(from: ourID, to: lhs, and: rhs) == 1
        }

        /// The list should now be sorted, closest to furthest
        print("Sorted Peer List (Closest to Furthest)")
        //        peers.forEach { key in
        //            let dist = bytesToInt(distanceBetween(key: ourID, and: key))
        //            print("\(key.asString(base: .base16)) (\(dist))")
        //        }
        peers.prefix(3).forEach { key in
            let dist = bytesToInt(distanceBetween(key: ourID, and: key))
            print("\(key.asString(base: .base58btc)) (\(dist)) (cpl: \(ourID.commonPrefixLength(with: key)))")
        }
        print(".\n.\n.\n")
        peers.suffix(3).forEach { key in
            let dist = bytesToInt(distanceBetween(key: ourID, and: key))
            print("\(key.asString(base: .base58btc)) (\(dist)) (cpl: \(ourID.commonPrefixLength(with: key)))")
        }
        print("^^^^^^^^^^^^^^^^^^")

        let dist = distanceBetween(key: ourID, and: peers.first!)
        print(bytesToInt(dist))

        let dist2 = bytesToInt(distanceBetween(key: ourID, and: ourID))
        print(dist2)
        XCTAssertEqual(dist2, 0)

        var bucket = KBucket(repeating: 0, count: 20)
        peers.forEach { key in
            let dist = distanceBetween(key: ourID, and: key).zeroPrefixLength()
            bucket[19 - dist] += 1
            //if let idx = dist.prefix(8).firstIndex(where: { $0 != 0 } ) {
            //    bucket[8-idx] += 1
            //} else {
            //    bucket[8] += 1
            //}
        }

        let bucketString = bucket.map { "\($0)" }.joined(separator: " | ")
        print(",\(String(repeating: "-", count: bucketString.count + 2)),")
        print("| \(bucketString) |")
        print("'\(String(repeating: "-", count: bucketString.count + 2))'")
    }

}
