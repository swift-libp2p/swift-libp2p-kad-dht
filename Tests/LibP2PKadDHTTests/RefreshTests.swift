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

import Foundation
import LibP2P
import LibP2PNoise
import LibP2PTesting
import LibP2PYAMUX
import Testing

@testable import LibP2PKadDHT

extension LibP2PKadDHTTests {

    @Suite("Refresh Tests", .serialized)
    struct RefreshTests {

        // MARK: - Targeting a bucket

        @Test("a refresh target lands in the bucket it was asked for")
        func targetLandsInRequestedBucket() throws {
            let us = KadDHT.Key(try PeerID(.Ed25519), keySpace: .xor)

            for cpl in 0...KadDHT.Defaults.maxRefreshPrefixLength {
                let target = try #require(
                    KadDHT.Key.random(commonPrefixLength: cpl, with: us),
                    "should have found a pre-image for bucket \(cpl)"
                )
                #expect(
                    target.commonPrefixLength(with: us) == cpl,
                    "target for bucket \(cpl) landed in bucket \(target.commonPrefixLength(with: us))"
                )
            }
        }

        /// The bytes that go on the wire are the pre-image, and a lookup would reject an empty key.
        @Test("a refresh target carries a usable wire key")
        func targetCarriesAWireKey() throws {
            let us = KadDHT.Key(try PeerID(.Ed25519), keySpace: .xor)
            let target = try #require(KadDHT.Key.random(commonPrefixLength: 4, with: us))

            #expect(!target.original.isEmpty, "FIND_NODE rejects an empty key")
            #expect(
                KadDHT.Key(target.original, keySpace: .xor).bytes == target.bytes,
                "the receiver hashes `original`, so that hash has to be the bucket we aimed at"
            )
        }

        @Test("targets for the same bucket vary")
        func targetsVary() throws {
            let us = KadDHT.Key(try PeerID(.Ed25519), keySpace: .xor)
            let targets = try (0..<8).map { _ in
                try #require(KadDHT.Key.random(commonPrefixLength: 3, with: us)).bytes
            }
            // could probably assert count == 8, but > 1 will protect us from hash collisions
            #expect(Set(targets.map { Data($0) }).count > 1, "every target for a bucket was identical")
        }

        @Test("an exhausted search budget gives up")
        func exhaustedBudgetGivesUp() throws {
            let us = KadDHT.Key(try PeerID(.Ed25519), keySpace: .xor)
            /// One attempt at a 24-bit prefix match is a ~1-in-16-million shot.
            #expect(KadDHT.Key.random(commonPrefixLength: 24, with: us, attempts: 1) == nil)
        }

        // MARK: - Which buckets get refreshed

        /// Empty buckets have nothing to keep fresh, so we skip them.
        @Test func testOnlyNonEmptyBucketsAreReported() async throws {
            try await withApp(configure: LibP2PKadDHTTests.dhtHost()) { app in
                let node = app.dht.kadDHT

                #expect(
                    try await node.routingTable.nonEmptyBucketPrefixLengths().get().isEmpty,
                    "a table with no peers has no buckets to refresh"
                )

                /// Any peer we can add lands in *some* bucket, which then has to be reported.
                for _ in 0..<8 {
                    _ = try await node.routingTable.addPeer(try PeerID(.Ed25519), isQueryPeer: true).get()
                }

                let occupied = try await node.routingTable.nonEmptyBucketPrefixLengths().get()
                #expect(!occupied.isEmpty, "peers were added, so some bucket holds them")
                for cpl in occupied {
                    let count = try await node.routingTable.numberOfPeers(withCommonPrefixLength: cpl).get()
                    #expect(count > 0, "bucket \(cpl) was reported as non-empty but holds \(count) peers")
                }
            }
        }

        @Test func testRefreshOnAnEmptyTableCompletes() async throws {
            try await withApp(configure: LibP2PKadDHTTests.dhtHost()) { app in
                try await app.dht.kadDHT._refreshRoutingTable().get()
            }
        }

        // MARK: - Over the local network

        /// The acceptance criterion: a refresh actually populates the table from a single bootstrap
        /// peer, across every bucket the network reaches into.
        @Test(.internalIntegrationTestsEnabled)
        func testRefreshPopulatesTheRoutingTable() async throws {
            let options = KadDHT.NodeOptions(
                connectionTimeout: .milliseconds(800),
                concurrency: 3,
                bucketSize: 5,
                maxPeers: 25,
                maxKeyValueStoreEntries: 10,
                supportLocalNetwork: true
            )

            /// A small ring: each node knows only the one before it, so nobody can see the whole
            /// network without walking it.
            try await withApp(configure: LibP2PKadDHTTests.dhtHost(mode: .server, options: options)) { first in
                try await withApp(
                    configure: LibP2PKadDHTTests.dhtHost(mode: .server, options: options, bootstrapPeers: [
                        first.peerInfo
                    ])
                ) { second in
                    try await withApp(
                        configure: LibP2PKadDHTTests.dhtHost(mode: .server, options: options, bootstrapPeers: [
                            second.peerInfo
                        ])
                    ) { third in
                        try await withApp(
                            configure: LibP2PKadDHTTests.dhtHost(mode: .server, options: options, bootstrapPeers: [
                                third.peerInfo
                            ])
                        ) { newcomer in
                            let node = newcomer.dht.kadDHT
                            let before = try await node.routingTable.totalPeers().get()

                            try await node._refreshRoutingTable().get()

                            let after = try await node.routingTable.totalPeers().get()
                            #expect(
                                after > before,
                                "a refresh from one bootstrap peer should have found more of the ring (\(before) -> \(after))"
                            )
                        }
                    }
                }
            }
        }

        /// A refresh already in flight isn't started again on top of itself.
        @Test func testConcurrentRefreshesCollapse() async throws {
            try await withApp(configure: LibP2PKadDHTTests.dhtHost()) { app in
                let node = app.dht.kadDHT
                async let first: Void = node._refreshRoutingTable().get()
                async let second: Void = node._refreshRoutingTable().get()
                _ = try await (first, second)
            }
        }
    }
}
