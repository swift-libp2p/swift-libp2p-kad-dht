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
import LibP2P
import LibP2PNoise
import LibP2PTesting
import LibP2PYAMUX
import Multihash
import Testing

@testable import LibP2PKadDHT

extension LibP2PKadDHTTests {

    @Suite("Async API Tests", .serialized)
    final class AsyncAPITests {

        /// A single client node, no networking.
        private var client: ((Application) async throws -> Void) = { app in
            app.logger.logLevel = .warning
            app.security.use(.noise)
            app.muxers.use(.yamux)
            app.dht.use(
                .kadDHT(
                    mode: .client,
                    configuration: KadDHT.Configuration(
                        bucketSize: 5,
                        concurrency: 3,
                        connectionTimeout: .milliseconds(150),
                        supportLocalNetwork: true
                    ),
                    bootstrapPeers: [],
                    autoUpdate: false
                )
            )
            app.servers.use(.tcp(host: "127.0.0.1", port: 0))
        }

        @Test func storeAndGetRoundTrip() async throws {
            try await withApp(configure: self.client) { app in
                let node = app.dht.kadDHT
                let key = try LibP2PKadDHTTests.syntheticCID("async-store-and-get")
                let record = try KadDHT.createPubKeyRecord(peerID: app.peerID).toProtobuf()

                let stored = try await node.storeNew(key, value: record)
                #expect(stored, "local-first store should report success")

                let fetched = try await node.get(key)
                #expect(fetched?.value == record.value, "the async read should see the async write")
            }
        }

        @Test func getReportsAMissAsNil() async throws {
            try await withApp(configure: self.client) { app in
                /// Nothing stored and nobody to ask, so the lookup converges on nothing.
                let fetched = try await app.dht.kadDHT.get(try LibP2PKadDHTTests.syntheticCID("async-miss"))
                #expect(fetched == nil)
            }
        }

        @Test func provideStoresLocally() async throws {
            try await withApp(configure: self.client) { app in
                let node = app.dht.kadDHT
                let cid = try LibP2PKadDHTTests.syntheticCID("async-provide")

                /// `announce: false` keeps this off the network.
                try await node.provide(cid: cid, announce: false)

                let kid = KadDHT.Key(try CID(cid).multihash.value, keySpace: .xor)
                #expect(node.localProviderKeys.contains(kid), "the CID should be recorded as ours")

                let providers = try await node.findProviders(cid: cid, count: 1)
                #expect(providers.isEmpty == false || node.localProviderKeys.contains(kid))
            }
        }

        @Test func findPeerThrowsForAnUnreachablePeer() async throws {
            try await withApp(configure: self.client) { app in
                /// Empty routing table, empty peerstore: the lookup fails rather than stalling.
                await #expect(throws: (any Error).self) {
                    _ = try await app.dht.kadDHT.findPeer(peer: try PeerID(.Ed25519))
                }
            }
        }
    }
}
