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

    /// The pre-encoded peerstore metadata values.
    /// - TODO: Remove these tests once swift-libp2p revamps it's peerstore and metadata book
    @Suite("Peer Metadata Tests", .serialized)
    struct PeerMetadataTests {

        @Test func testCachedEncodingsMatchAFreshEncode() throws {
            let necessary = try JSONEncoder().encode(MetadataBook.PrunableMetadata(prunable: .necessary)).byteArray
            let preferred = try JSONEncoder().encode(MetadataBook.PrunableMetadata(prunable: .preferred)).byteArray
            let prunable = try JSONEncoder().encode(MetadataBook.PrunableMetadata(prunable: .prunable)).byteArray

            #expect(KadDHT.PeerPrunableMetadata.necessary == necessary)
            #expect(KadDHT.PeerPrunableMetadata.preferred == preferred)
            #expect(KadDHT.PeerPrunableMetadata.prunable == prunable)
        }

        @Test func testEncodingIsStableAcrossCalls() throws {
            let encodings = try (0..<16).map { _ in
                try JSONEncoder().encode(MetadataBook.PrunableMetadata(prunable: .necessary)).byteArray
            }
            #expect(Set(encodings.map { Data($0) }).count == 1, "JSON encoding of the metadata isn't deterministic")
        }

        @Test func testCachedEncodingsRoundTrip() throws {
            let necessary = try JSONDecoder().decode(
                MetadataBook.PrunableMetadata.self,
                from: Data(KadDHT.PeerPrunableMetadata.necessary)
            )
            let preferred = try JSONDecoder().decode(
                MetadataBook.PrunableMetadata.self,
                from: Data(KadDHT.PeerPrunableMetadata.preferred)
            )
            let prunable = try JSONDecoder().decode(
                MetadataBook.PrunableMetadata.self,
                from: Data(KadDHT.PeerPrunableMetadata.prunable)
            )

            #expect(necessary.prunable == .necessary)
            #expect(preferred.prunable == .preferred)
            #expect(prunable.prunable == .prunable)
        }

        @Test func testTheCachedValuesDiffer() {
            #expect(!KadDHT.PeerPrunableMetadata.necessary.isEmpty)
            #expect(!KadDHT.PeerPrunableMetadata.preferred.isEmpty)
            #expect(!KadDHT.PeerPrunableMetadata.prunable.isEmpty)
            #expect(KadDHT.PeerPrunableMetadata.necessary != KadDHT.PeerPrunableMetadata.prunable)
            #expect(KadDHT.PeerPrunableMetadata.necessary != KadDHT.PeerPrunableMetadata.preferred)
            #expect(KadDHT.PeerPrunableMetadata.prunable != KadDHT.PeerPrunableMetadata.preferred)
        }

        // MARK: - Wiring

        /// A peer dropped from the routing table gets marked prunable in the peerstore, via the
        /// table's `peerRemovedHandler`.
        @Test func testDroppedPeerIsMarkedPrunable() async throws {
            try await withApp(configure: LibP2PKadDHTTests.dhtHost()) { app in
                let node = app.dht.kadDHT
                let peer = try PeerID(.Ed25519)
                try await app.peers.add(
                    peerInfo: PeerInfo(peer: peer, addresses: [try Multiaddr("/ip4/127.0.0.1/tcp/4001")])
                ).get()
                #expect(try await node.routingTable.addPeer(peer, isQueryPeer: true).get())

                #expect(try await node.routingTable.removePeer(peer).get())

                /// The handler fires off the removal, so give it a moment to land.
                let stored = try await Self.awaitPrunableMetadata(for: peer, on: app)
                #expect(
                    stored == KadDHT.PeerPrunableMetadata.prunable,
                    "a peer the table let go of should be prunable in the peerstore"
                )
            }
        }

        /// A bootstrap peer is marked necessary on the way in.
        @Test func testBootstrapPeerIsMarkedNecessary() async throws {
            let bootstrap = try generateRandomPeerInfo()
            try await withApp(
                configure: LibP2PKadDHTTests.dhtHost(bootstrapPeers: [bootstrap])
            ) { app in
                let stored = try await Self.awaitPrunableMetadata(for: bootstrap.peer, on: app)
                #expect(
                    stored == KadDHT.PeerPrunableMetadata.necessary,
                    "a bootstrap peer should be necessary, not prunable"
                )
            }
        }
    }
}

extension LibP2PKadDHTTests.PeerMetadataTests {

    /// Waits for `peer` to carry prunability metadata, since it's written off the back of a handler
    /// rather than synchronously with the call that triggered it.
    fileprivate static func awaitPrunableMetadata(
        for peer: PeerID,
        on app: Application,
        timeout: Duration = .seconds(2)
    ) async throws -> [UInt8]? {
        let deadline = ContinuousClock.now + timeout
        while true {
            let metadata = try? await app.peers.getMetadata(forPeer: peer).get()
            if let value = metadata?[MetadataBook.Keys.Prunable.rawValue] { return value }
            if ContinuousClock.now >= deadline { return nil }
            try await Task.sleep(for: .milliseconds(10))
        }
    }
}
