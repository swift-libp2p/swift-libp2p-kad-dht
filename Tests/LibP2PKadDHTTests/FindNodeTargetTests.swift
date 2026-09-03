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

    /// The `FIND_NODE` target exception, a server answers with the peer being looked for, whether or
    /// not that peer is in its routing table.
    ///
    /// A routing table isn't a directory of everyone we've met, only server-mode peers are admitted,
    /// and only when a bucket has room. So without this a `findPeer` for a client-only peer can never
    /// resolve, no matter how many of its neighbours we know, because nobody is allowed to hold it.
    @Suite("FindNode Target Tests", .serialized)
    struct FindNodeTargetTests {

        /// A peer known to the peerstore is appended to the response list.
        @Test(.internalIntegrationTestsEnabled)
        func testTargetKnownOnlyToThePeerstoreIsReturned() async throws {
            try await Self.withPair { client, server in
                let target = try Self.stageKnownPeer(on: server)

                #expect(
                    try await server.dht.kadDHT.routingTable.totalPeers().get() == 0,
                    "the target has to be absent from the table for this to be testing anything"
                )

                let closer = try await Self.findNode(target.peer, from: client, on: server)
                #expect(
                    closer.first?.id == Data(target.peer.id),
                    "the target should lead the answer, got \(closer.count) peer(s)"
                )
                let recovered = try #require(try closer.first?.toPeerInfo())
                #expect(recovered.addresses == target.addresses)
            }
        }

        /// A bare ID the requester can't dial costs a `closerPeers` slot and tells them nothing, so
        /// go prunes address-less entries and so do we.
        @Test(.internalIntegrationTestsEnabled)
        func testTargetWithNoKnownAddressesIsOmitted() async throws {
            try await Self.withPair { client, server in
                let target = try PeerID(.Ed25519)
                try await server.peers.add(peerInfo: PeerInfo(peer: target, addresses: [])).get()

                let closer = try await Self.findNode(target, from: client, on: server)
                #expect(
                    !closer.contains { $0.id == Data(target.id) },
                    "an entry we hold no addresses for shouldn't be advertised"
                )
            }
        }

        /// Never tell a peer about itself
        @Test(.internalIntegrationTestsEnabled)
        func testRequesterIsNotReturnedToItself() async throws {
            try await Self.withPair { client, server in
                /// The server knows the client
                let closer = try await Self.findNode(client.peerID, from: client, on: server)
                #expect(
                    !closer.contains { $0.id == Data(client.peerID.id) },
                    "the requester was echoed back to itself"
                )
            }
        }

        /// Canonical `FIND_NODE` walks arbitrary keys, so the target is often not a peer at all.
        @Test(.internalIntegrationTestsEnabled)
        func testNonPeerIDKeyStillAnswers() async throws {
            try await Self.withPair { client, server in
                let known = try Self.stageKnownPeer(on: server)
                _ = known

                let response = try await client.dht.kadDHT._sendQuery(
                    .findNode(key: (0..<32).map { UInt8($0) }),
                    to: PeerInfo(peer: server.peerID, addresses: server.listenAddresses)
                ).get()

                guard case .findNode = response else {
                    Issue.record("expected a findNode response, got \(response)")
                    return
                }
            }
        }

        /// The target still has to survive the k-bound on `closerPeers`, which is the reason it goes
        /// in front rather than on the end the way go does it.
        @Test(.internalIntegrationTestsEnabled)
        func testTargetSurvivesAFullRoutingTable() async throws {
            /// `Response.encode` bounds `closerPeers` at `Defaults.maxPeersPerMessage`, not at the
            /// node's configured `bucketSize`, so the table has to be able to fill *that* many slots
            /// before truncation is what's under test.
            let capacity = KadDHT.Defaults.maxPeersPerMessage
            try await Self.withPair(bucketSize: capacity) { client, server in
                let node = server.dht.kadDHT
                for index in 0..<(capacity + 5) {
                    let filler = try PeerID(.Ed25519)
                    try await server.peers.add(
                        peerInfo: PeerInfo(
                            peer: filler,
                            addresses: [try Multiaddr("/ip4/198.51.100.\(index % 250 + 1)/tcp/4001")]
                        )
                    ).get()
                    _ = try await node.routingTable.addPeer(filler, isQueryPeer: true).get()
                }

                let target = try Self.stageKnownPeer(on: server)
                let closer = try await Self.findNode(target.peer, from: client, on: server)

                #expect(
                    closer.count == capacity,
                    "the answer should be truncated, otherwise this isn't testing the k-bound"
                )
                #expect(
                    closer.first?.id == Data(target.peer.id),
                    "the target has to lead — appended, as go does it, it'd be the entry truncated away"
                )
            }
        }
    }
}

extension LibP2PKadDHTTests.FindNodeTargetTests {

    /// A client and a DHT server, both able to dial each other on loopback.
    fileprivate static func withPair(
        bucketSize: Int = 5,
        _ body: (Application, Application) async throws -> Void
    ) async throws {
        let configuration = KadDHT.Configuration(
            bucketSize: bucketSize,
            concurrency: 3,
            connectionTimeout: .milliseconds(800),
            supportLocalNetwork: true
        )
        try await withApp(configure: LibP2PKadDHTTests.dhtHost(mode: .server, configuration: configuration)) { server in
            try await withApp(
                configure: LibP2PKadDHTTests.dhtHost(
                    mode: .server,
                    configuration: configuration,
                    bootstrapPeers: [
                        server.peerInfo
                    ]
                )
            ) { client in
                try await body(client, server)
            }
        }
    }

    /// Puts a peer in `app`'s peerstore, and nowhere else. Nothing here touches the routing table:
    /// admission needs the peer to have announced the DHT protocol via identify, which a synthetic
    /// `PeerID` never does.
    fileprivate static func stageKnownPeer(on app: Application) throws -> PeerInfo {
        let peer = try PeerID(.Ed25519)
        let info = PeerInfo(
            peer: peer,
            addresses: [try Multiaddr("/ip4/192.0.2.10/tcp/4001/p2p/\(peer.b58String)")]
        )
        try app.peers.add(peerInfo: info).wait()
        return info
    }

    /// Asks `server` for `target` and returns the `closerPeers` it answered with.
    fileprivate static func findNode(
        _ target: PeerID,
        from client: Application,
        on server: Application
    ) async throws -> [DHT.Message.Peer] {
        let response = try await client.dht.kadDHT._sendQuery(
            .findNode(key: target.id),
            to: PeerInfo(peer: server.peerID, addresses: server.listenAddresses)
        ).get()

        guard case .findNode(let closerPeers) = response else {
            Issue.record("expected a findNode response, got \(response)")
            return []
        }
        return closerPeers
    }
}
