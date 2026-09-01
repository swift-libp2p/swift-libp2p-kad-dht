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
import Foundation
import LibP2P
import LibP2PNoise
import LibP2PTesting
import LibP2PYAMUX
import Multihash
import Testing

@testable import LibP2PKadDHT

extension LibP2PKadDHTTests {

    /// Which addresses we're willing to believe about a peer that contacts us.
    ///
    /// Trusting requester-supplied addresses over what identify already told us can pollute
    /// the routing table and potentially publishes undialable provider records.
    @Suite("Address Trust Tests", .serialized)
    final class AddressTrustTests {

        // MARK: - Inbound peer addresses

        /// With nothing on file the observed address is all we have.
        @Test func testUnknownPeerFallsBackToTheObservedAddress() async throws {
            try await withApp(configure: LibP2PKadDHTTests.dhtHost()) { app in
                let stranger = try PeerID(.Ed25519)
                let observed = try Multiaddr("/ip4/198.51.100.7/tcp/54321")

                let trusted = try await app.dht.kadDHT.trustedAddresses(for: stranger, observedOn: observed).get()

                #expect(trusted.peer == stranger)
                #expect(trusted.addresses == [observed])
            }
        }

        /// The addresses identify gave us come first; the observed one is only a trailing fallback.
        /// An inbound stream's address is normally an ephemeral source port nobody can dial back.
        @Test func testKnownPeerPrefersPeerstoreAddresses() async throws {
            try await withApp(configure: LibP2PKadDHTTests.dhtHost()) { app in
                let peer = try PeerID(.Ed25519)
                let announced = try Multiaddr("/ip4/203.0.113.5/tcp/4001")
                let observed = try Multiaddr("/ip4/203.0.113.5/tcp/61234")
                try await app.peers.add(peerInfo: PeerInfo(peer: peer, addresses: [announced])).get()

                let trusted = try await app.dht.kadDHT.trustedAddresses(for: peer, observedOn: observed).get()

                #expect(trusted.addresses.first == announced, "identify's address has to win the ordering")
                #expect(trusted.addresses.contains(observed), "the observed address is kept as a fallback")
            }
        }

        /// Merging shouldn't duplicate an address the peerstore already has.
        @Test func testObservedAddressIsNotDuplicated() async throws {
            try await withApp(configure: LibP2PKadDHTTests.dhtHost()) { app in
                let peer = try PeerID(.Ed25519)
                let observed = try Multiaddr("/ip4/203.0.113.9/tcp/4001")
                try await app.peers.add(peerInfo: PeerInfo(peer: peer, addresses: [observed])).get()

                let trusted = try await app.dht.kadDHT.trustedAddresses(for: peer, observedOn: observed).get()

                #expect(trusted.addresses == [observed])
            }
        }

        // MARK: - ADD_PROVIDER addresses

        /// go's `handleAddProvider` drops a record whose matching `providerPeers` entry carries no
        /// addresses. We used to substitute the address we observed, which publishes an ephemeral
        /// source port as a provider address, resulting in later GET_PROVIDERS receiving an undialable answer.
        @Test(.internalIntegrationTestsEnabled)
        func testAddProviderWithoutAdvertisedAddressesIsDropped() async throws {
            try await Self.withProviderExchange { client, server, key in
                _ = try? await client.dht.kadDHT._sendQuery(
                    .addProvider(key: key, providerPeers: []),
                    to: PeerInfo(peer: server.peerID, addresses: server.listenAddresses)
                ).get()

                let stored = try await Self.providers(for: key, on: server)
                #expect(stored.isEmpty, "a provider that advertises nothing shouldn't be recorded")
            }
        }

        /// Advertise properly and the record is kept, with the addresses the provider claimed
        /// rather than the one we saw.
        @Test(.internalIntegrationTestsEnabled)
        func testAddProviderWithAdvertisedAddressesIsRecorded() async throws {
            try await Self.withProviderExchange { client, server, key in
                let advertised = try DHT.Message.Peer(
                    PeerInfo(peer: client.peerID, addresses: client.listenAddresses)
                )
                _ = try? await client.dht.kadDHT._sendQuery(
                    .addProvider(key: key, providerPeers: [advertised]),
                    to: PeerInfo(peer: server.peerID, addresses: server.listenAddresses)
                ).get()

                let stored = try await Self.providers(for: key, on: server)
                #expect(stored.count == 1)
                #expect(stored.first?.id == Data(client.peerID.id))
                let recorded = try #require(try stored.first?.toPeerInfo())
                #expect(
                    client.listenAddresses.allSatisfy { recorded.addresses.contains($0) },
                    "the provider's own advertised addresses have to survive"
                )
            }
        }

        @Test(.internalIntegrationTestsEnabled)
        func testObservedProviderAddressIsAcceptedWhenOptedIn() async throws {
            try await Self.withProviderExchange(acceptObservedProviderAddress: true) { client, server, key in
                _ = try? await client.dht.kadDHT._sendQuery(
                    .addProvider(key: key, providerPeers: []),
                    to: PeerInfo(peer: server.peerID, addresses: server.listenAddresses)
                ).get()

                let stored = try await Self.providers(for: key, on: server)
                #expect(stored.count == 1, "opting in should record the observed address")
                #expect(stored.first?.id == Data(client.peerID.id))
            }
        }
    }
}

extension LibP2PKadDHTTests.AddressTrustTests {

    /// A client and a server, plus the provider key to exchange.
    ///
    /// `acceptObservedProviderAddress` configures the *server*, since it's the receiving side that
    /// decides whether to believe an address it wasn't told about.
    fileprivate static func withProviderExchange(
        acceptObservedProviderAddress: Bool = false,
        _ body: (Application, Application, [UInt8]) async throws -> Void
    ) async throws {
        let options = KadDHT.NodeOptions(
            connectionTimeout: .milliseconds(500),
            concurrency: 3,
            bucketSize: 5,
            maxPeers: 15,
            maxKeyValueStoreEntries: 10,
            supportLocalNetwork: true,
            acceptObservedProviderAddress: acceptObservedProviderAddress
        )
        try await withApp(configure: LibP2PKadDHTTests.dhtHost(mode: .server, options: options)) { server in
            try await withApp(
                configure: LibP2PKadDHTTests.dhtHost(
                    mode: .server,
                    options: options,
                    bootstrapPeers: [server.peerInfo]
                )
            ) { client in
                let key = try CID(try LibP2PKadDHTTests.syntheticCID("address-trust")).multihash.value
                try await body(client, server, key)
            }
        }
    }

    /// The provider records `app` holds for `key`.
    fileprivate static func providers(for key: [UInt8], on app: Application) async throws -> [DHT.Message.Peer] {
        try await app.dht.kadDHT.providerStore.getValue(
            forKey: KadDHT.Key(key, keySpace: .xor),
            default: []
        ).get()
    }
}
