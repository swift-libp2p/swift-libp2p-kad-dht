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
import Foundation
import LibP2P
import LibP2PCrypto
import LibP2PNoise
import LibP2PTesting
import LibP2PYAMUX
import Multihash
import Testing

@testable import LibP2PKadDHT

extension LibP2PKadDHTTests {

    /// Tests for ``KadDHT.Node/provide(cid:announce:)`` and the related
    /// provider-record renewal + expiry machinery
    ///
    /// - Unit-style tests (single-node, no real networking) verify the
    ///   state-machine behaviour: local storage, renewal eligibility,
    ///   expiry pruning, before-bootstrap handling.
    /// - Integration-style tests (gated by `PerformInternalIntegrationTests=true`,
    ///   matching the existing `InternalNetworkTests` convention) verify
    ///   real two-node round trips.
    @Suite("Provide Tests", .serialized)
    final class ProvideTests {

        @Test func testProvideStoresLocallyWithoutAnnounce() async throws {
            try await withApp(configure: defaultDHTClientConfig) { app in
                let node = app.dht.kadDHT

                // Use a deterministic CID derived from short content.
                let cid = try CID(
                    version: .v1,
                    codec: .raw,
                    multihash: try Multihash(raw: "phase-3.0-test".bytes, hashedWith: .sha2_256)
                ).rawBuffer

                // Provide with announce:false so no network RPCs are sent.
                try await node.provide(cid: cid, announce: false).get()

                let kid = try providerRoutingKey(cid)
                let stored = try await node.providerStore.getValue(forKey: kid, default: []).get()
                #expect(node.localProviderKeys.contains(kid), "localProviderKeys must record the CID")
                #expect(node.localProviderCIDs[kid] == cid, "localProviderCIDs must preserve the original CID bytes")
                #expect(stored.count == 1, "providerStore should have exactly our entry; got \(stored.count)")
                #expect(stored.first?.id == Data(node.peerID.id), "stored provider should be us")

                let composite = KadDHT.Node.providerRecordKey(kid, peerID: node.peerID)
                #expect(node.providerRecordAddedAt[composite] != nil, "addedAt should be tracked")
            }
        }

        @Test func testProvideExpiryPruning() async throws {
            try await withApp(configure: defaultDHTClientConfig) { app in
                let node = app.dht.kadDHT

                // Stage: insert a synthetic provider record for a *foreign*
                // peer with an obviously-stale addedAt timestamp.
                let foreignPeerID = try PeerID(.Ed25519)
                let cid = try syntheticCID("expiry-test")
                let kid = try providerRoutingKey(cid)
                let foreignInfo = PeerInfo(peer: foreignPeerID, addresses: [])
                guard let foreignProvider = try? DHT.Message.Peer(foreignInfo) else {
                    Issue.record("could not encode foreign peer")
                    return
                }
                let _ = try await node.providerStore.updateValue([foreignProvider], forKey: kid).get()
                let composite = KadDHT.Node.providerRecordKey(kid, peerID: foreignPeerID)
                // 25h ago — past 24h TTL
                node.providerRecordAddedAt[composite] = Date().addingTimeInterval(-25 * 60 * 60)

                // Run expiry; cutoff = now - 24h.
                let cutoff = Date().addingTimeInterval(-24 * 60 * 60)
                try await node._expireOldProviderRecords(before: cutoff).get()

                let afterPrune = try await node.providerStore.getValue(forKey: kid, default: []).get()
                #expect(afterPrune.isEmpty, "expired foreign provider record should be pruned")
                #expect(node.providerRecordAddedAt[composite] == nil, "stale timestamp entry should be removed")
            }
        }

        /// Capacity pruning used to pop arbitrary entries off an unordered dictionary, so it could
        /// evict our own records while keeping stale foreign ones.
        @Test func testCapacityPruningKeepsOurRecordsAndEvictsStalestFirst() async throws {
            let options = KadDHT.NodeOptions(
                connectionTimeout: .milliseconds(150),
                concurrency: 3,
                bucketSize: 5,
                maxPeers: 15,
                maxKeyValueStoreEntries: 10,
                maxProviderStoreSize: 2,
                supportLocalNetwork: true
            )
            try await withApp(configure: dhtHost(mode: .client, options: options)) { app in
                let node = app.dht.kadDHT

                // Our own record — must survive pruning.
                let ourCID = try syntheticCID("capacity-ours")
                try await node.provide(cid: ourCID, announce: false).get()
                let ourKey = try providerRoutingKey(ourCID)

                // Two foreign records, both well inside the TTL so only capacity pruning applies.
                let recent = try await stageForeignProvider("capacity-recent", on: node, age: 60)
                let stalest = try await stageForeignProvider("capacity-stalest", on: node, age: 3600)

                #expect(try await node.providerStore.count().get() == 3)

                try await node._pruneProviders().get()

                let remaining = Set(try await node.providerStore.all().get().map { $0.key })
                #expect(remaining.contains(ourKey), "our own provider record must never be pruned")
                #expect(remaining.contains(recent), "the fresher foreign record should survive")
                #expect(!remaining.contains(stalest), "the stalest foreign record should be evicted")
                #expect(remaining.count == 2, "store should be pruned to maxProviderStoreSize")
            }
        }

        @Test func testProvideRenewalEligibility() async throws {
            try await withApp(configure: defaultDHTClientConfig) { app in
                let node = app.dht.kadDHT

                let cid = try syntheticCID("renewal-test")
                let kid = try providerRoutingKey(cid)
                // Set up the local record manually so we can manipulate the
                // addedAt timestamp before kicking the renewal job.
                try await node.provide(cid: cid, announce: false).get()
                let composite = KadDHT.Node.providerRecordKey(kid, peerID: node.peerID)

                // Backdate past the republish interval so the record is due.
                //
                // Derived from the node's own interval rather than hard-coded: this was `-13 * 60 * 60`
                // against a 12h interval, and silently stopped exercising the renewal path when the
                // interval moved to go's 22h (`amino.DefaultReprovideInterval`).
                let staleTime = Date().addingTimeInterval(-(node.providerRecordRepublishInterval + 3600))
                node.providerRecordAddedAt[composite] = staleTime

                // Run the renewal job. With an empty routing table, the
                // announce path inside the job has no peers to send to, but
                // the job MUST still refresh our local addedAt timestamp on
                // completion — otherwise we'd retry on every heartbeat
                // forever.
                try await node._republishProviderRecords().get()

                let refreshed = try #require(node.providerRecordAddedAt[composite])
                #expect(refreshed > staleTime, "renewal job should refresh addedAt past the stale time")
            }
        }

        @Test func testProvideBeforeBootstrap() async throws {
            try await withApp(configure: defaultDHTClientConfig) { app in
                let node = app.dht.kadDHT

                let cid = try syntheticCID("before-bootstrap")
                let kid = try providerRoutingKey(cid)

                // Routing table starts empty. provide(announce:true) should
                // not crash. The iterative lookup finds zero peers; we send
                // ADD_PROVIDER to zero peers; locally we still record the
                // CID so a future heartbeat can re-attempt.
                try await node.provide(cid: cid, announce: true).get()

                #expect(node.localProviderKeys.contains(kid), "local record should be present even without peers")
                let stored = try await node.providerStore.getValue(forKey: kid, default: []).get()
                #expect(stored.count == 1, "local provider entry should exist")
            }
        }

        @Test func testProvideThenFindRoundTrip() async throws {
            let dhtParams = KadDHT.NodeOptions(
                connectionTimeout: .milliseconds(500),
                concurrency: 3,
                bucketSize: 5,
                maxPeers: 15,
                maxKeyValueStoreEntries: 10,
                supportLocalNetwork: true
            )
            try await withApp(configure: dhtHost(mode: .server, options: dhtParams, bootstrapPeers: [])) { nodeA in
                try await withApp(
                    configure: dhtHost(mode: .server, options: dhtParams, bootstrapPeers: [nodeA.peerInfo])
                ) { nodeB in
                    // NodeA creates and provides a CID
                    let cid = try syntheticCID("integration-round-trip")
                    try await nodeA.dht.kadDHT.provide(cid: cid, announce: true).get()

                    // Give the network a moment to settle.
                    try await Task.sleep(for: .milliseconds(25))

                    // NodeB should be able to find providers for the given CID
                    let providers = try await nodeB.dht.kadDHT.findProviders(cid: cid, count: 4).get()
                    #expect(!providers.isEmpty, "node B should have found at least one provider for the CID")
                }
            }
        }

        @Test func testProvideMultipleKeys() async throws {
            let dhtParams = KadDHT.NodeOptions(
                connectionTimeout: .milliseconds(500),
                concurrency: 3,
                bucketSize: 5,
                maxPeers: 15,
                maxKeyValueStoreEntries: 10,
                supportLocalNetwork: true
            )
            try await withApp(configure: dhtHost(mode: .server, options: dhtParams, bootstrapPeers: [])) { nodeA in
                try await withApp(
                    configure: dhtHost(mode: .server, options: dhtParams, bootstrapPeers: [nodeA.peerInfo])
                ) { nodeB in
                    // NodeA creates and provides multiple CIDs
                    let cids = try ["key-one", "key-two", "key-three"].map { try syntheticCID($0) }
                    for cid in cids {
                        try await nodeA.dht.kadDHT.provide(cid: cid, announce: true).get()
                    }

                    // Give the network a moment to settle.
                    try await Task.sleep(for: .milliseconds(25))

                    // NodeB should be able to find providers for each of the given CIDs
                    for cid in cids {
                        let providers = try await nodeB.dht.kadDHT.findProviders(cid: cid, count: 4).get()
                        #expect(!providers.isEmpty, "node B should have found a provider for cid \(cid)")
                    }
                }
            }
        }

        // MARK: - Helpers

        /// The routing-table key a provider record for `cid` is stored under.
        ///
        /// Provider records are keyed by the CID's *multihash*, not by the raw CID bytes, so that every
        /// CID encoding of the same content converges on one key. Tests have to derive the key the same
        /// way `provide(cid:announce:)` and `findProviders(cid:count:)` do — a CIDv1 `rawBuffer` carries
        /// a version/codec prefix, so keying off it yields a different (and unreachable) key.
        private func providerRoutingKey(_ cid: [UInt8]) throws -> KadDHT.Key {
            KadDHT.Key(try CID(cid).multihash.value, keySpace: .xor)
        }

        /// Inserts a provider record for a random foreign peer, stamped `age` seconds ago.
        /// - Returns: The routing-table key the record was stored under.
        private func stageForeignProvider(
            _ tag: String,
            on node: KadDHT.Node,
            age: TimeInterval
        ) async throws -> KadDHT.Key {
            let peerID = try PeerID(.Ed25519)
            let kid = try providerRoutingKey(try syntheticCID(tag))
            let provider = try DHT.Message.Peer(PeerInfo(peer: peerID, addresses: []))
            let _ = try await node.providerStore.updateValue([provider], forKey: kid).get()
            node.providerRecordAddedAt[KadDHT.Node.providerRecordKey(kid, peerID: peerID)] =
                Date().addingTimeInterval(-age)
            return kid
        }

        /// Default app configuration for a DHT Client suitable for the above tests
        var defaultDHTClientConfig: ((Application) async throws -> Void) = { app in
            app.logger.logLevel = .warning
            app.security.use(.noise)
            app.muxers.use(.yamux)
            app.dht.use(
                .kadDHT(
                    mode: .client,
                    options: KadDHT.NodeOptions(
                        connectionTimeout: .milliseconds(150),
                        concurrency: 3,
                        bucketSize: 5,
                        maxPeers: 15,
                        maxKeyValueStoreEntries: 10,
                        supportLocalNetwork: true
                    ),
                    bootstrapPeers: [],
                    autoUpdate: false
                )
            )
            app.servers.use(.tcp(host: "127.0.0.1", port: 0))
        }
    }
}
