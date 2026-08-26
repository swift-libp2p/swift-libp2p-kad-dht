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
import LibP2PYAMUX
import Multihash
import Testing

@testable import LibP2PKadDHT

/// Tests for ``KadDHT.Node/provide(cid:announce:)`` and the related
/// provider-record renewal + expiry machinery introduced in Phase 3.0.
///
/// - Unit-style tests (single-node, no real networking) verify the
///   state-machine behaviour: local storage, renewal eligibility,
///   expiry pruning, before-bootstrap handling.
/// - Integration-style tests (gated by `PerformInternalIntegrationTests=true`,
///   matching the existing `InternalNetworkTests` convention) verify
///   real two-node round trips.
@Suite("Provide Tests", .serialized)
final class ProvideTests {

    // MARK: - Unit-style

    @Test
    func testProvideStoresLocallyWithoutAnnounce() throws {
        let app = try makeApplication()
        defer { app.shutdown() }
        try app.start()
        let node = app.dht.kadDHT

        // Use a deterministic CID derived from short content.
        let cid = try CID(version: .v1, codec: .raw, multihash: try Multihash(raw: "phase-3.0-test".bytes, hashedWith: .sha2_256)).rawBuffer

        // Provide with announce:false so no network RPCs are sent.
        try node.provide(cid: cid, announce: false).wait()

        let kid = try providerRoutingKey(cid)
        let stored = try node.providerStore.getValue(forKey: kid, default: []).wait()
        #expect(node.localProviderKeys.contains(kid), "localProviderKeys must record the CID")
        #expect(node.localProviderCIDs[kid] == cid, "localProviderCIDs must preserve the original CID bytes")
        #expect(stored.count == 1, "providerStore should have exactly our entry; got \(stored.count)")
        #expect(stored.first?.id == Data(node.peerID.id), "stored provider should be us")

        let composite = KadDHT.Node.providerRecordKey(kid, peerID: node.peerID)
        #expect(node.providerRecordAddedAt[composite] != nil, "addedAt should be tracked")
    }

    @Test
    func testProvideExpiryPruning() throws {
        let app = try makeApplication()
        defer { app.shutdown() }
        try app.start()
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
        try node.providerStore.updateValue([foreignProvider], forKey: kid).wait()
        let composite = KadDHT.Node.providerRecordKey(kid, peerID: foreignPeerID)
        node.providerRecordAddedAt[composite] = Date().addingTimeInterval(-25 * 60 * 60)  // 25h ago — past 24h TTL

        // Run expiry; cutoff = now - 24h.
        let cutoff = Date().addingTimeInterval(-24 * 60 * 60)
        try node._expireOldProviderRecords(before: cutoff).wait()

        let afterPrune = try node.providerStore.getValue(forKey: kid, default: []).wait()
        #expect(afterPrune.isEmpty, "expired foreign provider record should be pruned")
        #expect(node.providerRecordAddedAt[composite] == nil, "stale timestamp entry should be removed")
    }

    @Test
    func testProvideRenewalEligibility() throws {
        let app = try makeApplication()
        defer { app.shutdown() }
        try app.start()
        let node = app.dht.kadDHT

        let cid = try syntheticCID("renewal-test")
        let kid = try providerRoutingKey(cid)
        // Set up the local record manually so we can manipulate the
        // addedAt timestamp before kicking the renewal job.
        try node.provide(cid: cid, announce: false).wait()
        let composite = KadDHT.Node.providerRecordKey(kid, peerID: node.peerID)

        // Backdate beyond the republish interval (12h).
        let staleTime = Date().addingTimeInterval(-13 * 60 * 60)
        node.providerRecordAddedAt[composite] = staleTime

        // Run the renewal job. With an empty routing table, the
        // announce path inside the job has no peers to send to, but
        // the job MUST still refresh our local addedAt timestamp on
        // completion — otherwise we'd retry on every heartbeat
        // forever.
        try node._republishProviderRecords().wait()

        let refreshed = try #require(node.providerRecordAddedAt[composite])
        #expect(refreshed > staleTime, "renewal job should refresh addedAt past the stale time")
    }

    @Test
    func testProvideBeforeBootstrap() throws {
        let app = try makeApplication()
        defer { app.shutdown() }
        try app.start()
        let node = app.dht.kadDHT

        let cid = try syntheticCID("before-bootstrap")
        let kid = try providerRoutingKey(cid)

        // Routing table starts empty. provide(announce:true) should
        // not crash. The iterative lookup finds zero peers; we send
        // ADD_PROVIDER to zero peers; locally we still record the
        // CID so a future heartbeat can re-attempt.
        try node.provide(cid: cid, announce: true).wait()

        #expect(node.localProviderKeys.contains(kid), "local record should be present even without peers")
        let stored = try node.providerStore.getValue(forKey: kid, default: []).wait()
        #expect(stored.count == 1, "local provider entry should exist")
    }

    // MARK: - Integration-style (gated)

    @Test(.internalIntegrationTestsEnabled)
    func testProvideThenFindRoundTrip() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { try! group.syncShutdownGracefully() }
        let dhtParams = KadDHT.NodeOptions(
            connectionTimeout: .milliseconds(500),
            maxConcurrentConnections: 3,
            bucketSize: 5,
            maxPeers: 15,
            maxKeyValueStoreEntries: 10,
            supportLocalNetwork: true
        )
        let nodeA = try makeHost(
            mode: .server,
            options: dhtParams,
            bootstrapPeers: [],
            usingGroup: .shared(group)
        )
        let nodeB = try makeHost(
            mode: .server,
            options: dhtParams,
            bootstrapPeers: [nodeA.peerInfo],
            usingGroup: .shared(group)
        )
        try nodeA.start()
        try nodeB.start()
        defer {
            nodeA.shutdown()
            nodeB.shutdown()
        }

        let cid = try syntheticCID("integration-round-trip")
        try nodeA.dht.kadDHT.provide(cid: cid, announce: true).wait()

        // Give the network a moment to settle.
        Thread.sleep(forTimeInterval: 0.5)

        let providers = try nodeB.dht.kadDHT.findProviders(cid: cid, count: 4).wait()
        #expect(!providers.isEmpty, "node B should have found at least one provider for the CID")
    }

    @Test(.internalIntegrationTestsEnabled)
    func testProvideMultipleKeys() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { try! group.syncShutdownGracefully() }
        let dhtParams = KadDHT.NodeOptions(
            connectionTimeout: .milliseconds(500),
            maxConcurrentConnections: 3,
            bucketSize: 5,
            maxPeers: 15,
            maxKeyValueStoreEntries: 10,
            supportLocalNetwork: true
        )
        let nodeA = try makeHost(
            mode: .server,
            options: dhtParams,
            bootstrapPeers: [],
            usingGroup: .shared(group)
        )
        let nodeB = try makeHost(
            mode: .server,
            options: dhtParams,
            bootstrapPeers: [nodeA.peerInfo],
            usingGroup: .shared(group)
        )
        try nodeA.start()
        try nodeB.start()
        defer {
            nodeA.shutdown()
            nodeB.shutdown()
        }

        let cids = try ["key-one", "key-two", "key-three"].map { try syntheticCID($0) }
        for cid in cids {
            try nodeA.dht.kadDHT.provide(cid: cid, announce: true).wait()
        }
        Thread.sleep(forTimeInterval: 0.5)

        for cid in cids {
            let providers = try nodeB.dht.kadDHT.findProviders(cid: cid, count: 4).wait()
            #expect(!providers.isEmpty, "node B should have found a provider for one of the CIDs")
        }
    }

    // MARK: - Helpers

    private func syntheticCID(_ tag: String) throws -> [UInt8] {
        try CID(
            version: .v1,
            codec: .raw,
            multihash: try Multihash(raw: tag.bytes, hashedWith: .sha2_256)
        ).rawBuffer
    }

    /// The routing-table key a provider record for `cid` is stored under.
    ///
    /// Provider records are keyed by the CID's *multihash*, not by the raw CID bytes, so that every
    /// CID encoding of the same content converges on one key. Tests have to derive the key the same
    /// way `provide(cid:announce:)` and `findProviders(cid:count:)` do — a CIDv1 `rawBuffer` carries
    /// a version/codec prefix, so keying off it yields a different (and unreachable) key.
    private func providerRoutingKey(_ cid: [UInt8]) throws -> KadDHT.Key {
        KadDHT.Key(try CID(cid).multihash.value, keySpace: .xor)
    }

    private func makeApplication() throws -> Application {
        let lib = try Application(.testing, peerID: PeerID(.Ed25519), eventLoopGroupProvider: .singleton)
        lib.logger.logLevel = .warning
        lib.security.use(.noise)
        lib.muxers.use(.yamux)
        lib.dht.use(
            .kadDHT(
                mode: .client,
                options: KadDHT.NodeOptions(
                    connectionTimeout: .milliseconds(150),
                    maxConcurrentConnections: 3,
                    bucketSize: 5,
                    maxPeers: 15,
                    maxKeyValueStoreEntries: 10,
                    supportLocalNetwork: true
                ),
                bootstrapPeers: [],
                autoUpdate: false
            )
        )
        lib.servers.use(.tcp(host: "127.0.0.1", port: self.nextPort))
        self.nextPort += 1
        return lib
    }

    private var nextPort: Int = 11000

    private func makeHost(
        mode: KadDHT.Mode = .client,
        options: KadDHT.NodeOptions = .default,
        bootstrapPeers: [PeerInfo] = [],
        usingGroup: Application.EventLoopGroupProvider = .singleton
    ) throws -> Application {
        let lib = try Application(.testing, peerID: PeerID(.Ed25519), eventLoopGroupProvider: usingGroup)
        lib.logger.logLevel = .warning
        lib.security.use(.noise)
        lib.muxers.use(.yamux)
        lib.dht.use(.kadDHT(mode: mode, options: options, bootstrapPeers: bootstrapPeers, autoUpdate: false))
        lib.servers.use(.tcp(host: "127.0.0.1", port: self.nextPort))
        self.nextPort += 1
        return lib
    }
}
