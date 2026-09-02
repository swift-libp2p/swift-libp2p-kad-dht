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

    @Suite("Usefulness Eviction Tests", .serialized)
    struct UsefulnessEvictionTests {

        // MARK: - Eviction preference

        /// Given two replaceable peers, the one whose proved useful most recently stays,
        /// even when the replacement strategy would've have picked it.
        @Test func testStalePeerIsEvictedBeforeAUsefulOne() throws {
            let local = RandomDHTPeer()
            let table = Self.table(for: local, bucketSize: 2, grace: .seconds(60))

            /// Distance and usefulness are deliberately set against each other: the stale peer is the
            /// *closer* of the two, so `.furthestReplaceable` left to itself would evict the useful
            /// one.
            let (stale, fresh) = Self.pair(closerFirst: local)
            #expect(try table.addPeer(stale, isQueryPeer: true).wait())
            #expect(try table.addPeer(fresh, isQueryPeer: true).wait())

            /// Push `stale` outside the grace window; `fresh` stays inside it.
            #expect(try table.updateLastUseful(at: Date().timeIntervalSince1970 - 600, for: stale).wait())

            /// The bucket is full, so admitting a third peer has to evict one of the two.
            let newcomer = RandomDHTPeer(withCPL: 1, wrt: local.dhtID, isReplaceable: true)
            #expect(try table.addPeer(newcomer, isQueryPeer: true, replacementStrategy: .furthestReplaceable).wait())

            let remaining = try table.getPeerInfos().wait().map { $0.id }
            #expect(!remaining.contains(stale.id), "the peer past its grace period should have been evicted")
            #expect(remaining.contains(fresh.id), "the recently useful peer should have been kept")
        }

        /// A peer that has never been useful still gets its grace period, measured from when it was
        /// admitted. Otherwise every newcomer would be first in the eviction queue and the table
        /// would churn.
        @Test func testNeverUsefulPeerIsNotStaleWhileWithinGrace() throws {
            let local = RandomDHTPeer()
            let table = Self.table(for: local, bucketSize: 2, grace: .seconds(60))

            /// `isQueryPeer: false` leaves `lastUsefulAt` nil, so the grace runs from `addedAt`.
            let newlyAdded = RandomDHTPeer(withCPL: 1, wrt: local.dhtID, isReplaceable: true)
            let alsoNew = RandomDHTPeer(withCPL: 1, wrt: local.dhtID, isReplaceable: true)
            #expect(try table.addPeer(newlyAdded, isQueryPeer: false).wait())
            #expect(try table.addPeer(alsoNew, isQueryPeer: false).wait())

            for peer in try table.getPeerInfos().wait() {
                #expect(peer.lastUsefulAt == nil, "a non-query peer shouldn't be stamped as useful")
            }

            /// Neither is stale, so `.furtherThanReplacement` is left to judge purely on distance and
            /// a peer further away than both can't get in.
            let further = Self.peer(furtherFrom: local, than: [newlyAdded, alsoNew])
            #expect(
                try table.addPeer(further, isQueryPeer: false, replacementStrategy: .furtherThanReplacement).wait()
                    == false,
                "with nobody stale, distance alone should turn away a further peer"
            )
        }

        /// Usefulness narrows the candidate field; it doesn't override the strategy's veto. A
        /// `.furtherThanReplacement` table shouldn't accept a worse peer just because someone is stale.
        @Test func testStalenessDoesNotOverrideTheStrategysVeto() throws {
            let local = RandomDHTPeer()
            let table = Self.table(for: local, bucketSize: 1, grace: .seconds(60))

            let resident = RandomDHTPeer(withCPL: 1, wrt: local.dhtID, isReplaceable: true)
            #expect(try table.addPeer(resident, isQueryPeer: true).wait())
            #expect(try table.updateLastUseful(at: Date().timeIntervalSince1970 - 600, for: resident).wait())

            let further = Self.peer(furtherFrom: local, than: [resident])
            #expect(
                try table.addPeer(further, isQueryPeer: true, replacementStrategy: .furtherThanReplacement).wait()
                    == false,
                "stale or not, `.furtherThanReplacement` shouldn't take a step backwards"
            )
        }

        /// Irreplaceable peers are never candidates, however stale they are.
        @Test func testIrreplaceablePeersAreNeverEvicted() throws {
            let local = RandomDHTPeer()
            let table = Self.table(for: local, bucketSize: 1, grace: .seconds(60))

            let resident = RandomDHTPeer(withCPL: 1, wrt: local.dhtID, isReplaceable: false)
            #expect(try table.addPeer(resident, isQueryPeer: true).wait())
            #expect(try table.updateLastUseful(at: Date().timeIntervalSince1970 - 600, for: resident).wait())

            let newcomer = RandomDHTPeer(withCPL: 1, wrt: local.dhtID, isReplaceable: true)
            #expect(
                try table.addPeer(newcomer, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait() == false
            )
            #expect(try table.getPeerInfos().wait().map { $0.id }.contains(resident.id))
        }

        // MARK: - Recording usefulness

        /// The engine has to mark a peer useful when its answer moved the lookup along, or nothing
        /// ever updates `lastUsefulAt` and the grace period is measuring `addedAt` forever.
        @Test func testEngineMarksPeersUsefulWhenTheyReturnCloserPeers() async throws {
            try await withApp(configure: LibP2PKadDHTTests.dhtHost()) { app in
                let node = app.dht.kadDHT
                let responder = try generateRandomPeerInfo()
                let referral = try generateRandomPeerInfo()

                /// Admitted as a non-query peer, so it starts with no usefulness stamp at all.
                #expect(try await node.routingTable.addPeer(responder.peer, isQueryPeer: false).get())
                #expect(try await Self.lastUsefulAt(of: responder.peer, in: node) == nil)

                _ = try await KadDHT.QueryEngine(
                    host: node,
                    target: KadDHT.Key(referral.peer, keySpace: .xor),
                    seeds: [responder]
                ) { _ in
                    node.eventLoop.makeSucceededFuture(.init(closerPeers: [referral]))
                }.run().get()

                #expect(
                    try await Self.lastUsefulAt(of: responder.peer, in: node) != nil,
                    "a peer that told us about someone closer has been marked useful"
                )
            }
        }

        /// Reachable but useless is exactly the case the grace period exists to prune, so an empty
        /// answer must *not* count.
        @Test func testEngineDoesNotMarkPeersUsefulForAnEmptyAnswer() async throws {
            try await withApp(configure: LibP2PKadDHTTests.dhtHost()) { app in
                let node = app.dht.kadDHT
                let responder = try generateRandomPeerInfo()

                #expect(try await node.routingTable.addPeer(responder.peer, isQueryPeer: false).get())

                _ = try await KadDHT.QueryEngine(
                    host: node,
                    target: KadDHT.Key(try PeerID(.Ed25519), keySpace: .xor),
                    seeds: [responder]
                ) { _ in
                    node.eventLoop.makeSucceededFuture(.init())
                }.run().get()

                #expect(
                    try await Self.lastUsefulAt(of: responder.peer, in: node) == nil,
                    "answering with nothing is reachable, not useful"
                )
            }
        }

        /// Handing us what we were looking for is the most useful thing a peer can do, even though it
        /// ends the walk and so reports no closer peers.
        @Test func testEngineMarksPeersUsefulWhenTheyEndTheLookup() async throws {
            try await withApp(configure: LibP2PKadDHTTests.dhtHost()) { app in
                let node = app.dht.kadDHT
                let responder = try generateRandomPeerInfo()

                #expect(try await node.routingTable.addPeer(responder.peer, isQueryPeer: false).get())

                _ = try await KadDHT.QueryEngine(
                    host: node,
                    target: KadDHT.Key(try PeerID(.Ed25519), keySpace: .xor),
                    seeds: [responder]
                ) { _ in
                    node.eventLoop.makeSucceededFuture(.init(stop: true))
                }.run().get()

                #expect(try await Self.lastUsefulAt(of: responder.peer, in: node) != nil)
            }
        }
    }
}

extension LibP2PKadDHTTests.UsefulnessEvictionTests {

    fileprivate static func table(for local: DHTPeerInfo, bucketSize: Int, grace: TimeAmount) -> RoutingTable {
        RoutingTable(
            eventloop: MultiThreadedEventLoopGroup(numberOfThreads: 1).next(),
            bucketSize: bucketSize,
            localPeerID: local.id,
            latency: .hours(1),
            peerstoreMetrics: [:],
            usefulnessGracePeriod: grace
        )
    }

    /// Two replaceable peers in the same bucket, the first strictly closer to `local` than the second.
    fileprivate static func pair(closerFirst local: DHTPeerInfo) -> (DHTPeerInfo, DHTPeerInfo) {
        while true {
            let a = RandomDHTPeer(withCPL: 1, wrt: local.dhtID, isReplaceable: true)
            let b = RandomDHTPeer(withCPL: 1, wrt: local.dhtID, isReplaceable: true)
            switch local.dhtID.compareDistancesFromSelf(to: a.dhtID, and: b.dhtID) {
            case .firstKey: return (a, b)
            case .secondKey: return (b, a)
            case .sameDistance: continue
            }
        }
    }

    /// A peer in the same bucket as `others` but further from `local` than any of them, so
    /// `.furtherThanReplacement` has grounds to refuse it.
    fileprivate static func peer(furtherFrom local: DHTPeerInfo, than others: [DHTPeerInfo]) -> DHTPeerInfo {
        while true {
            let candidate = RandomDHTPeer(withCPL: 1, wrt: local.dhtID, isReplaceable: true)
            let isFurtherThanAll = others.allSatisfy {
                local.dhtID.compareDistancesFromSelf(to: $0.dhtID, and: candidate.dhtID) == .firstKey
            }
            if isFurtherThanAll { return candidate }
        }
    }

    fileprivate static func lastUsefulAt(of peer: PeerID, in node: KadDHT.Node) async throws -> TimeInterval? {
        try await node.routingTable.find(id: peer).get()?.lastUsefulAt
    }
}
