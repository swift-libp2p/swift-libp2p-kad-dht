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

import LibP2P
import LibP2PCrypto
import LibP2PNoise
import LibP2PTesting
import LibP2PYAMUX
import Testing

@testable import LibP2PKadDHT

extension LibP2PKadDHTTests {

    @Suite("Query Engine Peer Set Tests")
    struct PeerSetTests {

        private typealias PeerSet = KadDHT.QueryEngine.PeerSet

        @Test func startsEmptyAndTakesSeeds() throws {
            #expect(PeerSet(target: KadDHT.Key.Zero).count == 0)
            let seeds = try (0..<3).map { _ in try generateRandomPeerInfo() }
            #expect(PeerSet(target: KadDHT.Key.Zero, seeds: seeds).count == 3)
        }

        @Test func keepsPeersSortedByDistanceToTheTarget() throws {
            let ourID = try PeerID(.Ed25519)
            let target = KadDHT.Key(ourID, keySpace: .xor)
            var peers = PeerSet(target: target)

            for peer in try (0..<50).map({ _ in try generateRandomPeerInfo() }) {
                let inserted = peers.insert(peer)
                #expect(inserted)
            }

            /// Every peer is retained, the old capacity-k list dropped the overflow.
            #expect(peers.count == 50)

            let heard = peers.peers(in: .heard)
            #expect(heard.count == 50)
            for i in 0..<(heard.count - 1) {
                #expect(ourID.compareDistancesFromSelf(to: heard[i].peer, and: heard[i + 1].peer) == .firstKey)
            }
        }

        @Test func ignoresPeersItHasAlreadyHeardOf() throws {
            let peer = try generateRandomPeerInfo()
            var peers = PeerSet(target: KadDHT.Key.Zero, seeds: [peer])

            let reinserted = peers.insert(peer)
            #expect(!reinserted)
            #expect(peers.count == 1)
        }

        @Test func handsOutTheClosestUnqueriedPeerFirst() throws {
            let randomPeers = try (0..<20).map { _ in try generateRandomPeerInfo() }
            let sorted = randomPeers.map { $0.peer }.sortedAbsolutely()
            var peers = PeerSet(target: KadDHT.Key.Zero, seeds: randomPeers)

            for expected in sorted {
                let next = peers.nextToQuery()
                #expect(next?.peer == expected)
            }
            /// Every peer is now `.waiting`, so there's nothing left to hand out.
            let exhausted = peers.nextToQuery()
            #expect(exhausted == nil)
            #expect(peers.inFlight == 20)
        }

        @Test func onlyRespondersAreResults() throws {
            let randomPeers = try (0..<5).map { _ in try generateRandomPeerInfo() }
            let sorted = randomPeers.map { $0.peer }.sortedAbsolutely()
            var peers = PeerSet(target: KadDHT.Key.Zero, seeds: randomPeers)

            peers.mark(sorted[0], as: .unreachable)
            peers.mark(sorted[1], as: .queried)
            peers.mark(sorted[2], as: .queried)

            #expect(peers.responded(20).map { $0.peer } == [sorted[1], sorted[2]])
            /// And the result set is capped.
            #expect(peers.responded(1).map { $0.peer } == [sorted[1]])
        }

        /// Once the β closest peers we know of have all responded, nothing closer can turn up,
        /// so the lookup is done, even with peers left unqueried.
        @Test func isCompleteWhenTheBetaClosestHaveResponded() throws {
            let randomPeers = try (0..<10).map { _ in try generateRandomPeerInfo() }
            let sorted = randomPeers.map { $0.peer }.sortedAbsolutely()
            var peers = PeerSet(target: KadDHT.Key.Zero, seeds: randomPeers)

            #expect(!peers.isComplete(resiliency: 3))

            peers.mark(sorted[0], as: .queried)
            peers.mark(sorted[1], as: .queried)
            #expect(!peers.isComplete(resiliency: 3), "two of three isn't termination")

            peers.mark(sorted[2], as: .queried)
            #expect(peers.isComplete(resiliency: 3))
        }

        /// An unreachable peer doesn't hold the lookup open, the β closest *reachable* peers do.
        @Test func unreachablePeersDoNotBlockTermination() throws {
            let randomPeers = try (0..<10).map { _ in try generateRandomPeerInfo() }
            let sorted = randomPeers.map { $0.peer }.sortedAbsolutely()
            var peers = PeerSet(target: KadDHT.Key.Zero, seeds: randomPeers)

            peers.mark(sorted[0], as: .unreachable)
            peers.mark(sorted[1], as: .queried)
            peers.mark(sorted[2], as: .queried)
            #expect(!peers.isComplete(resiliency: 3))

            peers.mark(sorted[3], as: .queried)
            #expect(peers.isComplete(resiliency: 3))
        }

        @Test func isStarvedOnlyWhenNothingIsLeft() throws {
            var peers = PeerSet(target: KadDHT.Key.Zero)
            #expect(peers.isStarved, "no seeds means nothing to do")

            let peer = try generateRandomPeerInfo()
            let inserted = peers.insert(peer)
            #expect(inserted)
            #expect(!peers.isStarved)

            _ = peers.nextToQuery()
            #expect(!peers.isStarved, "a query is still in flight")

            peers.mark(peer.peer, as: .queried)
            #expect(peers.isStarved)
        }
    }

    @Suite("Query Engine Tests", .serialized)
    final class QueryEngineTests {

        /// A node with a known α/β and no network traffic of its own.
        private func engineHost(
            concurrency: Int,
            resiliency: Int,
            bucketSize: Int = KadDHT.Defaults.bucketSize
        ) -> ((Application) async throws -> Void) {
            LibP2PKadDHTTests.dhtHost(
                mode: .client,
                options: .init(
                    connectionTimeout: .milliseconds(150),
                    concurrency: concurrency,
                    resiliency: resiliency,
                    bucketSize: bucketSize,
                    supportLocalNetwork: true
                )
            )
        }

        /// β-termination: the engine stops well before draining its candidates.
        @Test func terminatesOnceTheClosestPeersHaveResponded() async throws {
            try await withApp(configure: engineHost(concurrency: 3, resiliency: 3)) { app in
                let node = app.dht.kadDHT
                let seeds = try (0..<30).map { _ in try generateRandomPeerInfo() }
                let queried = NIOLockedValueBox<[PeerID]>([])

                let results = try await KadDHT.QueryEngine(
                    host: node,
                    target: KadDHT.Key.Zero,
                    seeds: seeds
                ) { peer in
                    queried.withLockedValue { $0.append(peer.peer) }
                    return node.eventLoop.makeSucceededFuture(.init())
                }.run().get()

                let asked = queried.withLockedValue { $0.count }
                #expect(asked < seeds.count, "the whole candidate set shouldn't be drained")
                #expect(asked >= 3, "at least the β closest have to be asked")
                #expect(results.count == asked, "every responder is a result")
            }
        }

        /// The old capacity-k list let failures shrink the result set below k: peers bumped past k by
        /// closer candidates were gone for good, so when those candidates turned out to be dead there
        /// was nothing to fall back on. Every peer heard of is now retained.
        ///
        /// β is raised to k here so termination can't mask the effect, the lookup has to reach k
        /// live peers, past the five dead ones in front of them.
        @Test func fillsResultsToKDespiteFailures() async throws {
            try await withApp(configure: engineHost(concurrency: 4, resiliency: 5, bucketSize: 5)) { app in
                let node = app.dht.kadDHT
                let peers = try (0..<12).map { _ in try generateRandomPeerInfo() }
                let sorted = peers.map { $0.peer }.sortedAbsolutely()
                /// The five closest peers are all dead.
                let dead = Set(sorted.prefix(5).map { $0.b58String })

                let results = try await KadDHT.QueryEngine(
                    host: node,
                    target: KadDHT.Key.Zero,
                    seeds: peers
                ) { peer in
                    guard !dead.contains(peer.peer.b58String) else {
                        return node.eventLoop.makeFailedFuture(KadDHT.Errors.connectionTimedOut)
                    }
                    return node.eventLoop.makeSucceededFuture(.init())
                }.run().get()

                #expect(results.count == 5, "the result set should still be filled to k")
                #expect(results.allSatisfy { !dead.contains($0.peer.b58String) })
            }
        }

        /// Peers a response tells us about become candidates, so the lookup walks toward the target.
        ///
        /// β is set past the peer count so this measures referral-following alone, with the lookup
        /// running until nothing is left rather than stopping at the β closest.
        @Test func followsCloserPeersFromResponses() async throws {
            try await withApp(configure: engineHost(concurrency: 2, resiliency: 20, bucketSize: 20)) { app in
                let node = app.dht.kadDHT
                let seed = try generateRandomPeerInfo()
                let referred = try (0..<4).map { _ in try generateRandomPeerInfo() }
                let referrals = NIOLockedValueBox<[PeerInfo]>(referred)

                let results = try await KadDHT.QueryEngine(
                    host: node,
                    target: KadDHT.Key.Zero,
                    seeds: [seed]
                ) { _ in
                    /// The first response hands over everyone; later ones add nothing.
                    let peers = referrals.withLockedValue { pending -> [PeerInfo] in
                        defer { pending = [] }
                        return pending
                    }
                    return node.eventLoop.makeSucceededFuture(.init(closerPeers: peers))
                }.run().get()

                #expect(results.count == 5, "the seed plus everyone it referred us to")
            }
        }

        /// A step that asks to stop ends the lookup, this is what quorum and provider counts use.
        @Test func stopEndsTheLookupEarly() async throws {
            try await withApp(configure: engineHost(concurrency: 1, resiliency: 3)) { app in
                let node = app.dht.kadDHT
                let seeds = try (0..<10).map { _ in try generateRandomPeerInfo() }
                let queried = NIOLockedValueBox<Int>(0)

                let results = try await KadDHT.QueryEngine(
                    host: node,
                    target: KadDHT.Key.Zero,
                    seeds: seeds
                ) { _ in
                    queried.withLockedValue { $0 += 1 }
                    return node.eventLoop.makeSucceededFuture(.init(stop: true))
                }.run().get()

                #expect(queried.withLockedValue { $0 } == 1)
                #expect(results.count == 1)
            }
        }

        /// Lookups run on the node's event loop. Each one used to spin up its own
        /// `MultiThreadedEventLoopGroup` of `System.coreCount` threads and tear it down afterwards.
        @Test func runsOnTheHostsEventLoop() async throws {
            try await withApp(configure: engineHost(concurrency: 2, resiliency: 2)) { app in
                let node = app.dht.kadDHT
                let onHostLoop = NIOLockedValueBox<Bool>(false)

                _ = try await KadDHT.QueryEngine(
                    host: node,
                    target: KadDHT.Key.Zero,
                    seeds: try (0..<4).map { _ in try generateRandomPeerInfo() }
                ) { _ in
                    if node.eventLoop.inEventLoop { onHostLoop.withLockedValue { $0 = true } }
                    return node.eventLoop.makeSucceededFuture(.init())
                }.run().get()

                #expect(onHostLoop.withLockedValue { $0 }, "steps must be dispatched on the host's loop")
            }
        }

        /// A timeout returns whatever responded in time rather than hanging the caller.
        @Test func timeoutReturnsWhatItHas() async throws {
            try await withApp(configure: engineHost(concurrency: 2, resiliency: 3)) { app in
                let node = app.dht.kadDHT
                let seeds = try (0..<6).map { _ in try generateRandomPeerInfo() }

                let results = try await KadDHT.QueryEngine(
                    host: node,
                    target: KadDHT.Key.Zero,
                    seeds: seeds,
                    timeout: .milliseconds(50)
                ) { _ in
                    /// Never answers.
                    node.eventLoop.makePromise(of: KadDHT.QueryEngine.StepResult.self).futureResult
                }.run().get()

                #expect(results.isEmpty)
            }
        }

        @Test func refusesToRunTwice() async throws {
            try await withApp(configure: engineHost(concurrency: 2, resiliency: 2)) { app in
                let node = app.dht.kadDHT
                let engine = KadDHT.QueryEngine(
                    host: node,
                    target: KadDHT.Key.Zero,
                    seeds: try (0..<2).map { _ in try generateRandomPeerInfo() }
                ) { _ in node.eventLoop.makeSucceededFuture(.init()) }

                _ = try await engine.run().get()
                await #expect(throws: (any Error).self) { try await engine.run().get() }
            }
        }
    }
}
