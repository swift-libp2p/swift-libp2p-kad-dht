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

    @Suite("Value Answer Tests")
    struct ValueAnswerTests {

        /// Records ordered by an integer, standing in for an IPNS sequence number.
        private func record(_ rank: UInt8) -> DHT.Record {
            DHT.Record.with {
                $0.key = Data("/test/key".bytes)
                $0.value = Data([rank])
            }
        }

        private func prefers(_ candidate: DHT.Record, over current: DHT.Record) -> Bool {
            (candidate.value.first ?? 0) > (current.value.first ?? 0)
        }

        @Test(arguments: [[1, 5], [5, 1]] as [[UInt8]])
        func correctsThePeerThatWasBehindWhicheverOrderAnswersArriveIn(_ ranks: [UInt8]) throws {
            let peers = try (0..<2).map { _ in try generateRandomPeerInfo() }
            var answers = KadDHT.Node.ValueAnswers()

            for (peer, rank) in zip(peers, ranks) {
                answers.add(record(rank), from: peer, prefers: prefers)
            }

            let behind = peers[ranks.firstIndex(of: 1)!]
            let current = peers[ranks.firstIndex(of: 5)!]

            #expect(answers.best?.value == Data([5]))
            #expect(answers.holders.map { $0.peer } == [current.peer])
            #expect(answers.outdated.map { $0.peer } == [behind.peer], "the stale holder is corrected either way")
            #expect(answers.count == 2)
        }

        /// Peers holding the same value are all up to date, so none of them get a correcting PUT.
        @Test func leavesPeersHoldingTheSameValueAlone() throws {
            let peers = try (0..<3).map { _ in try generateRandomPeerInfo() }
            var answers = KadDHT.Node.ValueAnswers()

            for peer in peers { answers.add(record(7), from: peer, prefers: prefers) }

            #expect(answers.holders.count == 3)
            #expect(answers.outdated.isEmpty)
        }
    }

    /// Value retrieval across peers that disagree.
    ///
    /// A lookup used to cancel on the first record it saw, so whichever peer answered first won,
    /// including a peer holding a record the rest of the network had long since superseded. The
    /// engine now collects every answer, resolves them with the namespace's validator, and pushes
    /// the winning record back to the peers that were behind.
    @Suite("Value Lookup Tests", .serialized)
    final class ValueLookupTests {

        private var configuration: KadDHT.Configuration {
            .init(
                bucketSize: 5,
                concurrency: 3,
                connectionTimeout: .milliseconds(500),
                supportLocalNetwork: true
            )
        }

        @Test func picksTheBestRecordAndCorrectsThePeerThatWasBehind() async throws {
            let name = try PeerID(.Ed25519)
            let stale = try IPNSFixture(name: name, sequence: 1)
            let fresh = try IPNSFixture(name: name, sequence: 5)
            let kid = KadDHT.Key(stale.key, keySpace: .xor)

            try await withApp(configure: dhtHost(mode: .server, configuration: configuration)) { first in
                try await withApp(configure: dhtHost(mode: .server, configuration: configuration)) { second in
                    try await withApp(
                        configure: dhtHost(
                            /// One query in flight, so "closest first" is also "first to answer".
                            mode: .server,
                            configuration: Self.searcherConfiguration,
                            bootstrapPeers: [first.peerInfo, second.peerInfo]
                        )
                    ) { searcher in
                        /// A lookup walks its candidates closest-first, so the closer of the two
                        /// holders is the one that answers first. Give that one the stale record.
                        let holders = [first, second].sorted { lhs, rhs in
                            kid.compareDistancesFromSelf(
                                to: KadDHT.Key(lhs.peerID, keySpace: .xor),
                                and: KadDHT.Key(rhs.peerID, keySpace: .xor)
                            ) == .firstKey
                        }
                        let behind = holders[0].dht.kadDHT
                        let current = holders[1].dht.kadDHT

                        _ = try await behind.dht.updateValue(
                            KadDHT.timeStamped(try Self.record(stale)),
                            forKey: kid
                        ).get()
                        _ = try await current.dht.updateValue(
                            KadDHT.timeStamped(try Self.record(fresh)),
                            forKey: kid
                        ).get()

                        /// Bootstrap peers are added asynchronously, so make the precondition
                        /// explicit rather than silently querying whoever made it into the table.
                        try await Self.waitForRoutingTable(of: searcher.dht.kadDHT, toReach: 2)

                        let found = try await searcher.dht.kadDHT.get(stale.key).get()
                        #expect(
                            try Self.sequence(of: found) == 5,
                            "the higher sequence has to win regardless of who answered first"
                        )

                        /// Entry correction: the holder that was behind gets sent the winner. The
                        /// correcting PUT completes before `get` resolves, so there's nothing to
                        /// wait for here.
                        let held = try await behind.dht.getValue(forKey: kid).get()
                        #expect(try Self.sequence(of: held) == 5, "the outdated holder should have been corrected")

                        /// And the holder that was already current is left alone.
                        #expect(try Self.sequence(of: try await current.dht.getValue(forKey: kid).get()) == 5)
                    }
                }
            }
        }

        /// A record that fails its namespace validator is dropped rather than returned, so one
        /// misbehaving holder can't answer for a key it has no valid record for.
        @Test func ignoresRecordsThatFailValidation() async throws {
            let name = try PeerID(.Ed25519)
            /// Signed by someone else entirely, the signature verifies against the wrong key.
            let forged = try IPNSFixture(name: name, signer: try PeerID(.Ed25519), sequence: 9)
            let kid = KadDHT.Key(forged.key, keySpace: .xor)

            try await withApp(configure: dhtHost(mode: .server, configuration: configuration)) { holder in
                try await withApp(
                    configure: dhtHost(mode: .server, configuration: configuration, bootstrapPeers: [holder.peerInfo])
                ) { searcher in
                    _ = try await holder.dht.kadDHT.dht.updateValue(
                        KadDHT.timeStamped(try Self.record(forged)),
                        forKey: kid
                    ).get()

                    let found = try await searcher.dht.kadDHT.get(forged.key).get()
                    #expect(found == nil, "an invalid record is not an answer")
                }
            }
        }

        // MARK: - Helpers

        /// Searcher settings: one query in flight, so responses arrive in closest-first order.
        private static let searcherConfiguration = KadDHT.Configuration(
            bucketSize: 5,
            concurrency: 1,
            connectionTimeout: .milliseconds(500),
            supportLocalNetwork: true
        )

        /// Waits for the asynchronous bootstrap-peer additions to land.
        private static func waitForRoutingTable(
            of node: KadDHT.Node,
            toReach count: Int,
            within attempts: Int = 40
        ) async throws {
            for _ in 0..<attempts {
                if try await node.routingTable.getPeerInfos().get().count >= count { return }
                try await Task.sleep(for: .milliseconds(25))
            }
            Issue.record("routing table never reached \(count) peers")
        }

        /// The `DHT.Record` a fixture would be stored as.
        private static func record(_ fixture: IPNSFixture) throws -> DHT.Record {
            try DHT.Record(serializedBytes: fixture.record)
        }

        /// The IPNS sequence number carried by a record, if it holds one.
        private static func sequence(of record: DHTRecord?) throws -> UInt64? {
            guard let record else { return nil }
            return try IpnsEntry(serializedBytes: record.value).sequence
        }
    }
}
