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

    /// Value retrieval across peers that disagree.
    ///
    /// A lookup used to cancel on the first record it saw, so whichever peer answered first won —
    /// including a peer holding a record the rest of the network had long since superseded. The
    /// engine now collects every answer, resolves them with the namespace's validator, and pushes
    /// the winner back to the peers that were behind.
    @Suite("Value Lookup Tests", .serialized)
    final class ValueLookupTests {

        private var options: KadDHT.NodeOptions {
            .init(
                connectionTimeout: .milliseconds(500),
                concurrency: 3,
                bucketSize: 5,
                maxPeers: 15,
                maxKeyValueStoreEntries: 10,
                supportLocalNetwork: true
            )
        }

        @Test func picksTheBestRecordAndCorrectsThePeerThatWasBehind() async throws {
            let name = try PeerID(.Ed25519)
            let stale = try IPNSFixture(name: name, sequence: 1)
            let fresh = try IPNSFixture(name: name, sequence: 5)
            let kid = KadDHT.Key(stale.key, keySpace: .xor)

            try await withApp(configure: dhtHost(mode: .server, options: options)) { behind in
                try await withApp(configure: dhtHost(mode: .server, options: options)) { current in
                    try await withApp(
                        configure: dhtHost(
                            mode: .server,
                            options: options,
                            bootstrapPeers: [behind.peerInfo, current.peerInfo]
                        )
                    ) { searcher in
                        /// One holder is on sequence 1, the other on sequence 5.
                        _ = try await behind.dht.kadDHT.dht.updateValue(
                            KadDHT.timeStamped(try Self.record(stale)),
                            forKey: kid
                        ).get()
                        _ = try await current.dht.kadDHT.dht.updateValue(
                            KadDHT.timeStamped(try Self.record(fresh)),
                            forKey: kid
                        ).get()

                        let found = try await searcher.dht.kadDHT.get(stale.key).get()
                        #expect(
                            try Self.sequence(of: found) == 5,
                            "the higher sequence has to win regardless of who answered first"
                        )

                        /// Entry correction: the holder that was behind gets sent the winner.
                        var corrected: UInt64? = nil
                        for _ in 0..<20 where corrected != 5 {
                            try await Task.sleep(for: .milliseconds(25))
                            let held = try await behind.dht.kadDHT.dht.getValue(forKey: kid).get()
                            corrected = try Self.sequence(of: held)
                        }
                        #expect(corrected == 5, "the outdated holder should have been corrected")
                    }
                }
            }
        }

        /// A record that fails its namespace validator is dropped rather than returned, so one
        /// misbehaving holder can't answer for a key it has no valid record for.
        @Test func ignoresRecordsThatFailValidation() async throws {
            let name = try PeerID(.Ed25519)
            /// Signed by someone else entirely — the signature verifies against the wrong key.
            let forged = try IPNSFixture(name: name, signer: try PeerID(.Ed25519), sequence: 9)
            let kid = KadDHT.Key(forged.key, keySpace: .xor)

            try await withApp(configure: dhtHost(mode: .server, options: options)) { holder in
                try await withApp(
                    configure: dhtHost(mode: .server, options: options, bootstrapPeers: [holder.peerInfo])
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
