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
    /// Regression coverage for the value-store path
    /// (`storeNew` + `getUsingLookupList`).
    ///
    /// Before this fix, `storeNew` returned `false` whenever the
    /// publisher's routing table was empty at call time — for
    /// example, immediately after `Application.start()` while the
    /// asynchronous bootstrap-peer addition was still in flight on
    /// the event loop. This broke any caller that wanted to publish
    /// a value soon after node startup.
    ///
    /// The fix shifts `storeNew` to local-first semantics — the
    /// value lands in the publisher's own kv store synchronously,
    /// remote acceptance is best-effort, and the return value
    /// reflects local success. Heartbeat's `_shareDHTKVs` continues
    /// pushing the value out on the standard cadence.
    @Suite("Value Store Tests", .serialized)
    final class ValueStoreTests {

        @Test func testStoreNewWithEmptyRoutingTableReturnsTrue() async throws {
            let dhtParams = KadDHT.NodeOptions(
                connectionTimeout: .milliseconds(150),
                concurrency: 3,
                bucketSize: 5,
                maxPeers: 15,
                maxKeyValueStoreEntries: 10,
                supportLocalNetwork: true
            )

            try await withApp(configure: dhtHost(mode: .server, options: dhtParams)) { node in
                let key = try syntheticCID("local-first-empty-rt")
                let record = TestRecord(
                    key: Data(key),
                    value: Data("hello-local-first".utf8)
                )
                let accepted = try await node.dht.kadDHT.storeNew(key, value: record).get()
                #expect(accepted, "storeNew should accept even with empty routing table")

                // Publisher's own get must succeed without any remote
                // hop — local-first means the value is sitting in our
                // own kv store.
                let fetched = try await node.dht.kadDHT.getUsingLookupList(key).get()
                #expect(fetched != nil, "publisher's own getUsingLookupList should hit the local kv")
                #expect(fetched?.value == Data("hello-local-first".utf8))
            }
        }

        /// The receiving side of a PUT: namespace lookup, validation against the record's *value*,
        /// then storage. `/pk/` is the namespace both nodes validate out of the box.
        @Test func testCrossNodePutValueIsValidatedAndStored() async throws {
            let dhtParams = KadDHT.NodeOptions(
                connectionTimeout: .milliseconds(500),
                concurrency: 3,
                bucketSize: 5,
                maxPeers: 15,
                maxKeyValueStoreEntries: 10,
                supportLocalNetwork: true
            )

            try await withApp(configure: dhtHost(mode: .server, options: dhtParams)) { receiver in
                try await withApp(
                    configure: dhtHost(mode: .server, options: dhtParams, bootstrapPeers: [receiver.peerInfo])
                ) { publisher in
                    let record = try KadDHT.createPubKeyRecord(peerID: publisher.peerID).toProtobuf()
                    let stored = try await publisher.dht.kadDHT.storeNew(record.key.byteArray, value: record).get()
                    #expect(stored)

                    /// The PUT is best-effort and asynchronous, so poll rather than sleep.
                    let kid = KadDHT.Key(record.key.byteArray, keySpace: .xor)
                    var accepted: DHT.Record? = nil
                    for _ in 0..<20 where accepted == nil {
                        try await Task.sleep(for: .milliseconds(25))
                        accepted = try await receiver.dht.kadDHT.dht.getValue(forKey: kid).get()
                    }

                    #expect(accepted != nil, "receiver should have validated and stored the /pk/ record")
                    #expect(accepted?.value == record.value)
                    /// Stamped on arrival rather than carried from the sender.
                    #expect(accepted?.timeReceived.isEmpty == false)
                }
            }
        }

        @Test func testStoreNewCrossNodeRoundTrip() async throws {
            let dhtParams = KadDHT.NodeOptions(
                connectionTimeout: .milliseconds(500),
                concurrency: 3,
                bucketSize: 5,
                maxPeers: 15,
                maxKeyValueStoreEntries: 10,
                supportLocalNetwork: true
            )

            try await withApp(configure: dhtHost(mode: .server, options: dhtParams)) { publisher in
                try await withApp(
                    configure: dhtHost(mode: .server, options: dhtParams, bootstrapPeers: [publisher.peerInfo])
                ) { consumer in
                    let key = try syntheticCID("local-first-cross-node")
                    let record = TestRecord(
                        key: Data(key),
                        value: Data("hello-cross-node".utf8)
                    )
                    let stored = try await publisher.dht.kadDHT.storeNew(key, value: record).get()
                    #expect(stored, "storeNew should accept (local-first semantics)")

                    let fetched = try await consumer.dht.kadDHT.getUsingLookupList(key).get()
                    #expect(fetched != nil, "consumer should fetch publisher's value via iterative lookup")
                    #expect(fetched?.value == Data("hello-cross-node".utf8))
                }
            }
        }
    }

    private struct TestRecord: DHTRecord {
        let key: Data
        let value: Data
        let author: Data = Data()
        let signature: Data = Data()
        let timeReceived: String = ""
    }

}

extension LibP2PKadDHTTests {
    static func syntheticCID(_ tag: String) throws -> [UInt8] {
        try CID(
            version: .v1,
            codec: .raw,
            multihash: try Multihash(raw: tag.bytes, hashedWith: .sha2_256)
        ).rawBuffer
    }

    /// Helper method for configuring a DHT Node for the above tests
    static func dhtHost(
        mode: KadDHT.Mode = .client,
        options: KadDHT.NodeOptions = .default,
        bootstrapPeers: [PeerInfo] = [],
        logLevel: Logger.Level = .warning
    ) -> ((Application) async throws -> Void) {
        { app in
            app.logger.logLevel = logLevel
            app.security.use(.noise)
            app.muxers.use(.yamux)
            app.dht.use(.kadDHT(mode: mode, options: options, bootstrapPeers: bootstrapPeers, autoUpdate: false))
            app.servers.use(.tcp(host: "127.0.0.1", port: 0))
        }
    }
}
