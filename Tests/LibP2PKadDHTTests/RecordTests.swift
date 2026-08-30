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

import CryptoSwift
import Foundation
import LibP2P
import LibP2PCrypto
import Testing

@testable import LibP2PKadDHT

extension LibP2PKadDHTTests {

    /// Timestamp parsing/formatting for `DHT.Record.timeReceived`.
    ///
    /// go writes this field with `util.FormatRFC3339(t)` = `t.UTC().Format(time.RFC3339Nano)`.
    /// `RFC3339Nano` trims trailing zeros from the fractional-seconds component, so the fraction is
    /// 0–9 digits wide and is omitted entirely on a whole second.
    @Suite("RFC3339Date Tests", .serialized)
    struct RFC3339DateTests {

        @Test(arguments: [
            "2022-09-12T14:46:52.892973042Z",  // 9 — full nanosecond precision
            "2022-09-12T14:46:52.892973Z",  // 6
            "2022-09-12T14:46:52.892Z",  // 3
            "2022-09-12T14:46:52.8Z",  // 1
            "2022-09-12T14:46:52Z",  // 0 — whole second, no '.'
        ])
        func parsesEveryFractionalWidthGoEmits(_ timestamp: String) throws {
            let parsed = try RFC3339Date(string: timestamp)
            /// A parsed timestamp round-trips byte-for-byte, so a record we forward is unchanged.
            #expect(parsed.string == timestamp)
        }

        @Test func truncatesSubNanosecondPrecision() throws {
            let parsed = try RFC3339Date(string: "2022-09-12T14:46:52.892973042123Z")
            #expect(parsed.nanoseconds == 892_973_042)
        }

        @Test(arguments: [
            "2022-09-12T14:46:52.Z",  // bare trailing dot
            "2022-09-12T14:46:52",  // no zone designator — ambiguous instant
            "2022-09-12T14:46:52.abcZ",  // non-numeric fraction
            "2022-09-12T14:46:52.892973042+99:00",  // out-of-range offset
            "not-a-date",
        ])
        func rejectsMalformedStrings(_ timestamp: String) throws {
            #expect(throws: (any Error).self) { try RFC3339Date(string: timestamp) }
        }

        @Test func parsesAsUTCRegardlessOfHostTimeZone() throws {
            let parsed = try RFC3339Date(string: "2022-09-12T14:46:52Z")
            #expect(parsed.date.timeIntervalSince1970 == 1_662_994_012)
        }

        @Test func zoneOffsetsNormalizeToTheSameInstant() throws {
            let utc = try RFC3339Date(string: "2022-09-12T14:46:52Z")
            let minusFive = try RFC3339Date(string: "2022-09-12T09:46:52-05:00")
            let plusOneThirty = try RFC3339Date(string: "2022-09-12T16:16:52+01:30")
            #expect(utc == minusFive)
            #expect(utc == plusOneThirty)
        }

        @Test func ordersAcrossFractionalWidths() throws {
            let whole = try RFC3339Date(string: "2022-09-12T14:46:52Z")
            let tenth = try RFC3339Date(string: "2022-09-12T14:46:52.1Z")
            let fifth = try RFC3339Date(string: "2022-09-12T14:46:52.2Z")
            let nextSecond = try RFC3339Date(string: "2022-09-12T14:46:53Z")

            #expect(whole < tenth)
            #expect(tenth < fifth)
            #expect(fifth < nextSecond)

            /// `.1` and `.100000000` are the same instant written two ways.
            #expect(tenth == (try? RFC3339Date(string: "2022-09-12T14:46:52.100000000Z")))
        }

        /// Nanosecond-resolution ordering must survive; `Date` is a `Double` and can't resolve this
        /// on its own near the present epoch, which is why the type stores nanos separately.
        @Test func ordersAtNanosecondResolution() throws {
            let lower = try RFC3339Date(string: "2022-09-12T14:46:52.892973041Z")
            let middle = try RFC3339Date(string: "2022-09-12T14:46:52.892973042Z")
            let upper = try RFC3339Date(string: "2022-09-12T14:46:52.892973043Z")

            #expect(lower < middle)
            #expect(middle < upper)
            #expect(middle == (try? RFC3339Date(string: "2022-09-12T14:46:52.892973042Z")))
        }

        /// Our encoder must produce something go would produce — and that we can read back.
        @Test func emitsRFC3339NanoShapeAndReparses() throws {
            /// Whole second: no fractional component and no '.' at all.
            let whole = RFC3339Date(date: Date(timeIntervalSince1970: 1_662_993_612))
            #expect(whole.string == "2022-09-12T14:40:12Z")

            /// Half second: trailing zeros trimmed, so ".5" rather than ".500000000".
            let half = RFC3339Date(date: Date(timeIntervalSince1970: 1_662_993_612.5))
            #expect(half.string == "2022-09-12T14:40:12.5Z")

            /// Anything we emit, we can parse.
            for emitted in [whole, half] {
                #expect((try? RFC3339Date(string: emitted.string)) == emitted)
            }
        }

        @Test func pubKeyRecordTimestampIsSelfParseable() throws {
            let rec = try KadDHT.createPubKeyRecord(peerID: PeerID(.Ed25519)).toProtobuf()
            let record = KadDHT.timeStamped(rec)
            #expect(!record.timeReceived.isEmpty)
            #expect(throws: Never.self) { try RFC3339Date(string: record.timeReceived) }
        }
    }

    /// Record selection, and the bounds-safety of the code that consumes a `Validator`'s index.
    @Suite("Record Selection Tests", .serialized)
    struct RecordSelectionTests {

        // MARK: - /pk/

        /// go-libp2p-record's `PublicKeyValidator.Select` is `return 0, nil` — "It always returns 0
        /// as all public keys are equivalently valid." Once `validate` binds the key to the hash of
        /// the public key, every value that validates is the same value.
        @Test func pubKeySelectAlwaysReturnsZero() throws {
            let validator = KadDHT.PubKeyValidator()
            let a = try KadDHT.createPubKeyRecord(peerID: PeerID(.Ed25519)).toProtobuf().serializedData()
                .byteArray
            let b = try KadDHT.createPubKeyRecord(peerID: PeerID(.Ed25519)).toProtobuf().serializedData()
                .byteArray

            #expect(try validator.select(key: "/pk/".bytes, values: [a, b]) == 0)
            #expect(try validator.select(key: "/pk/".bytes, values: [b, a]) == 0)
        }

        @Test func pubKeySelectThrowsOnEmptyInput() throws {
            #expect(throws: (any Error).self) {
                try KadDHT.PubKeyValidator().select(key: "/pk/".bytes, values: [])
            }
        }

        // MARK: - /ipns/

        @Test func ipnsSelectPrefersHigherSequence() throws {
            let older = try ipnsRecord(sequence: 4, validity: "2030-01-01T00:00:00Z")
            let newer = try ipnsRecord(sequence: 7, validity: "2030-01-01T00:00:00Z")

            let validator = KadDHT.IPNSValidator()
            #expect(try validator.select(key: "/ipns/".bytes, values: [older, newer]) == 1)
            #expect(try validator.select(key: "/ipns/".bytes, values: [newer, older]) == 0)
        }

        /// When the sequence number is equal the later EOL wins. This is the real-world case where a
        /// publisher whose republish interval is shorter than its record lifetime re-emits the same
        /// sequence with a nearer EOL, the longer-lived record has to survive.
        @Test func ipnsSelectBreaksSequenceTiesOnLaterValidity() throws {
            let shortLived = try ipnsRecord(sequence: 9, validity: "2027-01-01T00:00:00Z")
            let longLived = try ipnsRecord(sequence: 9, validity: "2027-01-01T00:00:00.1Z")

            let validator = KadDHT.IPNSValidator()
            #expect(try validator.select(key: "/ipns/".bytes, values: [shortLived, longLived]) == 1)
            #expect(try validator.select(key: "/ipns/".bytes, values: [longLived, shortLived]) == 0)
        }

        /// `timeReceived` is our own local timestamp, it should carry no weight in record sorting
        @Test func ipnsSelectIgnoresTimeReceived() throws {
            /// Lower sequence but stamped far in the future; the higher sequence must still win.
            let lowSeqFreshStamp = try ipnsRecord(
                sequence: 1,
                validity: "2030-01-01T00:00:00Z",
                timeReceived: "2035-01-01T00:00:00Z"
            )
            let highSeqOldStamp = try ipnsRecord(
                sequence: 2,
                validity: "2030-01-01T00:00:00Z",
                timeReceived: "2020-01-01T00:00:00Z"
            )

            let validator = KadDHT.IPNSValidator()
            #expect(try validator.select(key: "/ipns/".bytes, values: [lowSeqFreshStamp, highSeqOldStamp]) == 1)
        }

        @Test func ipnsSelectSkipsUnparseableLeadingValue() throws {
            let valid = try ipnsRecord(sequence: 3, validity: "2030-01-01T00:00:00Z")
            let garbage: [UInt8] = [0xff, 0xff, 0xff, 0xff]

            let chosen = try KadDHT.IPNSValidator().select(key: "/ipns/".bytes, values: [garbage, valid])
            #expect(chosen == 1, "must skip the unparseable value rather than returning index 0")
        }

        @Test func ipnsSelectThrowsWhenNothingParses() throws {
            #expect(throws: (any Error).self) {
                try KadDHT.IPNSValidator().select(key: "/ipns/".bytes, values: [[0xff], [0xfe]])
            }
        }

        @Test(arguments: [-1, 2, 99, Int.max])
        func storeSurvivesOutOfRangeSelectIndex(_ rogueIndex: Int) throws {
            let group = MultiThreadedEventLoopGroup(numberOfThreads: 1)
            defer { try! group.syncShutdownGracefully() }
            let loop = group.next()

            let store = EventLoopDictionary(key: KadDHT.Key.self, value: DHT.Record.self, on: loop)
            let kid = KadDHT.Key("/pk/test".bytes, keySpace: .xor)

            let existing = DHT.Record.with {
                $0.key = Data("/pk/test".bytes)
                $0.value = Data("original".utf8)
            }
            let _ = try store.updateValue(existing, forKey: kid).wait()

            let incoming = DHT.Record.with {
                $0.key = Data("/pk/test".bytes)
                $0.value = Data("replacement".utf8)
            }

            let rogue = KadDHT.BaseValidator(
                validationFunction: { _, _ in },
                selectFunction: { _, _ in rogueIndex }
            )

            /// The rogue validator yields an invalid index, ensure we handle it correctly without erroring
            let result = try store.addKeyIfSpaceOrCloser(
                key: kid,
                value: incoming,
                usingValidator: rogue,
                maxStoreSize: 10,
                targetKey: KadDHT.Key("self".bytes, keySpace: .xor)
            ).wait()
            _ = result

            let stored = try store.getValue(forKey: kid).wait()
            #expect(stored != nil)
            #expect(stored == existing)
        }

        // MARK: - Helpers

        /// Builds an unsigned serialized `DHT.Record` wrapping an `IpnsEntry`.
        ///
        /// Only the fields that selection reads are populated.
        private func ipnsRecord(
            sequence: UInt64,
            validity: String,
            timeReceived: String = "2024-01-01T00:00:00Z"
        ) throws -> [UInt8] {
            let entry = IpnsEntry.with {
                $0.value = Data("/ipfs/bafyfoo".utf8)
                $0.sequence = sequence
                $0.validityType = .eol
                $0.validity = Data(validity.utf8)
            }
            let record = DHT.Record.with {
                $0.key = Data("/ipns/".bytes)
                $0.value = (try? entry.serializedData()) ?? Data()
                $0.timeReceived = timeReceived
            }
            return try record.serializedData().byteArray
        }
    }

    @Suite("Provider Interval Tests", .serialized)
    struct ProviderIntervalTests {

        /// Spec: "In the IPFS DHT the Expiration Interval is set to 48 hours" /
        /// "For the IPFS network it is currently set to 22 hours".
        /// go: `DefaultProvideValidity = 48 * time.Hour`, `DefaultReprovideInterval = 22 * time.Hour`.
        @Test func matchesGoProviderLifetimes() async throws {
            let app = try await Application.make(.testing, peerID: .ephemeral(type: .Ed25519))
            app.logger.logLevel = .warning
            app.security.use(.noise)
            app.muxers.use(.yamux)
            app.dht.use(.kadDHT(mode: .client, options: .default, bootstrapPeers: [], autoUpdate: false))

            let node = app.dht.kadDHT
            #expect(node.providerRecordTTL == 48 * 60 * 60)
            #expect(node.providerRecordRepublishInterval == 22 * 60 * 60)

            /// The republish cadence must leave slack before expiry, or a single missed renewal drops
            /// us out of remote stores.
            #expect(node.providerRecordRepublishInterval < node.providerRecordTTL)

            try await app.asyncShutdown()
        }
    }
}
