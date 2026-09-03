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
import Testing

@testable import LibP2PKadDHT

extension LibP2PKadDHTTests {

    /// Wire coverage for the regenerated protobufs (proto3 with explicit presence, embedded `Record`).
    /// - Note: We're using `proto3` instead of the spec defined `proto2` for the closed vs. open enum
    /// case semantics. As we understand it, `proto3` should be backwards compatible with `proto2`.
    @Suite("Protobuf Wire Tests")
    struct ProtobufWireTests {

        // MARK: - Fixtures

        private static let key = Array("/pk/golden".utf8)
        private static let value = Array("golden-value".utf8)
        private static let timeReceived = "2026-09-02T00:00:00.000000000Z"

        private static var goldenRecord: [UInt8] {
            self.record(key: self.key, value: self.value, timeReceived: self.timeReceived)
        }

        private static var protobufRecord: DHT.Record {
            DHT.Record.with {
                $0.key = Data(self.key)
                $0.value = Data(self.value)
                $0.timeReceived = self.timeReceived
            }
        }

        // MARK: - Record

        /// The record on its own, so a field-number change shows up here rather than as a confusing
        /// failure inside one of the message tests.
        @Test func recordEncodesToTheGoldenBytes() throws {
            #expect(try [UInt8](Self.protobufRecord.serializedData()) == Self.goldenRecord)
        }

        /// `PUT_VALUE` is the message the schema change actually touched: field 3 used to hold these
        /// same bytes as an opaque `bytes` payload.
        @Test func putValueEncodesToTheGoldenBytes() throws {
            let expected = Self.framed(
                Self.varint(1, 0) /// type = PUT_VALUE
                    + Self.delimited(2, Self.key)
                    + Self.delimited(3, Self.goldenRecord)
            )

            let encoded = try KadDHT.Query.putValue(key: Self.key, record: Self.protobufRecord).encode()
            #expect(encoded == expected)
        }

        /// The response side embeds the same record, under the same field number.
        @Test func getValueResponseEncodesToTheGoldenBytes() throws {
            let expected = Self.framed(
                Self.varint(1, 1) /// type = `GET_VALUE`
                    + Self.delimited(2, Self.key)
                    + Self.delimited(3, Self.goldenRecord)
            )

            let encoded = try KadDHT.Response.getValue(
                key: Self.key,
                record: Self.protobufRecord,
                closerPeers: []
            ).encode()
            #expect(encoded == expected)
        }

        /// Reading a message assembled the way the old encoder wrote it, record bytes dropped
        /// into field 3, has to yield the record back.
        @Test func decodesAGoldenPutValue() throws {
            let payload =
                Self.varint(1, 0)
                + Self.delimited(2, Self.key)
                + Self.delimited(3, Self.goldenRecord)

            guard case .putValue(let decodedKey, let decoded) = try KadDHT.Query.decode(payload) else {
                Issue.record("expected a putValue query")
                return
            }

            #expect(decodedKey == Self.key)
            #expect(decoded.key == Data(Self.key))
            #expect(decoded.value == Data(Self.value))
            #expect(decoded.timeReceived == Self.timeReceived)
        }

        // MARK: - Fields the schema dropped

        /// `author` (3) and `signature` (4) came off `Record`. A peer still sending them should still parse.
        @Test func toleratesTheRemovedAuthorAndSignatureFields() throws {
            let legacyRecord =
                Self.delimited(1, Self.key)
                + Self.delimited(2, Self.value)
                + Self.delimited(3, Array("author".utf8)) /// removed
                + Self.delimited(4, Array("signature".utf8)) /// removed
                + Self.delimited(5, Array(Self.timeReceived.utf8))

            let payload =
                Self.varint(1, 0)
                + Self.delimited(2, Self.key)
                + Self.delimited(3, legacyRecord)

            guard case .putValue(_, let decoded) = try KadDHT.Query.decode(payload) else {
                Issue.record("expected a putValue query")
                return
            }

            #expect(decoded.key == Data(Self.key))
            #expect(decoded.value == Data(Self.value))
            #expect(decoded.timeReceived == Self.timeReceived)

            /// `DHTRecord` still declares both; we report empty rather than carrying them.
            #expect(decoded.author.isEmpty)
            #expect(decoded.signature.isEmpty)
        }

        // MARK: - Message type

        /// proto3 enums are open, so a type we don't implement arrives as `.UNRECOGNIZED` rather
        /// than collapsing into a known case, and both decoders refuse it.
        @Test func rejectsAnUnknownMessageType() throws {
            let unknownType = Self.varint(1, 42) + Self.delimited(2, Self.key)

            let parsed = try DHT.Message(serializedBytes: unknownType)
            #expect(parsed.type == .UNRECOGNIZED(42), "the value survives parsing, it just isn't a case we serve")

            #expect(throws: (any Error).self) { try KadDHT.Query.decode(unknownType) }
            #expect(throws: (any Error).self) { try KadDHT.Response.decode(Self.framed(unknownType)) }
        }

        /// An absent type is not the same thing: `PUT_VALUE` is numbered 0, and a sender whose
        /// schema gives `type` implicit presence (proto2 spec) elides it.
        @Test func readsATypelessMessageAsPutValue() throws {
            let goShaped =
                Self.delimited(2, Self.key)
                + Self.delimited(3, Self.goldenRecord)

            guard case .putValue(let key, let record) = try KadDHT.Query.decode(goShaped) else {
                Issue.record("a message with no type is a PUT_VALUE")
                return
            }
            #expect(key == Self.key)
            #expect(record.value == Data(Self.value))

            /// The echo (proto2) sends back for a `PUT_VALUE` is the request, so it's typeless too.
            guard case .putValue(let echoedKey, let echoed) = try KadDHT.Response.decode(Self.framed(goShaped))
            else {
                Issue.record("a typeless response is a PUT_VALUE echo")
                return
            }
            #expect(echoedKey == Self.key)
            #expect(echoed?.value == Data(Self.value))
        }

        /// A `PUT_VALUE` with no record at all is a malformed store request, not an empty one.
        @Test func rejectsAPutValueWithNoRecord() throws {
            let noRecord = Self.varint(1, 0) + Self.delimited(2, Self.key)

            #expect(throws: (any Error).self) { try KadDHT.Query.decode(noRecord) }
        }

        // MARK: - IPNS

        /// `IpnsEntry`'s fields became `optional`, which is explicit presence: a zero `sequence` is
        /// now written rather than elided. Nothing signs the protobuf encoding (V2 signs the
        /// DAG-CBOR `data` field), but the presence flags have to behave.
        @Test func ipnsEntryTracksFieldPresence() throws {
            var entry = IpnsEntry()
            #expect(!entry.hasSequence)
            #expect(!entry.hasSignatureV2)
            #expect(!entry.hasData)

            entry.sequence = 0
            #expect(entry.hasSequence, "explicit presence: an assigned zero is still set")

            let roundTripped = try IpnsEntry(serializedBytes: try entry.serializedData())
            #expect(roundTripped.hasSequence)
            #expect(roundTripped.sequence == 0)
        }

        // MARK: - Hand-assembled protobuf

        /// A length-delimited field: `(fieldNumber << 3) | 2`, a uvarint length, then the payload.
        private static func delimited(_ field: UInt8, _ payload: [UInt8]) -> [UInt8] {
            [field << 3 | 2] + putUVarInt(UInt64(payload.count)) + payload
        }

        /// A varint field: `(fieldNumber << 3) | 0`, then the value.
        private static func varint(_ field: UInt8, _ value: UInt64) -> [UInt8] {
            [field << 3 | 0] + putUVarInt(value)
        }

        /// `Record { key = 1, value = 2, timeReceived = 5 }`, in field order.
        private static func record(key: [UInt8], value: [UInt8], timeReceived: String) -> [UInt8] {
            delimited(1, key) + delimited(2, value) + delimited(5, Array(timeReceived.utf8))
        }

        /// The uvarint-length-prefixed frame both `Query.encode` and `Response.encode` emit.
        private static func framed(_ payload: [UInt8]) -> [UInt8] {
            putUVarInt(UInt64(payload.count)) + payload
        }
    }
}
