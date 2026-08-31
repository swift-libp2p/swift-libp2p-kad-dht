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
import NIOCore
import NIOEmbedded
import NIOTestUtils
import Testing

@testable import LibP2PKadDHT

extension LibP2PKadDHTTests {

    /// Proves the `/ipfs/kad/1.0.0` receive path reassembles messages
    /// correctly regardless of how the peer chunks its writes.
    @Suite("KadDHT wire framing")
    struct KadFramingTests {

        /// A key large enough that its length prefix is a multi-byte uvarint
        /// (>= 128), so splitting inside the prefix also exercises the varint
        /// continuation path, not just the prefix/body boundary.
        private var largeKey: [UInt8] { Array(repeating: UInt8(ascii: "k"), count: 300) }

        @Test("the decoder emits exactly one frame per message, however the peer chunks its writes")
        func passesTheDecoderVerifier() throws {
            let small = try wireBytes(for: .getValue(key: Array("first-key".utf8)))
            let large = try wireBytes(for: .getValue(key: largeKey))
            #expect(large.count > 129, "the large key should force a multi-byte length prefix")

            /// Pairs of (inbound bytes, and the frames they must produce).
            let pairs: [(ByteBuffer, [ByteBuffer])] = [
                (buffer(small), [expectedFrame(small)]),
                (buffer(large), [expectedFrame(large)]),
                /// Two messages arriving together still come out as two frames.
                (buffer(small + large), [expectedFrame(small), expectedFrame(large)]),
            ]

            #expect(throws: Never.self) {
                try ByteToMessageDecoderVerifier.verifyDecoder(
                    inputOutputPairs: pairs,
                    decoderFactory: { KadDHT.FrameDecoder() }
                )
            }
        }

        /// The frames the decoder emits have to be a bare protobuf, no length prefix.
        @Test("an emitted frame round-trips through Query.decode")
        func emittedFramesDecode() throws {
            let key = largeKey
            let frame = expectedFrame(try wireBytes(for: .getValue(key: key)))

            let channel = try decoderChannel()
            _ = try channel.writeInbound(buffer(try wireBytes(for: .getValue(key: key))))
            let emitted = try #require(try channel.readInbound(as: ByteBuffer.self))
            _ = try channel.finish()

            #expect(emitted == frame)
            guard case .getValue(let decodedKey) = try KadDHT.Query.decode(Array(emitted.readableBytesView)) else {
                Issue.record("expected a getValue query")
                return
            }
            #expect(decodedKey == key)
        }

        /// A length prefix is remote input. Without a ceiling a peer can announce a huge frame and
        /// we'll buffer toward it, so an oversized prefix has to error the channel immediately.
        @Test("an oversized length prefix is rejected")
        func rejectsAnOversizedLengthPrefix() throws {
            let channel = try decoderChannel()
            let announced = buffer(putUVarInt(UInt64(KadDHT.Defaults.maxMessageSize + 1)) + [0x08])

            #expect(throws: (any Error).self) { try channel.writeInbound(announced) }
            _ = try? channel.finish()
        }

        /// The largest frame we do accept still decodes.
        @Test("a frame at the size limit still decodes")
        func acceptsAFrameAtTheLimit() throws {
            let key = Array(repeating: UInt8(ascii: "k"), count: 4096)
            let wire = try wireBytes(for: .getValue(key: key))
            #expect(wire.count <= KadDHT.Defaults.maxMessageSize)

            let channel = try decoderChannel()
            _ = try channel.writeInbound(buffer(wire))
            #expect(try channel.readInbound(as: ByteBuffer.self) == expectedFrame(wire))
            _ = try channel.finish()
        }

        @Test("FIND_NODE accepts an arbitrary (non-PeerID) key")
        func findNodeAcceptsArbitraryKey() throws {
            // rust-libp2p bootstraps with raw 32-byte keys that are not
            // multihash-shaped PeerIds. The decoder should accept them.
            let randomKey = (0..<32).map { UInt8($0) }

            guard case .findNode(let decodedKey) = try decodeQueryFrame(try wireBytes(for: .findNode(key: randomKey)))
            else {
                Issue.record("expected a findNode query")
                return
            }
            #expect(decodedKey == randomKey)
        }

        // - MARK: Helpers

        /// The on-the-wire bytes a sender produces: `uvarint(len) + protobuf`.
        private func wireBytes(for query: KadDHT.Query) throws -> [UInt8] {
            try query.encode()
        }

        /// A channel holding just the decoder the kad route installs.
        private func decoderChannel() throws -> EmbeddedChannel {
            let channel = EmbeddedChannel()
            try channel.pipeline.addHandler(ByteToMessageHandler(KadDHT.FrameDecoder())).wait()
            return channel
        }

        private func buffer(_ bytes: [UInt8]) -> ByteBuffer {
            ByteBuffer(bytes: bytes)
        }

        /// The frame a sender's wire bytes should decode to: the payload, minus the length prefix.
        private func expectedFrame(_ wire: [UInt8]) -> ByteBuffer {
            self.buffer(Array(wire.dropFirst(uVarInt(wire).bytesRead)))
        }
    }

}
