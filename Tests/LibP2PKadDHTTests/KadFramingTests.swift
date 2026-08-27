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
import Testing

@testable import LibP2PKadDHT

extension LibP2PKadDHTTests {

    /// Proves the `/ipfs/kad/1.0.0` receive path reassembles messages
    /// correctly regardless of how the peer chunks its writes.
    ///
    /// Canonical libp2p frames every kad message as `uvarint(len) + protobuf`.
    /// The route installs `VarintFrameDecoder` and `Query.decode` parses the
    /// already-stripped frame. Previously the route had no frame decoder and
    /// `Query.decode` asserted the whole frame arrived in a single read — which
    /// only held when the peer flushed one message per yamux frame (swift↔swift)
    /// and broke against peers that segment differently (e.g. rust-libp2p).
    ///
    /// These tests feed the decoder adversarially-split input — every byte
    /// boundary, byte-by-byte dribble, and multiple messages in one buffer — and
    /// assert it always emits exactly the original frames, each of which
    /// `Query.decode` round-trips.
    @Suite("KadDHT wire framing")
    struct KadFramingTests {

        /// The on-the-wire bytes a sender produces: `uvarint(len) + protobuf`.
        private func wireBytes(for query: KadDHT.Query) throws -> [UInt8] {
            try query.encode()
        }

        /// Runs `bytes`, delivered as `chunks`, through a `VarintFrameDecoder`
        /// (the exact handler the kad route installs) and returns the emitted,
        /// length-prefix-stripped frames.
        private func framesFrom(chunks: [[UInt8]]) throws -> [[UInt8]] {
            let channel = EmbeddedChannel()
            try channel.pipeline.addHandler(ByteToMessageHandler(VarintFrameDecoder())).wait()
            for chunk in chunks where !chunk.isEmpty {
                var buf = channel.allocator.buffer(capacity: chunk.count)
                buf.writeBytes(chunk)
                _ = try channel.writeInbound(buf)
            }
            var frames: [[UInt8]] = []
            while let frame = try channel.readInbound(as: ByteBuffer.self) {
                frames.append(Array(frame.readableBytesView))
            }
            _ = try channel.finish()
            return frames
        }

        /// A key large enough that its length prefix is a *multi-byte* uvarint
        /// (>= 128), so splitting inside the prefix also exercises the varint
        /// continuation path — not just the prefix/body boundary.
        private var largeKey: [UInt8] { Array(repeating: UInt8(ascii: "k"), count: 300) }

        @Test("a single frame split at every byte boundary yields exactly one decodable Query")
        func splitAtEveryBoundary() throws {
            let key = largeKey
            let wire = try wireBytes(for: .getValue(key: key))
            #expect(wire.count > 129, "key should force a multi-byte length prefix")

            for split in 0...wire.count {
                let frames = try framesFrom(chunks: [Array(wire[0..<split]), Array(wire[split...])])
                #expect(frames.count == 1, "split at offset \(split) should yield exactly one frame")
                guard case .getValue(let decodedKey) = try KadDHT.Query.decode(frames[0]) else {
                    Issue.record("split \(split): expected a getValue query")
                    continue
                }
                #expect(decodedKey == key, "split \(split): key survived reassembly")
            }
        }

        @Test("byte-by-byte dribble reassembles into one frame")
        func byteByByteDribble() throws {
            let key = largeKey
            let wire = try wireBytes(for: .getValue(key: key))
            let frames = try framesFrom(chunks: wire.map { [$0] })
            #expect(frames.count == 1)
            guard case .getValue(let decodedKey) = try KadDHT.Query.decode(frames[0]) else {
                Issue.record("expected a getValue query")
                return
            }
            #expect(decodedKey == key)
        }

        @Test("two concatenated messages in one read yield two frames")
        func twoMessagesInOneBuffer() throws {
            let k1 = Array("first-key".utf8)
            let k2 = largeKey
            let wire = try wireBytes(for: .getValue(key: k1)) + wireBytes(for: .getValue(key: k2))
            let frames = try framesFrom(chunks: [wire])
            #expect(frames.count == 2)
            guard case .getValue(let a) = try KadDHT.Query.decode(frames[0]),
                case .getValue(let b) = try KadDHT.Query.decode(frames[1])
            else {
                Issue.record("expected two getValue queries")
                return
            }
            #expect(a == k1)
            #expect(b == k2)
        }

        @Test("FIND_NODE accepts an arbitrary (non-PeerID) key")
        func findNodeAcceptsArbitraryKey() throws {
            // rust-libp2p bootstraps with raw 32-byte keys that are not
            // multihash-shaped PeerIds. The decoder must accept them.
            let randomKey = (0..<32).map { UInt8($0) }
            let wire = try wireBytes(for: .findNode(key: randomKey))
            let frames = try framesFrom(chunks: [wire])
            #expect(frames.count == 1)
            guard case .findNode(let decodedKey) = try KadDHT.Query.decode(frames[0]) else {
                Issue.record("expected a findNode query")
                return
            }
            #expect(decodedKey == randomKey)
        }
    }

}
