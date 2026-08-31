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

extension KadDHT {

    /// A uvarint length-prefix frame decoder that refuses oversized frames.
    final class FrameDecoder: ByteToMessageDecoder {
        typealias InboundOut = ByteBuffer

        let maxFrameLength: Int
        private var messageLength: Int? = nil

        init(maxFrameLength: Int = KadDHT.Defaults.maxMessageSize) {
            self.maxFrameLength = maxFrameLength
        }

        func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
            if self.messageLength == nil {
                guard let length = try buffer.readKadVarInt() else { return .needMoreData }
                guard length <= self.maxFrameLength else {
                    throw KadDHT.Errors.messageTooLarge(bytes: length, limit: self.maxFrameLength)
                }
                self.messageLength = length
            }
            guard let length = self.messageLength else { return .needMoreData }

            guard let messageBytes = buffer.readSlice(length: length) else { return .needMoreData }
            self.messageLength = nil

            context.fireChannelRead(self.wrapInboundOut(messageBytes))
            return .continue
        }

        func decodeLast(
            context: ChannelHandlerContext,
            buffer: inout ByteBuffer,
            seenEOF: Bool
        ) throws -> DecodingState {
            try self.decode(context: context, buffer: &buffer)
        }
    }
}

extension Application.ChildChannelHandlers.Provider {

    /// Installs the KadDHT inbound frame decoder, bounded at ``KadDHT/maxMessageSize``.
    static var kadFrameDecoder: Self {
        .init { _ in [ByteToMessageHandler(KadDHT.FrameDecoder())] }
    }
}

extension ByteBuffer {
    /// The most bytes a 64-bit uvarint can occupy.
    private static let maxVarIntBytes = 10

    /// Reads a uvarint, or `nil` when the buffer doesn't hold a complete one yet.
    fileprivate mutating func readKadVarInt() throws -> Int? {
        let peek =
            self.getBytes(
                at: self.readerIndex,
                length: min(Self.maxVarIntBytes, self.readableBytes)
            ) ?? []

        /// A uvarint ends at the first byte with the continuation bit clear.
        guard peek.contains(where: { $0 & 0x80 == 0 }) else {
            /// No terminator within 10 bytes can never become valid; anything shorter just hasn't
            /// fully arrived yet.
            guard peek.count < Self.maxVarIntBytes else { throw KadDHT.Errors.DecodingErrorInvalidLength }
            return nil
        }

        /// With a terminator present, a non-positive `bytesRead` means overflow or a non-minimal
        /// encoding rather than a short buffer.
        let (value, bytesRead) = uVarInt(peek)
        guard bytesRead > 0, value <= UInt64(Int.max) else {
            throw KadDHT.Errors.DecodingErrorInvalidLength
        }

        self.moveReaderIndex(forwardBy: bytesRead)
        return Int(value)
    }
}
