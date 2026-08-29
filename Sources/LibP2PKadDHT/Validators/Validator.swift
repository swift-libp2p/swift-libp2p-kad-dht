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

public protocol Validator: Sendable {
    func validate(key: [UInt8], value: [UInt8]) throws
    func select(key: [UInt8], values: [[UInt8]]) throws -> Int
}

extension Validator {
    public func asChannelHandler() -> ChannelHandler {
        ValidatorChannelHandler(validator: self, logger: Logger(label: "DHT[namespace]"))
    }
}

extension Application.ChildChannelHandlers.Provider {

    /// Loggers installs a set of inbound and outbound logging handlers that simply dump all data flowing through the pipeline out to the console for debugging purposes
    public static func validator(_ validator: Validator) -> Self {
        .init { connection -> [ChannelHandler] in
            [ValidatorChannelHandler(validator: validator, logger: connection.logger)]
        }
    }
}

/// Wraps a Validator in a ChannelHandler so it can be installed in our pipeline and act like middleware
internal final class ValidatorChannelHandler: ChannelInboundHandler {
    public typealias InboundIn = ByteBuffer
    public typealias OutboundOut = ByteBuffer

    public let validator: Validator
    private var logger: Logger

    public init(validator: some Validator, logger: Logger) {
        self.logger = logger
        self.validator = validator
        self.logger[metadataKey: "DHTValidator"] = .string("namespace")
    }

    public func channelActive(context: ChannelHandlerContext) {
        self.logger.trace("DHT[namespace] Validator Installed")
    }

    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {

        let dataToBeValidated = self.unwrapInboundIn(data)

        // TODO: Validate Data...

        context.fireChannelRead(self.wrapOutboundOut(dataToBeValidated))
    }

    public func handlerRemoved(context: ChannelHandlerContext) {
        self.logger.trace("handler removed.")
    }

    public func errorCaught(context: ChannelHandlerContext, error: Error) {
        self.logger.error("\(error)")

        // As we are not really interested getting notified on success or failure
        // we just pass nil as promise to reduce allocations.
        context.close(promise: nil)
    }
}

extension KadDHT {
    struct BaseValidator: Validator {
        typealias ValidateFuntion = @Sendable (_ key: [UInt8], _ value: [UInt8]) throws -> Void
        typealias SelectFunction = @Sendable (_ key: [UInt8], _ values: [[UInt8]]) throws -> Int
        let validateFunction: ValidateFuntion
        let selectFunction: SelectFunction

        init(
            validationFunction: @escaping ValidateFuntion,
            selectFunction: @escaping SelectFunction
        ) {
            self.validateFunction = validationFunction
            self.selectFunction = selectFunction
        }

        func validate(key: [UInt8], value: [UInt8]) throws {
            try self.validateFunction(key, value)
        }

        func select(key: [UInt8], values: [[UInt8]]) throws -> Int {
            try self.selectFunction(key, values)
        }

        struct AllowAll: Validator {
            init() {}

            func validate(key: [UInt8], value: [UInt8]) throws {
                print("🔎 AllowAllValidator::Validating key `\(key.toHexString())`")
            }

            func select(key: [UInt8], values: [[UInt8]]) throws -> Int {
                print("🔎 AllowAllValidator::Selecting key `\(key.toHexString())` from \(values.count) values")
                return 0
            }
        }
    }

    struct PubKeyValidator: Validator {
        func validate(key: [UInt8], value: [UInt8]) throws {
            print("🔎 PubKeyValidator::Validating key `\(key.toHexString())`")
            let record = try DHT.Record(serializedBytes: value)
            guard Data(key) == record.key else {
                throw NSError(domain: "Validator::Key Mismatch. Expected \(Data(key)) got \(record.key) ", code: 0)
            }
            let _ = try PeerID(marshaledPublicKey: Data(record.value))
        }

        /// Every valid value for a `/pk/` key is byte-identical, so there is nothing to choose
        /// between: `validate` binds the key to the hash of the public key it carries, which means
        /// any value that passes validation is a good value.
        ///
        /// - Note: This mirrors go-libp2p-record's `PublicKeyValidator.Select`
        func select(key: [UInt8], values: [[UInt8]]) throws -> Int {
            guard !values.isEmpty else {
                throw NSError(domain: "Validator::No Records to select", code: 0)
            }
            return 0
        }
    }

    struct IPNSValidator: Validator {
        func validate(key: [UInt8], value: [UInt8]) throws {
            print("🔎 IPNSValidator::Validating key `\(key.toHexString())`")
            let record = try DHT.Record(serializedBytes: value)
            guard Data(key) == record.key else {
                throw NSError(domain: "Validator::Key Mismatch. Expected \(Data(key)) got \(record.key) ", code: 0)
            }
            let _ = try IpnsEntry(serializedBytes: record.value)
        }

        /// Picks the best of several IPNS records: **highest sequence number, then latest validity**.
        ///
        /// - Note: The validity tie-break matters in practice: a publisher whose republish interval is
        /// shorter than its record lifetime re-emits the same sequence number with an updated EOL,
        /// so on equal sequence the longer-lived record has to win.
        func select(key: [UInt8], values: [[UInt8]]) throws -> Int {
            let entries = values.map { value -> IpnsEntry? in
                guard let record = try? DHT.Record(serializedBytes: value),
                    let entry = try? IpnsEntry(serializedBytes: record.value)
                else { return nil }
                return entry
            }

            var bestIndex: Int? = nil
            for (index, entry) in entries.enumerated() {
                guard let entry else { continue }
                guard let currentBest = bestIndex, let best = entries[currentBest] else {
                    bestIndex = index
                    continue
                }
                /// compare the current best to the next record
                if Self.isPreferred(entry, over: best) {
                    /// update the best index if it's prefered
                    bestIndex = index
                }
            }

            guard let bestIndex else {
                throw NSError(domain: "Validator::No Records to select", code: 0)
            }
            return bestIndex
        }

        /// `true` when `candidate` should win over `current`.
        private static func isPreferred(_ candidate: IpnsEntry, over current: IpnsEntry) -> Bool {
            /// Highest sequence numbers wins
            guard candidate.sequence == current.sequence else {
                return candidate.sequence > current.sequence
            }

            /// If they have the same sequence number, prefer the later EOL.
            /// If the candidate doesn't have an EOL, we prefer the current Record
            guard let candidateEOL = Self.endOfLife(candidate) else { return false }
            /// If the candidate does have an EOL and the current record doesn't, we prefer the candidate
            guard let currentEOL = Self.endOfLife(current) else { return true }
            /// If they both have EOLs keep the longest living record
            return candidateEOL > currentEOL
        }

        /// Parses the `validity` field as an `RFC3339Date` EOL timestamp, or `nil` otherwise
        private static func endOfLife(_ entry: IpnsEntry) -> RFC3339Date? {
            guard entry.hasValidity, entry.validityType == .eol,
                let string = String(data: entry.validity, encoding: .utf8)
            else { return nil }
            return try? RFC3339Date(string: string)
        }
    }
}
