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

public protocol Validator {
    func validate(key: [UInt8], value: [UInt8]) throws
    func select(key: [UInt8], values: [[UInt8]]) throws -> Int
}

public extension Validator {
    func asChannelHandler() -> ChannelHandler {
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
        let validateFunction: (_ key: [UInt8], _ value: [UInt8]) throws -> Void
        let selectFunction: (_ key: [UInt8], _ values: [[UInt8]]) throws -> Int

        init(validationFunction: @escaping (_ key: [UInt8], _ value: [UInt8]) throws -> Void, selectFunction: @escaping (_ key: [UInt8], _ values: [[UInt8]]) throws -> Int) {
            self.validateFunction = validationFunction
            self.selectFunction = selectFunction
        }

        func validate(key: [UInt8], value: [UInt8]) throws {
            return try self.validateFunction(key, value)
        }

        func select(key: [UInt8], values: [[UInt8]]) throws -> Int {
            return try self.selectFunction(key, values)
        }

        struct AllowAll: Validator {
            init() { }

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
            let record = try DHT.Record(contiguousBytes: value)
            guard Data(key) == record.key else { throw NSError(domain: "Validator::Key Mismatch. Expected \(Data(key)) got \(record.key) ", code: 0) }
            let _ = try PeerID(marshaledPublicKey: Data(record.value))
        }

        func select(key: [UInt8], values: [[UInt8]]) throws -> Int {
            print("🔎 PubKeyValidator::Selecting key `\(key.toHexString())` from \(values.count) values")
            let records = values.map { try? DHT.Record(contiguousBytes: $0) }
            guard !records.compactMap({ $0 }).isEmpty else { throw NSError(domain: "Validator::No Records to select", code: 0) }
            guard records.count > 1 && records[0] != nil else { return 0 }

            var bestValueIndex: Int = 0
            var bestValue: DHT.Record?
            for (index, record) in records.enumerated() {
                guard let record = record else { continue }
                guard let newRecord = try? RFC3339Date(string: record.timeReceived) else { continue }

                if let currentBest = bestValue {
                    guard let currentRecord = try? RFC3339Date(string: currentBest.timeReceived) else { continue }
                    // If this record is more recent then our currentBest, update our current best!
                    if newRecord > currentRecord {
                        bestValue = record
                        bestValueIndex = index
                    }
                } else {
                    // If we don't have a current best, set it!
                    bestValue = record
                    bestValueIndex = index
                }
            }

            guard bestValue != nil else { throw NSError(domain: "Validator::Failed to select a valid record", code: 0) }
            return bestValueIndex
        }
    }

    struct IPNSValidator: Validator {
        func validate(key: [UInt8], value: [UInt8]) throws {
            print("🔎 IPNSValidator::Validating key `\(key.toHexString())`")
            let record = try DHT.Record(contiguousBytes: value)
            guard Data(key) == record.key else { throw NSError(domain: "Validator::Key Mismatch. Expected \(Data(key)) got \(record.key) ", code: 0) }
            let _ = try IpnsEntry(contiguousBytes: record.value)
        }

        func select(key: [UInt8], values: [[UInt8]]) throws -> Int {
            print("🔎 IPNSValidator::Selecting key `\(key.toHexString())` from \(values.count) values")
            let records = values.map { try? DHT.Record(contiguousBytes: $0) }
            guard !records.compactMap({ $0 }).isEmpty else { throw NSError(domain: "Validator::No Records to select", code: 0) }
            guard records.count > 1 && records[0] != nil else { return 0 }

            var bestValueIndex: Int = 0
            var bestValue: DHT.Record?
            for (index, record) in records.enumerated() {
                guard let record = record else { continue }
                guard let newRecord = try? RFC3339Date(string: record.timeReceived) else { continue }

                if let currentBest = bestValue {
                    guard let currentRecord = try? RFC3339Date(string: currentBest.timeReceived) else { continue }
                    // If this record is more recent then our currentBest, update our current best!
                    if newRecord > currentRecord {
                        bestValue = record
                        bestValueIndex = index
                    }
                } else {
                    // If we don't have a current best, set it!
                    bestValue = record
                    bestValueIndex = index
                }
            }

            guard bestValue != nil else { throw NSError(domain: "Validator::Failed to select a valid record", code: 0) }
            return bestValueIndex
        }
    }
}
