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

/// Validates and orders the records stored under one DHT namespace.
///
/// Both methods take the record's `value` bytes
/// - Note: `Query.decode` has already checked that the message key and the record key agree,
///   and `timeReceived` is our own local stamp, which is why neither is a validator input.
public protocol Validator: Sendable {
    /// Whether `value` is an acceptable record value for `key`.
    ///
    /// - Parameters:
    ///   - key: The full DHT key, including its `/<namespace>/` prefix.
    ///   - value: The record's `value` bytes.
    /// - Throws: If the value is invalid for this key.
    func validate(key: [UInt8], value: [UInt8]) throws

    /// The index of the best value in `values`.
    ///
    /// - Parameters:
    ///   - key: The full DHT key, including its `/<namespace>/` prefix.
    ///   - values: Candidate record `value` bytes.
    /// - Returns: An index into `values`.
    /// - Throws: If none of the candidates can be ordered.
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

        /// Accepts anything and always picks the first value.
        struct AllowAll: Validator {
            init() {}

            func validate(key: [UInt8], value: [UInt8]) throws {}

            func select(key: [UInt8], values: [[UInt8]]) throws -> Int { 0 }
        }
    }

    struct PubKeyValidator: Validator {
        /// A `/pk/` value is valid only for the key whose multihash is the hash of the public key it
        /// carries.
        func validate(key: [UInt8], value: [UInt8]) throws {
            let record = try DHT.Record(serializedBytes: value)
            guard Data(key) == record.key else {
                throw KadDHT.ValidationError.keyMismatch(expected: key, found: record.key.byteArray)
            }

            let peerIDBytes = try KadDHT.namespacedKeyBody(key, expecting: "pk")
            // init a PeerID from the raw mh bytes
            guard let claimed = try? PeerID(fromBytesID: peerIDBytes) else {
                throw KadDHT.ValidationError.keyIsNotAMultihash
            }
            // init a PeerID from the marshaled body
            let carried = try PeerID(marshaledPublicKey: Data(record.value))
            // make sure they match
            guard carried == claimed else {
                throw KadDHT.ValidationError.publicKeyDoesNotMatchKey(
                    key: claimed.b58String,
                    publicKey: carried.b58String
                )
            }
        }

        /// Every valid value for a `/pk/` key is byte-identical, so there is nothing to choose
        /// between: `validate` binds the key to the hash of the public key it carries, which means
        /// any value that passes validation is a good value.
        ///
        /// - Note: This mirrors go-libp2p-record's `PublicKeyValidator.Select`
        func select(key: [UInt8], values: [[UInt8]]) throws -> Int {
            guard !values.isEmpty else {
                throw KadDHT.ValidationError.noRecordsToSelect
            }
            return 0
        }
    }

    struct IPNSValidator: Validator {
        /// Spec: "IPNS implementations _MUST_ support sending and receiving a serialized `IpnsEntry`
        /// less than or equal to 10 KiB in size."
        static let maxRecordSize: Int = 10 * 1024

        /// The `signatureV2` payload is this prefix followed by the raw CBOR `data` bytes.
        ///
        /// Spec: "Create bytes for signing by concatenating `ipns-signature:` prefix (bytes in hex:
        /// `69706e732d7369676e61747572653a`) with raw CBOR bytes from `IpnsEntry.data`".
        static let signaturePrefix: [UInt8] = Array("ipns-signature:".utf8)

        func validate(key: [UInt8], value: [UInt8]) throws {
            let record = try DHT.Record(serializedBytes: value)
            guard Data(key) == record.key else {
                throw KadDHT.ValidationError.keyMismatch(expected: key, found: record.key.byteArray)
            }
            /// The IPNS Name is the multihash that follows `/ipns/` in the routing key.
            let name = try KadDHT.namespacedKeyBody(key, expecting: "ipns")
            try Self.verify(serializedEntry: record.value, forName: name)
        }

        /// Runs the spec's ordered record-verification steps over a serialized `IpnsEntry`.
        ///
        /// - Parameters:
        ///   - serializedEntry: The `IpnsEntry` protobuf, i.e. a `DHT.Record`'s `value`.
        ///   - name: The IPNS Name — the multihash following `/ipns/` in the routing key.
        ///   - now: The instant expiry is measured against. Injectable for tests.
        /// - Returns: The verified DAG-CBOR document, so a caller that needs its fields doesn't
        ///   have to decode a second time.
        @discardableResult
        static func verify(
            serializedEntry: Data,
            forName name: [UInt8],
            now: Date = Date()
        ) throws -> KadDHT.IPNSData {
            /// make sure the record doesn't exceed our max
            guard serializedEntry.count <= Self.maxRecordSize else {
                throw KadDHT.ValidationError.recordTooLarge(bytes: serializedEntry.count, limit: Self.maxRecordSize)
            }
            let entry = try IpnsEntry(serializedBytes: serializedEntry)

            /// ensure hasSignatureV2 is set and not empty
            /// - Note: A V1-only record isn't acceptable.`signatureV1` covers only `value` ‖ `validity`,
            ///   so it authenticates neither the sequence number nor the TTL
            guard entry.hasSignatureV2, !entry.signatureV2.isEmpty else {
                throw KadDHT.ValidationError.missingSignatureV2
            }
            /// ensure that data is set and not empty
            guard entry.hasData, !entry.data.isEmpty else {
                throw KadDHT.ValidationError.missingData
            }

            /// extract the public key from `pubKey` when it's carried, otherwise from the name.
            let signer = try Self.signer(for: entry, name: name)

            /// deserialize `data` as DAG-CBOR.
            let data = try KadDHT.IPNSData.decode(dagCBOR: entry.data.byteArray)

            /// verify `signatureV2` over the prefix + the raw record data.
            guard let pubkey = signer.keyPair else {
                throw KadDHT.ValidationError.publicKeyUnavailable
            }
            let payload = Data(Self.signaturePrefix) + entry.data
            guard (try? pubkey.verify(signature: entry.signatureV2, for: payload)) == true else {
                throw KadDHT.ValidationError.invalidSignature
            }

            /// when the legacy protobuf fields are present they have to agree with the signed
            /// document, otherwise a reader that trusts the protobuf sees values the signature
            /// never covered.
            try Self.assertLegacyFieldsAgree(entry, with: data)

            /// ensure the record hasn't expired.
            try Self.assertNotExpired(data, now: now)

            return data
        }

        /// Resolves the public key that signed this record.
        private static func signer(for entry: IpnsEntry, name: [UInt8]) throws -> PeerID {
            guard let named = try? PeerID(fromBytesID: name) else {
                throw KadDHT.ValidationError.keyIsNotAMultihash
            }

            guard entry.hasPubKey, !entry.pubKey.isEmpty else {
                /// Nothing carried, so the Name itself has to be self-describing.
                guard named.keyPair != nil else { throw KadDHT.ValidationError.publicKeyUnavailable }
                return named
            }

            /// Ensure the carried key is a valid marshaled pubkey and that it matches the name
            let carried = try PeerID(marshaledPublicKey: entry.pubKey)
            guard carried == named else {
                throw KadDHT.ValidationError.publicKeyDoesNotMatchName(
                    name: named.b58String,
                    publicKey: carried.b58String
                )
            }
            return carried
        }

        /// Confirms the legacy top-level protobuf fields mirror the signed DAG-CBOR document.
        private static func assertLegacyFieldsAgree(_ entry: IpnsEntry, with data: KadDHT.IPNSData) throws {
            /// `value` is a proto3 scalar without presence, so an absent field is indistinguishable
            /// from an empty one — an empty `value` is treated as "not carried".
            if !entry.value.isEmpty, entry.value.byteArray != data.value {
                throw KadDHT.ValidationError.legacyFieldMismatch("value")
            }
            if entry.hasValidity, entry.validity.byteArray != data.validity {
                throw KadDHT.ValidationError.legacyFieldMismatch("validity")
            }
            if entry.hasValidityType, UInt64(exactly: entry.validityType.rawValue) != data.validityType {
                throw KadDHT.ValidationError.legacyFieldMismatch("validityType")
            }
            if entry.hasSequence, entry.sequence != data.sequence {
                throw KadDHT.ValidationError.legacyFieldMismatch("sequence")
            }
            if entry.hasTtl, entry.ttl != data.ttl {
                throw KadDHT.ValidationError.legacyFieldMismatch("ttl")
            }
        }

        /// Confirms an EOL record hasn't passed its `Validity` timestamp.
        private static func assertNotExpired(_ data: KadDHT.IPNSData, now: Date) throws {
            /// EOL is the only validity type the spec defines
            guard data.validityType == UInt64(IpnsEntry.ValidityType.eol.rawValue) else {
                throw KadDHT.ValidationError.unknownValidityType(data.validityType)
            }
            let timestamp = String(decoding: data.validity, as: UTF8.self)
            guard let endOfLife = try? RFC3339Date(string: timestamp) else {
                throw KadDHT.ValidationError.invalidValidityTimestamp(timestamp)
            }
            guard endOfLife.date > now else {
                throw KadDHT.ValidationError.recordExpired(timestamp)
            }
        }

        /// Picks the best of several IPNS records: highest sequence number, then latest validity.
        func select(key: [UInt8], values: [[UInt8]]) throws -> Int {
            let candidates = values.map { Candidate(serializedRecord: $0) }

            var bestIndex: Int? = nil
            for (index, candidate) in candidates.enumerated() {
                guard let candidate else { continue }
                guard let currentBest = bestIndex, let best = candidates[currentBest] else {
                    bestIndex = index
                    continue
                }
                /// compare the current best to the next record
                if candidate.isPreferred(over: best) {
                    /// update the best index if it's prefered
                    bestIndex = index
                }
            }

            guard let bestIndex else {
                throw KadDHT.ValidationError.noRecordsToSelect
            }
            return bestIndex
        }

        /// The two fields selection orders on, read from the signed DAG-CBOR document when the record
        /// carries one and from the legacy top-level protobuf fields otherwise.
        ///
        /// The fallback matters in both directions: a V2-only record leaves the protobuf
        /// `sequence`/`validity` unset, so reading only those would make every such record compare as
        /// sequence 0; a record that predates V2 has no `data` to read.
        private struct Candidate {
            let sequence: UInt64
            let endOfLife: RFC3339Date?

            init?(serializedRecord: [UInt8]) {
                guard let record = try? DHT.Record(serializedBytes: serializedRecord),
                    let entry = try? IpnsEntry(serializedBytes: record.value)
                else { return nil }

                let data = entry.hasData ? try? KadDHT.IPNSData.decode(dagCBOR: entry.data.byteArray) : nil

                if let data {
                    self.sequence = data.sequence
                    self.endOfLife = Self.endOfLife(
                        validity: data.validity,
                        validityType: data.validityType
                    )
                } else {
                    self.sequence = entry.sequence
                    self.endOfLife = Self.endOfLife(
                        validity: entry.hasValidity ? entry.validity.byteArray : nil,
                        validityType: UInt64(exactly: entry.validityType.rawValue)
                    )
                }
            }

            /// `true` when `self` should win over `other`.
            func isPreferred(over other: Candidate) -> Bool {
                /// Highest sequence numbers wins
                guard self.sequence == other.sequence else {
                    return self.sequence > other.sequence
                }

                /// If they have the same sequence number, prefer the later EOL.
                /// If we don't have an EOL, we prefer the other Record
                guard let ours = self.endOfLife else { return false }
                /// If we do have an EOL and the other record doesn't, we're preferred
                guard let theirs = other.endOfLife else { return true }
                /// If they both have EOLs keep the longest living record
                return ours > theirs
            }

            /// Parses `validity` as an `RFC3339Date` EOL timestamp, or `nil`
            private static func endOfLife(validity: [UInt8]?, validityType: UInt64?) -> RFC3339Date? {
                guard let validity, validityType == UInt64(IpnsEntry.ValidityType.eol.rawValue) else {
                    return nil
                }
                return try? RFC3339Date(string: String(decoding: validity, as: UTF8.self))
            }
        }
    }
}

// MARK: - Namespaced keys

extension KadDHT {
    /// Returns the bytes following `/<namespace>/` in a namespaced DHT key, asserting the namespace
    /// is the expected one.
    ///
    /// - Note: The offset is counted in *bytes*. Using a `String`'s `count` here would be a character
    /// count, which is only accidentally right for an ASCII namespace.
    static func namespacedKeyBody(_ key: [UInt8], expecting namespace: String) throws -> [UInt8] {
        guard let found = KadDHT.extractNamespace(key) else {
            throw ValidationError.notNamespaced
        }
        guard found == Array(namespace.utf8) else {
            throw ValidationError.wrongNamespace(
                expected: namespace,
                found: String(decoding: found, as: UTF8.self)
            )
        }
        /// The leading `/` and the separator `/` bracketing the namespace.
        let body = Array(key.dropFirst(found.count + 2))
        guard !body.isEmpty else { throw ValidationError.emptyKeyBody }
        return body
    }
}

// MARK: - Errors

extension KadDHT {
    /// Failures raised by the built-in `/pk/` and `/ipns/` validators.
    enum ValidationError: Error, CustomStringConvertible {
        case notNamespaced
        case wrongNamespace(expected: String, found: String)
        case emptyKeyBody
        case keyMismatch(expected: [UInt8], found: [UInt8])
        case keyIsNotAMultihash
        case publicKeyDoesNotMatchKey(key: String, publicKey: String)
        case noRecordsToSelect
        case recordTooLarge(bytes: Int, limit: Int)
        case missingSignatureV2
        case missingData
        case publicKeyUnavailable
        case publicKeyDoesNotMatchName(name: String, publicKey: String)
        case invalidSignature
        case legacyFieldMismatch(String)
        case unknownValidityType(UInt64)
        case invalidValidityTimestamp(String)
        case recordExpired(String)

        var description: String {
            switch self {
            case .notNamespaced:
                return "Validator: key is not of the form /<namespace>/<multihash>"
            case .wrongNamespace(let expected, let found):
                return "Validator: expected namespace '\(expected)', found '\(found)'"
            case .emptyKeyBody:
                return "Validator: key carries a namespace but no body"
            case .keyMismatch(let expected, let found):
                return
                    "Validator: key mismatch, expected \(expected.toHexString()) got \(found.toHexString())"
            case .keyIsNotAMultihash:
                return "Validator: key body is not a valid multihash"
            case .publicKeyDoesNotMatchKey(let key, let publicKey):
                return "Validator: /pk/ key names \(key) but carries the public key of \(publicKey)"
            case .noRecordsToSelect:
                return "Validator: no parseable records to select between"
            case .recordTooLarge(let bytes, let limit):
                return "Validator: serialized record is \(bytes) bytes, over the \(limit) byte limit"
            case .missingSignatureV2:
                return "Validator: record has no signatureV2"
            case .missingData:
                return "Validator: record has no data (DAG-CBOR) field"
            case .publicKeyUnavailable:
                return "Validator: no public key available to verify against"
            case .publicKeyDoesNotMatchName(let name, let publicKey):
                return "Validator: record names \(name) but carries the public key of \(publicKey)"
            case .invalidSignature:
                return "Validator: signatureV2 does not verify against the signed data"
            case .legacyFieldMismatch(let field):
                return "Validator: protobuf '\(field)' disagrees with the signed DAG-CBOR document"
            case .unknownValidityType(let type):
                return "Validator: unsupported validity type \(type)"
            case .invalidValidityTimestamp(let timestamp):
                return "Validator: validity '\(timestamp)' is not an RFC3339 timestamp"
            case .recordExpired(let timestamp):
                return "Validator: record expired at \(timestamp)"
            }
        }
    }
}
