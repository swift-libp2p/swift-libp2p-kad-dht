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

import Foundation

extension KadDHT {
    /// The DAG-CBOR document carried in an IPNS record's `data` field, and the payload its
    /// `signatureV2` is computed over.
    ///
    /// This is deliberately *not* a general CBOR implementation. IPNS `data` is a single map with
    /// five known keys, so we decode exactly that shape and reject everything else. A general
    /// decoder would have to make choices about tags, floats, nesting and non-canonical encodings
    /// that this format never uses, and every one of those choices is attack surface on a document
    /// whose bytes are signed — the signature covers the raw CBOR, so a decoder that accepts two
    /// spellings of the same map lets a publisher and a validator disagree about what was signed.
    ///
    /// Strictness rules come from the DAG-CBOR spec:
    /// - "The keys in every map must be sorted in (byte-wise) lexical order, including their major
    ///   type 3 and length."
    /// - "Indefinite-length items are not supported, only definite-length items are usable."
    /// - "Integer encoding must be as short as possible."
    /// - "In DAG-CBOR, map keys must be strings."
    struct IPNSData: Equatable {
        /// The path this name resolves to, e.g. `/ipfs/bafy…`.
        let value: [UInt8]

        /// The end-of-life timestamp, as the UTF-8 bytes of an RFC3339 string.
        let validity: [UInt8]

        /// `0` — EOL — is the only type the spec defines.
        let validityType: UInt64

        /// Monotonically increasing per publication; the primary ordering key for selection.
        let sequence: UInt64

        /// Suggested caching lifetime, in nanoseconds.
        let ttl: UInt64

        /// The five keys of the canonical map, spelled exactly as they appear on the wire.
        ///
        /// - Note: The declaration order here is the canonical *encoding* order (byte-wise on the
        ///   encoded key, header included), which is not alphabetical: `TTL` sorts first because its
        ///   length byte is smaller, and `Sequence` precedes `Validity` because `S` < `V`.
        enum Field: String, CaseIterable {
            case ttl = "TTL"
            case value = "Value"
            case sequence = "Sequence"
            case validity = "Validity"
            case validityType = "ValidityType"
        }
    }
}

// MARK: - Decoding

extension KadDHT.IPNSData {

    /// Decodes the canonical IPNS `data` map.
    ///
    /// - Throws: `Errors` for anything that isn't exactly that map in canonical DAG-CBOR form.
    static func decode(dagCBOR encoded: [UInt8]) throws -> KadDHT.IPNSData {
        var reader = Reader(encoded)

        let (major, entryCount) = try reader.header()
        guard major == MajorType.map else { throw Errors.expectedMap(major) }
        guard entryCount == UInt64(Field.allCases.count) else {
            throw Errors.unexpectedFieldCount(Int(clamping: entryCount))
        }

        var value: [UInt8]?
        var validity: [UInt8]?
        var validityType: UInt64?
        var sequence: UInt64?
        var ttl: UInt64?

        /// The previous key *as encoded*, so the sort check can compare header bytes and all.
        var previousKey: [UInt8]? = nil

        for _ in 0..<entryCount {
            let keyStart = reader.offset
            let (keyMajor, keyLength) = try reader.header()
            guard keyMajor == MajorType.textString else { throw Errors.nonStringMapKey(keyMajor) }
            let keyBytes = try reader.take(keyLength)
            let encodedKey = Array(encoded[keyStart..<reader.offset])

            /// Strictly ascending, which rejects duplicate keys along with unsorted ones.
            if let previousKey, !Self.isAscending(previousKey, encodedKey) {
                throw Errors.unsortedMapKeys
            }
            previousKey = encodedKey

            guard let name = String(bytes: keyBytes, encoding: .utf8), let field = Field(rawValue: name) else {
                throw Errors.unknownField(String(decoding: keyBytes, as: UTF8.self))
            }

            switch field {
            case .ttl: ttl = try reader.unsignedIntegerValue()
            case .value: value = try reader.byteStringValue()
            case .sequence: sequence = try reader.unsignedIntegerValue()
            case .validity: validity = try reader.byteStringValue()
            case .validityType: validityType = try reader.unsignedIntegerValue()
            }
        }

        /// The signature covers these exact bytes, so anything appended to them is not signed.
        guard reader.isAtEnd else { throw Errors.trailingBytes(encoded.count - reader.offset) }

        guard let value, let validity, let validityType, let sequence, let ttl else {
            /// Unreachable while `entryCount` equals the field count and keys are unique, but a
            /// missing binding here would otherwise mean silently substituting a zero value.
            throw Errors.unexpectedFieldCount(Int(entryCount))
        }

        return KadDHT.IPNSData(
            value: value,
            validity: validity,
            validityType: validityType,
            sequence: sequence,
            ttl: ttl
        )
    }

    /// Byte-wise lexicographic `lhs < rhs`, with a shorter prefix ordering before its extensions.
    private static func isAscending(_ lhs: [UInt8], _ rhs: [UInt8]) -> Bool {
        for (l, r) in zip(lhs, rhs) where l != r { return l < r }
        return lhs.count < rhs.count
    }

    /// CBOR major types this format uses. Everything else is rejected.
    private enum MajorType {
        static let unsignedInteger: UInt8 = 0
        static let byteString: UInt8 = 2
        static let textString: UInt8 = 3
        static let map: UInt8 = 5
    }

    /// A cursor over the encoded document.
    private struct Reader {
        private let buffer: [UInt8]
        private(set) var offset: Int = 0

        init(_ buffer: [UInt8]) {
            self.buffer = buffer
        }

        var isAtEnd: Bool { self.offset >= self.buffer.count }

        mutating func byte() throws -> UInt8 {
            guard self.offset < self.buffer.count else { throw Errors.truncated }
            defer { self.offset += 1 }
            return self.buffer[self.offset]
        }

        /// Reads a major type and its argument.
        ///
        /// Rejects indefinite lengths and any argument that could have been spelled shorter.
        mutating func header() throws -> (major: UInt8, argument: UInt64) {
            let initial = try self.byte()
            let major = initial >> 5
            let additional = initial & 0x1f

            switch additional {
            case 0...23:
                return (major, UInt64(additional))
            case 24:
                let argument = try self.unsigned(width: 1)
                /// 0…23 has to live in the initial byte.
                guard argument > 23 else { throw Errors.nonMinimalEncoding }
                return (major, argument)
            case 25:
                let argument = try self.unsigned(width: 2)
                guard argument > UInt64(UInt8.max) else { throw Errors.nonMinimalEncoding }
                return (major, argument)
            case 26:
                let argument = try self.unsigned(width: 4)
                guard argument > UInt64(UInt16.max) else { throw Errors.nonMinimalEncoding }
                return (major, argument)
            case 27:
                let argument = try self.unsigned(width: 8)
                guard argument > UInt64(UInt32.max) else { throw Errors.nonMinimalEncoding }
                return (major, argument)
            case 31:
                throw Errors.indefiniteLength
            default:
                throw Errors.reservedAdditionalInformation(additional)
            }
        }

        private mutating func unsigned(width: Int) throws -> UInt64 {
            var argument: UInt64 = 0
            for _ in 0..<width { argument = (argument << 8) | UInt64(try self.byte()) }
            return argument
        }

        /// Consumes `count` bytes, bounds-checked against the remaining input rather than against
        /// `count` itself — a forged multi-gigabyte length must not become an allocation.
        mutating func take(_ count: UInt64) throws -> [UInt8] {
            guard count <= UInt64(self.buffer.count - self.offset) else { throw Errors.truncated }
            let end = self.offset + Int(count)
            defer { self.offset = end }
            return Array(self.buffer[self.offset..<end])
        }

        mutating func unsignedIntegerValue() throws -> UInt64 {
            let (major, argument) = try self.header()
            guard major == MajorType.unsignedInteger else { throw Errors.expectedUnsignedInteger(major) }
            return argument
        }

        mutating func byteStringValue() throws -> [UInt8] {
            let (major, length) = try self.header()
            guard major == MajorType.byteString else { throw Errors.expectedByteString(major) }
            return try self.take(length)
        }
    }

    enum Errors: Error, CustomStringConvertible {
        case truncated
        case indefiniteLength
        case nonMinimalEncoding
        case reservedAdditionalInformation(UInt8)
        case expectedMap(UInt8)
        case unexpectedFieldCount(Int)
        case nonStringMapKey(UInt8)
        case unsortedMapKeys
        case unknownField(String)
        case expectedUnsignedInteger(UInt8)
        case expectedByteString(UInt8)
        case trailingBytes(Int)

        var description: String {
            switch self {
            case .truncated:
                return "IPNSData: CBOR ended mid-item"
            case .indefiniteLength:
                return "IPNSData: indefinite-length items are not valid DAG-CBOR"
            case .nonMinimalEncoding:
                return "IPNSData: argument is not encoded as short as possible"
            case .reservedAdditionalInformation(let value):
                return "IPNSData: reserved additional information \(value)"
            case .expectedMap(let major):
                return "IPNSData: expected a map, found major type \(major)"
            case .unexpectedFieldCount(let count):
                return "IPNSData: expected \(Field.allCases.count) fields, found \(count)"
            case .nonStringMapKey(let major):
                return "IPNSData: map keys must be text strings, found major type \(major)"
            case .unsortedMapKeys:
                return "IPNSData: map keys are not in canonical (byte-wise ascending) order"
            case .unknownField(let name):
                return "IPNSData: unexpected field '\(name)'"
            case .expectedUnsignedInteger(let major):
                return "IPNSData: expected an unsigned integer, found major type \(major)"
            case .expectedByteString(let major):
                return "IPNSData: expected a byte string, found major type \(major)"
            case .trailingBytes(let count):
                return "IPNSData: \(count) unsigned byte(s) trail the signed document"
            }
        }
    }
}

// MARK: - Encoding

extension KadDHT.IPNSData {

    /// Re-encodes this document in canonical DAG-CBOR.
    ///
    /// Round-tripping is what makes the decoder testable — `decode(dagCBOR: encode()) == self` — and
    /// it's the payload a publisher signs, so the field order here has to be the canonical one.
    func encode() -> [UInt8] {
        var out: [UInt8] = []
        out.append(contentsOf: Self.header(major: MajorType.map, argument: UInt64(Field.allCases.count)))
        for field in Field.allCases {
            let key = Array(field.rawValue.utf8)
            out.append(contentsOf: Self.header(major: MajorType.textString, argument: UInt64(key.count)))
            out.append(contentsOf: key)

            switch field {
            case .ttl:
                out.append(contentsOf: Self.header(major: MajorType.unsignedInteger, argument: self.ttl))
            case .value:
                out.append(contentsOf: Self.byteString(self.value))
            case .sequence:
                out.append(contentsOf: Self.header(major: MajorType.unsignedInteger, argument: self.sequence))
            case .validity:
                out.append(contentsOf: Self.byteString(self.validity))
            case .validityType:
                out.append(contentsOf: Self.header(major: MajorType.unsignedInteger, argument: self.validityType))
            }
        }
        return out
    }

    private static func byteString(_ bytes: [UInt8]) -> [UInt8] {
        Self.header(major: MajorType.byteString, argument: UInt64(bytes.count)) + bytes
    }

    /// Emits a major type and argument using the shortest legal spelling.
    private static func header(major: UInt8, argument: UInt64) -> [UInt8] {
        let prefix = major << 5
        switch argument {
        case 0...23:
            return [prefix | UInt8(argument)]
        case 24...UInt64(UInt8.max):
            return [prefix | 24, UInt8(argument)]
        case (UInt64(UInt8.max) + 1)...UInt64(UInt16.max):
            return [prefix | 25] + Self.bigEndian(argument, width: 2)
        case (UInt64(UInt16.max) + 1)...UInt64(UInt32.max):
            return [prefix | 26] + Self.bigEndian(argument, width: 4)
        default:
            return [prefix | 27] + Self.bigEndian(argument, width: 8)
        }
    }

    private static func bigEndian(_ argument: UInt64, width: Int) -> [UInt8] {
        (0..<width).reversed().map { UInt8(truncatingIfNeeded: argument >> (8 * UInt64($0))) }
    }
}
