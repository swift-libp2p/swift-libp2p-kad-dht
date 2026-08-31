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

/// Non-canonical spellings of the IPNS `data` map that the decoder has to refuse.
///
/// Each is the canonical document with exactly one rule broken.
private let nonCanonicalIPNSDocuments: [(label: String, hex: String)] = [
    (
        "indefinite-length map",
        "bf6354544c006556616c75654268696853657175656e6365016856616c6964697479417a6c56616c69646974795479706500ff"
    ),
    (
        "keys out of canonical order (Value before TTL)",
        "a56556616c75654268696354544c006853657175656e6365016856616c6964697479417a6c56616c69646974795479706500"
    ),
    (
        "non-minimal integer (TTL 0 spelled with a trailing byte)",
        "a56354544c18006556616c75654268696853657175656e6365016856616c6964697479417a6c56616c69646974795479706500"
    ),
    (
        "unknown field 'XXX' in place of 'TTL'",
        "a563585858006556616c75654268696853657175656e6365016856616c6964697479417a6c56616c69646974795479706500"
    ),
    (
        "duplicate key ('TTL' twice, so 'Value' is missing)",
        "a56354544c006354544c006853657175656e6365016856616c6964697479417a6c56616c69646974795479706500"
    ),
    (
        "Value as a text string rather than bytes",
        "a56354544c006556616c75656268696853657175656e6365016856616c6964697479417a6c56616c69646974795479706500"
    ),
    (
        "four fields instead of five",
        "a46354544c006556616c75654268696853657175656e6365016856616c6964697479417a"
    ),
    (
        "truncated mid-item",
        "a56354544c006556616c756542"
    ),
]

extension LibP2PKadDHTTests {

    static let keyTypes = [LibP2PCrypto.Keys.KeyPairType.Ed25519, .Secp256k1, .RSA(bits: .B1024)]

    /// The minimal DAG-CBOR reader behind IPNS `data`.
    ///
    /// Every case here is expressed as literal bytes rather than round-tripped through our own
    /// encoder: the decoder's whole job is to reject spellings the encoder would never produce, so
    /// testing it against the encoder would only prove they agree with each other.
    @Suite("IPNS DAG-CBOR Tests", .serialized)
    struct IPNSDataTests {

        /// The canonical encoding of `{TTL: 0, Value: h'6869', Sequence: 1, Validity: h'7a',
        /// ValidityType: 0}`, hand-assembled from the CBOR major types.
        ///
        /// The key order is the DAG-CBOR rule, "sorted in (byte-wise) lexical order, including their
        /// major type 3 and length", which is not alphabetical: `TTL` leads because its length
        /// byte is smaller, and `Sequence` precedes `Validity` on `S` < `V`.
        static let canonical =
            "a5"  // map(5)
            + "63" + "54544c" + "00"  // "TTL": 0
            + "65" + "56616c7565" + "42" + "6869"  // "Value": h'6869'
            + "68" + "53657175656e6365" + "01"  // "Sequence": 1
            + "68" + "56616c6964697479" + "41" + "7a"  // "Validity": h'7a'
            + "6c" + "56616c696469747954797065" + "00"  // "ValidityType": 0

        static var expected: KadDHT.IPNSData {
            KadDHT.IPNSData(
                value: Array("hi".utf8),
                validity: Array("z".utf8),
                validityType: 0,
                sequence: 1,
                ttl: 0
            )
        }

        @Test func decodesTheCanonicalDocument() throws {
            let decoded = try KadDHT.IPNSData.decode(dagCBOR: Array(hex: Self.canonical))
            #expect(decoded == Self.expected)
        }

        /// Our encoder has to produce the canonical spelling, since that's the byte string a
        /// publisher signs.
        @Test func encodesTheCanonicalDocument() throws {
            #expect(Self.expected.encode().toHexString() == Self.canonical)
        }

        @Test func roundTripsLargeArguments() throws {
            let document = KadDHT.IPNSData(
                value: Array(repeating: 0xab, count: 300),
                validity: Array("2035-01-01T00:00:00.000000001Z".utf8),
                validityType: 0,
                sequence: .max,
                ttl: 3_600_000_000_000
            )
            #expect(try KadDHT.IPNSData.decode(dagCBOR: document.encode()) == document)
        }

        @Test(arguments: nonCanonicalIPNSDocuments)
        func rejectsNonCanonicalDocuments(_ testCase: (label: String, hex: String)) throws {
            #expect(throws: (any Error).self, "should reject \(testCase.label)") {
                try KadDHT.IPNSData.decode(dagCBOR: Array(hex: testCase.hex))
            }
        }

        /// The signature covers exactly the `data` bytes, so anything appended to them is unsigned
        /// and has to be refused rather than ignored.
        @Test func rejectsTrailingBytes() throws {
            #expect(throws: (any Error).self) {
                try KadDHT.IPNSData.decode(dagCBOR: Array(hex: Self.canonical) + [0x00])
            }
        }
    }

    /// `/pk/` records must bind the key to the public key they carry.
    @Suite("PubKey Validator Tests", .serialized)
    struct PubKeyValidatorTests {

        @Test(arguments: keyTypes)
        func acceptsAKeyThatMatchesItsPublicKey(_ keyType: LibP2PCrypto.Keys.KeyPairType) throws {
            let peer = try PeerID(keyType)
            let record = try KadDHT.createPubKeyRecord(peerID: peer).toProtobuf()

            #expect(throws: Never.self) {
                try KadDHT.PubKeyValidator().validate(
                    key: record.key.byteArray,
                    value: record.value.byteArray
                )
            }
        }

        @Test(arguments: keyTypes)
        func rejectsAnotherPeersPublicKey(_ keyType: LibP2PCrypto.Keys.KeyPairType) throws {
            let alice = try PeerID(keyType)
            let bob = try PeerID(keyType)

            /// Alice's key, Bob's public key.
            #expect(throws: (any Error).self) {
                try KadDHT.PubKeyValidator().validate(
                    key: "/pk/".bytes + alice.id,
                    value: try bob.marshalPublicKey()
                )
            }
        }

        @Test(arguments: keyTypes)
        func rejectsAKeyOutsideThePkNamespace(_ keyType: LibP2PCrypto.Keys.KeyPairType) throws {
            let peer = try PeerID(keyType)

            #expect(throws: (any Error).self) {
                try KadDHT.PubKeyValidator().validate(
                    key: "/ipns/".bytes + peer.id,
                    value: try peer.marshalPublicKey()
                )
            }
        }

        /// The validator sees record values, so a value that's a serialized `DHT.Record` rather than
        /// a marshaled public key has to be refused.
        @Test(arguments: keyTypes)
        func rejectsASerializedRecordAsItsValue(_ keyType: LibP2PCrypto.Keys.KeyPairType) throws {
            let peer = try PeerID(keyType)
            let record = try KadDHT.createPubKeyRecord(peerID: peer).toProtobuf()

            #expect(throws: (any Error).self) {
                try KadDHT.PubKeyValidator().validate(
                    key: record.key.byteArray,
                    value: try record.serializedData().byteArray
                )
            }
        }
    }

    /// `/ipns/` record verification, per the spec's ordered steps.
    @Suite("IPNS Validator Tests", .serialized)
    struct IPNSValidatorTests {

        @Test func acceptsAValidV2Record() throws {
            // Ed25519 and SecP256k1 support inlining pubkeys, so we expect these to never fail
            let peerED25519 = try PeerID(.Ed25519)
            let signed1 = try IPNSFixture(name: peerED25519, includePubKey: false)
            #expect(throws: Never.self) {
                try KadDHT.IPNSValidator().validate(key: signed1.key, value: signed1.value)
            }

            let peerSecP256k1 = try PeerID(.Secp256k1)
            let signed2 = try IPNSFixture(name: peerSecP256k1, includePubKey: false)
            #expect(throws: Never.self) {
                try KadDHT.IPNSValidator().validate(key: signed2.key, value: signed2.value)
            }

            // RSA doesn't support inlined pubkeys, so when we don't include the pubkey in
            // the record, our validator throws due to no pubkey available
            let peerRSA = try PeerID(.RSA(bits: .B1024))
            let signed3 = try IPNSFixture(name: peerRSA, includePubKey: false)

            #expect(throws: KadDHT.ValidationError.self) {
                try KadDHT.IPNSValidator().validate(key: signed3.key, value: signed3.value)
            }
        }

        @Test(arguments: keyTypes)
        func acceptsAValidRecordCarryingItsPublicKey(_ keyType: LibP2PCrypto.Keys.KeyPairType) throws {
            let peer = try PeerID(keyType)
            let signed = try IPNSFixture(name: peer, includePubKey: true)

            #expect(throws: Never.self) {
                try KadDHT.IPNSValidator().validate(key: signed.key, value: signed.value)
            }
        }

        @Test(arguments: keyTypes)
        func rejectsATamperedSignature(_ keyType: LibP2PCrypto.Keys.KeyPairType) throws {
            let peer = try PeerID(keyType)
            let signed = try IPNSFixture(name: peer, tamperSignature: true)

            #expect(throws: (any Error).self) {
                try KadDHT.IPNSValidator().validate(key: signed.key, value: signed.value)
            }
        }

        /// A record for Alice's name, signed by Bob.
        @Test(arguments: keyTypes)
        func rejectsARecordSignedByTheWrongKey(_ keyType: LibP2PCrypto.Keys.KeyPairType) throws {
            let alice = try PeerID(keyType)
            let bob = try PeerID(.Ed25519)
            let signed = try IPNSFixture(name: alice, signer: bob)

            #expect(throws: (any Error).self) {
                try KadDHT.IPNSValidator().validate(key: signed.key, value: signed.value)
            }
        }

        /// A record for Alice's name, signed by Bob but with Bob's key attached, so the signature *does* verify against the
        /// carried key. Ensures the key-name enforcement happens.
        @Test(arguments: keyTypes)
        func rejectsACarriedPublicKeyThatDoesNotMatchTheName(_ keyType: LibP2PCrypto.Keys.KeyPairType) throws {
            let alice = try PeerID(keyType)
            let bob = try PeerID(.Ed25519)
            let signed = try IPNSFixture(name: alice, signer: bob, includePubKey: true)

            #expect(throws: (any Error).self) {
                try KadDHT.IPNSValidator().validate(key: signed.key, value: signed.value)
            }
        }

        @Test(arguments: keyTypes)
        func rejectsAnExpiredRecord(_ keyType: LibP2PCrypto.Keys.KeyPairType) throws {
            let peer = try PeerID(keyType)
            let signed = try IPNSFixture(name: peer, validity: "2020-01-01T00:00:00Z")

            #expect(throws: (any Error).self) {
                try KadDHT.IPNSValidator().validate(key: signed.key, value: signed.value)
            }
        }

        /// Where the legacy protobuf fields are present they have to mirror the signed document
        @Test(arguments: keyTypes)
        func rejectsProtobufFieldsThatDisagreeWithTheSignedDocument(_ keyType: LibP2PCrypto.Keys.KeyPairType) throws {
            let peer = try PeerID(keyType)
            let signed = try IPNSFixture(name: peer, sequence: 4) { entry in
                entry.sequence = 9
            }

            #expect(throws: (any Error).self) {
                try KadDHT.IPNSValidator().validate(key: signed.key, value: signed.value)
            }
        }

        @Test(arguments: keyTypes)
        func rejectsAV1OnlyRecord(_ keyType: LibP2PCrypto.Keys.KeyPairType) throws {
            let peer = try PeerID(keyType)
            /// Strip the V2 fields, leaving the legacy shape a pre-V2 publisher would have emitted.
            let signed = try IPNSFixture(name: peer) { entry in
                entry.clearSignatureV2()
                entry.clearData()
                entry.signatureV1 = Data(repeating: 0x01, count: 64)
            }

            #expect(throws: (any Error).self) {
                try KadDHT.IPNSValidator().validate(key: signed.key, value: signed.value)
            }
        }

        @Test(arguments: keyTypes)
        func rejectsARecordOverTheSizeLimit(_ keyType: LibP2PCrypto.Keys.KeyPairType) throws {
            let peer = try PeerID(keyType)
            /// A `value` big enough to push the serialized entry past 10 KiB.
            let signed = try IPNSFixture(
                name: peer,
                value: String(repeating: "a", count: KadDHT.IPNSValidator.maxRecordSize)
            )

            #expect(throws: (any Error).self) {
                try KadDHT.IPNSValidator().validate(key: signed.key, value: signed.value)
            }
        }

        @Test(arguments: keyTypes)
        func rejectsANonCanonicalSignedDocument(_ keyType: LibP2PCrypto.Keys.KeyPairType) throws {
            let peer = try PeerID(keyType)
            /// Signed correctly, but the signed bytes are a non-canonical spelling of the map. The
            /// signature verifies; the decoder is what has to refuse it.
            let signed = try IPNSFixture(
                name: peer,
                includeLegacyFields: false,
                overrideData: Array(hex: nonCanonicalIPNSDocuments[1].hex)
            )

            #expect(throws: (any Error).self) {
                try KadDHT.IPNSValidator().validate(key: signed.key, value: signed.value)
            }
        }

        /// A V2-only record leaves the legacy protobuf `sequence` unset. Selection has to read the
        /// signed document, or every such record compares as sequence 0.
        @Test(arguments: keyTypes)
        func selectReadsTheSequenceFromTheSignedDocument(_ keyType: LibP2PCrypto.Keys.KeyPairType) throws {
            let peer = try PeerID(keyType)
            let low = try IPNSFixture(name: peer, sequence: 2, includeLegacyFields: false)
            let high = try IPNSFixture(name: peer, sequence: 8, includeLegacyFields: false)

            let validator = KadDHT.IPNSValidator()
            #expect(try validator.select(key: low.key, values: [low.value, high.value]) == 1)
            #expect(try validator.select(key: low.key, values: [high.value, low.value]) == 0)
        }
    }
}

// MARK: - Fixtures

extension LibP2PKadDHTTests {

    /// A signed `/ipns/` record and the routing key it belongs under.
    struct IPNSFixture {
        /// `/ipns/<multihash-of-name>`
        let key: [UInt8]
        /// The serialized `IpnsEntry` — the record's `value`, which is what validators see.
        let value: [UInt8]
        /// The serialized `DHT.Record` wrapping the `IpnsEntry`, i.e. what goes on the wire.
        let record: [UInt8]

        /// - Parameters:
        ///   - name: The peer whose IPNS Name the record is published under.
        ///   - signer: The peer whose private key signs it. Defaults to `name`; pass a different peer
        ///     to build a forgery.
        ///   - includeLegacyFields: Mirror the signed document into the top-level protobuf fields, as
        ///     a real publisher does for backwards compatibility.
        ///   - includePubKey: Attach the signer's marshaled public key.
        ///   - tamperSignature: Flip a bit of `signatureV2` after signing.
        ///   - overrideData: Sign these bytes instead of the canonical encoding of the document.
        ///   - mutate: Applied to the finished `IpnsEntry`, after signing.
        init(
            name: PeerID,
            signer: PeerID? = nil,
            value: String = "/ipfs/bafkqablimvwgy3y",
            validity: String = "2035-01-01T00:00:00Z",
            sequence: UInt64 = 1,
            ttl: UInt64 = 60_000_000_000,
            includeLegacyFields: Bool = true,
            includePubKey: Bool = false,
            tamperSignature: Bool = false,
            overrideData: [UInt8]? = nil,
            mutate: ((inout IpnsEntry) -> Void)? = nil
        ) throws {
            let document = KadDHT.IPNSData(
                value: Array(value.utf8),
                validity: Array(validity.utf8),
                validityType: UInt64(IpnsEntry.ValidityType.eol.rawValue),
                sequence: sequence,
                ttl: ttl
            )
            let cbor = overrideData ?? document.encode()

            let signingPeer = signer ?? name
            guard let keyPair = signingPeer.keyPair else {
                throw Errors.signerHasNoPrivateKey
            }
            var signature = try keyPair.sign(
                message: Data(KadDHT.IPNSValidator.signaturePrefix) + Data(cbor)
            )
            if tamperSignature {
                signature[signature.startIndex] ^= 0xff
            }

            var entry = try IpnsEntry.with { entry in
                entry.data = Data(cbor)
                entry.signatureV2 = signature
                if includeLegacyFields {
                    entry.value = Data(document.value)
                    entry.validity = Data(document.validity)
                    entry.validityType = .eol
                    entry.sequence = document.sequence
                    entry.ttl = document.ttl
                }
                if includePubKey {
                    entry.pubKey = try Data(signingPeer.marshalPublicKey())
                }
            }
            mutate?(&entry)

            let key = "/ipns/".bytes + name.id
            let serializedEntry = try entry.serializedData()
            self.key = key
            self.value = serializedEntry.byteArray
            self.record = try DHT.Record.with { record in
                record.key = Data(key)
                record.value = serializedEntry
                record.timeReceived = RFC3339Date().string
            }.serializedData().byteArray
        }

        enum Errors: Error {
            case signerHasNoPrivateKey
        }
    }
}
