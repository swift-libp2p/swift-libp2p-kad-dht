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

extension KadDHT {

    /// The maximum provider key length we'll accept
    /// - Note: We match go-libp2p-kad-dht's bound in `handleGetProviders` and `handleAddProvider`.
    static let maxProviderKeyLength: Int = 80

    enum Query: CustomStringConvertible {
        /// In the request `key` is the raw target of the lookup.
        ///
        /// - Note: This is usually the binary PeerId of the node being looked for, but canonical FIND_NODE
        ///   returns the closest peers to *any* key, and Kademlia treats it as opaque bytes.
        ///   `PUT_VALUE`/`GET_VALUE` lookups walk towards a *record* key (e.g. `/pk/<multihash>`), and
        ///   rust-libp2p's bootstrap probes raw 32-byte keys. Neither is a valid PeerID.
        case findNode(key: [UInt8])

        /// In the request, `key` is an unstructured array of bytes.
        case getValue(key: [UInt8])

        /// In the request, `record` is set to the record to be stored.
        /// In the response, `key` is set to equal the key of the Record.
        case putValue(key: [UInt8], record: DHT.Record)

        /// In the request, `key` is the multihash of the content being looked for.
        ///
        /// - Note: Provider keys are multihashes, not CIDs.
        case getProviders(key: [UInt8])

        /// In the request, `key` is the multihash of the content being provided, and `providerPeers` carries
        /// the provider's own `PeerInfo` (the entries whose ID matches the sender are the ones recorded).
        case addProvider(key: [UInt8], providerPeers: [DHT.Message.Peer])

        /// Deprecated message type replaced by the dedicated ping protocol. Implementations may still handle incoming PING requests for backwards compatibility. Implementations must not actively send PING requests.
        case ping  // Deprecated

        func encode() throws -> [UInt8] {
            var req = DHT.Message()

            switch self {
            case .ping:
                /// Ping is deprecated, we don't send ping messages through DHT anymore! Use the dedicated "ipfs/ping/1.0.0" protocol instead.
                /// - Note: Encoding stays for round-trip tests. PING carries no payload.
                req.type = .ping

            case let .findNode(key):
                req.type = .findNode
                /// In the request, key is the Kademlia key whose closest peers we want (often, but not necessarily, a PeerId).
                guard !key.isEmpty else { throw Errors.encodingError }
                req.key = Data(key)

            case let .getValue(key):
                req.type = .getValue
                /// In the request, key is an unstructured array of bytes.
                req.key = Data(key)

            case let .putValue(key, record):
                //guard !providers.isEmpty else { throw Errors.cantPutValueWithoutExternallyDialableAddress }

                req.type = .putValue
                req.key = Data(key)

                /// In the request `record` is set to the record to be stored and `key` on Message is set to equal `key` of the Record.
                guard req.key == record.key else { throw Errors.encodingError }

                let serializedRecord = try record.serializedData()
                guard serializedRecord.count <= KadDHT.Defaults.maxRecordSize else {
                    throw Errors.recordTooLarge(bytes: serializedRecord.count, limit: KadDHT.Defaults.maxRecordSize)
                }
                req.record = serializedRecord
            //req.providerPeers = try providers.map { try DHT.Message.Peer($0) }

            case let .getProviders(key):
                req.type = .getProviders
                guard (1...KadDHT.maxProviderKeyLength).contains(key.count) else { throw Errors.encodingError }
                req.key = Data(key)

            case let .addProvider(key, providerPeers):
                req.type = .addProvider
                guard (1...KadDHT.maxProviderKeyLength).contains(key.count) else { throw Errors.encodingError }
                req.key = Data(key)
                /// The provider advertises itself (and its dialable addresses) in `providerPeers`.
                req.providerPeers = providerPeers
            }

            let payload = try [UInt8](req.serializedData())
            //return payload
            return putUVarInt(UInt64(payload.count)) + payload
        }

        /// This is someone sending our node a query, the remote peer is the initiator, we're just reacting...
        ///
        /// `bytes` is one complete protobuf message whose uvarint length
        /// prefix has already been stripped by the route's
        /// `VarintFrameDecoder` (see `registerDHTRoute`). We deliberately do
        /// NOT re-read a length prefix here: the old
        /// `prefix.value == bytes.count - bytesRead` assertion assumed the
        /// whole frame arrived in a single read, which breaks against peers
        /// that segment their writes differently (e.g. rust-libp2p).
        static func decode(_ bytes: [UInt8]) throws -> Query {
            guard let dht = try? DHT.Message(serializedBytes: bytes) else { throw Errors.DecodingErrorInvalidType }

            switch dht.type {
            case .findNode:
                /// .findNode
                /// In the request, key is an arbitrary Kademlia key. We do
                /// NOT require it to be a multihash-shaped PeerId: canonical
                /// FIND_NODE finds the closest peers to any key, and peers
                /// like rust-libp2p send raw 32-byte keys during bootstrap.
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }
                return Query.findNode(key: [UInt8](dht.key))

            case .getValue:
                /// .findValue
                ///In the request, key is an unstructured array of bytes.
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }
                return Query.getValue(key: [UInt8](dht.key))

            case .putValue:
                /// .store
                /// In the request, record is set to the record to be stored and key on Message is set to equal key of the Record.
                guard dht.record.count <= KadDHT.Defaults.maxRecordSize else {
                    throw Errors.recordTooLarge(bytes: dht.record.count, limit: KadDHT.Defaults.maxRecordSize)
                }
                let rec = try DHT.Record(serializedBytes: dht.record)
                guard rec.hasValue, rec.hasKey, !rec.value.isEmpty, !rec.key.isEmpty, dht.key == rec.key else {
                    throw Errors.DecodingErrorInvalidType
                }
                //let providers = try dht.providerPeers.map { try $0.toPeerInfo() }
                return Query.putValue(key: [UInt8](dht.key), record: rec)

            case .getProviders:
                /// In the request, key is the multihash of the content being looked for.
                guard dht.hasKey, (1...KadDHT.maxProviderKeyLength).contains(dht.key.count) else {
                    throw Errors.DecodingErrorInvalidType
                }
                return Query.getProviders(key: [UInt8](dht.key))

            case .addProvider:
                /// In the request, key is the multihash of the content being provided.
                guard dht.hasKey, (1...KadDHT.maxProviderKeyLength).contains(dht.key.count) else {
                    throw Errors.DecodingErrorInvalidType
                }
                return Query.addProvider(key: [UInt8](dht.key), providerPeers: dht.providerPeers)

            case .ping:
                /// .ping (deprecated)
                return Query.ping

            default:
                throw Errors.DecodingErrorInvalidType
            }
        }

        /// The result to report for an RPC the peer never answers, or `nil` when we expect a reply.
        ///
        /// go's `handleAddProvider` returns no message at all, so waiting for an echo means every
        /// announce burns the full `connectionTimeout` against a peer that behaved correctly. There's
        /// nothing on the wire to decode either
        ///
        /// Only ADD_PROVIDER is fire-and-forget. PUT_VALUE still expects its echo, matching go, which
        /// reads a reply for every other request type.
        var fireAndForgetResponse: Response? {
            switch self {
            case let .addProvider(key, providerPeers):
                .addProvider(cid: key, providerPeers: providerPeers)
            case .findNode, .getValue, .putValue, .getProviders, .ping:
                nil
            }
        }

        var description: String {
            switch self {
            case .findNode(let key):
                return "Query::FindNode(key: \(KadDHT.keyToHumanReadableString(key)))"
            case .getValue(let key):
                return "Query::GetValue(key: \(KadDHT.keyToHumanReadableString(key)))"
            case .putValue(let key, let record):
                return "Query::PutValue(key: \(KadDHT.keyToHumanReadableString(key)), record: \(record))"
            case .getProviders(let key):
                return "Query::GetProviders(key: \(KadDHT.keyToHumanReadableString(key)))"
            case .addProvider(let key, let providerPeers):
                return
                    "Query::AddProviders(key: \(KadDHT.keyToHumanReadableString(key)), providers: \(providerPeers.count))"
            case .ping:
                return "Query::PING"
            }
        }
    }
}
