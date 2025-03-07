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

    enum Query: CustomStringConvertible {
        /// In the request key must be set to the binary PeerId of the node to be found
        case findNode(id: PeerID)
        /// In the request key is an unstructured array of bytes.
        case getValue(key: [UInt8])
        /// In the request record is set to the record to be stored and key on Message is set to equal key of the Record.
        case putValue(key: [UInt8], record: DHT.Record)
        /// In the request key is set to a CID.
        case getProviders(cid: [UInt8])
        /// In the request key is set to a CID.
        case addProvider(cid: [UInt8])
        /// Deprecated message type replaced by the dedicated ping protocol. Implementations may still handle incoming PING requests for backwards compatibility. Implementations must not actively send PING requests.
        case ping  // Deprecated

        func encode() throws -> [UInt8] {
            var req = DHT.Message()

            switch self {
            case .ping:
                /// Ping is deprecated, we don't send ping messages through DHT anymore! Use the dedicated "ipfs/ping/1.0.0" protocol instead.
                //throw Errors.encodingError
                req.type = .ping
                req.key = Data(DispatchTime.now().uptimeNanoseconds.toBytes)

            case let .findNode(pid):
                req.type = .findNode
                ///  In the request, key must be set to the binary PeerId of the node to be found
                req.key = Data(pid.id)

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

                req.record = try record.serializedData()
            //req.providerPeers = try providers.map { try DHT.Message.Peer($0) }

            case let .getProviders(key):
                req.type = .getProviders
                req.key = Data(key)

            case let .addProvider(key):
                req.type = .addProvider
                req.key = Data(key)
            }

            let payload = try [UInt8](req.serializedData())
            //return payload
            return putUVarInt(UInt64(payload.count)) + payload
        }

        /// This is someone sending our node a query, the remote peer is the initiator, we're just reacting...
        static func decode(_ bytes: [UInt8]) throws -> Query {
            let prefix = uVarInt(bytes)
            guard prefix.value > 0, prefix.value == (bytes.count - prefix.bytesRead) else {
                throw Errors.DecodingErrorInvalidLength
            }
            let payload: [UInt8] = [UInt8](bytes.dropFirst(prefix.bytesRead))

            guard let dht = try? DHT.Message(contiguousBytes: payload) else { throw Errors.DecodingErrorInvalidType }

            switch dht.type {
            case .findNode:
                /// .findNode
                /// In the request, key must be set to the binary PeerId of the node to be found
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }
                guard let cid = try? PeerID(fromBytesID: [UInt8](dht.key)) else {
                    throw Errors.DecodingErrorInvalidType
                }
                return Query.findNode(id: cid)

            case .getValue:
                /// .findValue
                ///In the request, key is an unstructured array of bytes.
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }
                return Query.getValue(key: [UInt8](dht.key))

            case .putValue:
                /// .store
                /// In the request, record is set to the record to be stored and key on Message is set to equal key of the Record.
                let rec = try DHT.Record(contiguousBytes: dht.record)
                guard rec.hasValue, rec.hasKey, !rec.value.isEmpty, !rec.key.isEmpty, dht.key == rec.key else {
                    throw Errors.DecodingErrorInvalidType
                }
                //let providers = try dht.providerPeers.map { try $0.toPeerInfo() }
                return Query.putValue(key: [UInt8](dht.key), record: rec)

            case .getProviders:
                /// In the request, key is set to a CID.
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }
                return Query.getProviders(cid: [UInt8](dht.key))

            case .addProvider:
                /// In the request, key is set to a CID.
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }
                return Query.addProvider(cid: [UInt8](dht.key))

            case .ping:
                /// .ping (deprecated)
                return Query.ping

            default:
                throw Errors.DecodingErrorInvalidType
            }
        }

        var description: String {
            switch self {
            case .findNode(let peerID):
                return "Query::FindNode(peerID: \(peerID.b58String))"
            case .getValue(let key):
                return "Query::GetValue(key: \(KadDHT.keyToHumanReadableString(key)))"
            case .putValue(let key, let record):
                return "Query::PutValue(key: \(KadDHT.keyToHumanReadableString(key)), record: \(record))"
            case .getProviders(let cid):
                return "Query::GetProviders(cid: \(KadDHT.keyToHumanReadableString(cid)))"
            case .addProvider(let cid):
                return "Query::AddProviders(cid: \(KadDHT.keyToHumanReadableString(cid)))"
            case .ping:
                return "Query::PING"
            }
        }
    }
}
