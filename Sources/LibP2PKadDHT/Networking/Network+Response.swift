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

    enum Response {
        /// In the response closerPeers is set to the k closest Peers.
        case findNode(closerPeers: [DHT.Message.Peer])
        /// In the response the record is set to the value for the given key (if found in the datastore) and closerPeers is set to the k closest peers.
        case getValue(key: [UInt8], record: DHT.Record?, closerPeers: [DHT.Message.Peer])
        /// In the response the target node validates record, and if it is valid, it stores it in the datastore and as a response echoes the request.
        case putValue(key: [UInt8], record: DHT.Record?)
        /// In the response the target node returns the closest known providerPeers (if any) and the k closest known closerPeers.
        case getProviders(cid: [UInt8], providerPeers: [DHT.Message.Peer], closerPeers: [DHT.Message.Peer])
        /// Do we receive a response from addProvider? Is it the list of providerPeers??
        case addProvider(cid: [UInt8], providerPeers: [DHT.Message.Peer])
        /// Deprecated...
        case ping

        /// Serializes `record`, refusing anything over ``KadDHT/maxRecordSize``.
        private static func serialized(_ record: DHT.Record) throws -> Data {
            let bytes = try record.serializedData()
            guard bytes.count <= KadDHT.Defaults.maxRecordSize else {
                throw Errors.recordTooLarge(bytes: bytes.count, limit: KadDHT.Defaults.maxRecordSize)
            }
            return bytes
        }

        /// The spec's responses carry "the k closest peers", so we never put more than k on the wire.
        private static func bounded(_ peers: [DHT.Message.Peer]) -> [DHT.Message.Peer] {
            Array(peers.prefix(KadDHT.Defaults.maxPeersPerMessage))
        }

        func encode() throws -> [UInt8] {
            var dht = DHT.Message()

            switch self {
            case let .findNode(closerPeers):
                dht.type = .findNode
                //dht.key = Data(id)
                dht.closerPeers = Self.bounded(closerPeers)

            case let .getValue(key, record, closerPeers):
                dht.type = .getValue
                dht.key = Data(key)
                if let record = record {
                    dht.record = try Self.serialized(record)
                }
                /// Should we only set this if record is nil? Do we set it even if closerPeers is empty?
                dht.closerPeers = Self.bounded(closerPeers)

            case let .putValue(key, record):
                dht.type = .putValue
                dht.key = Data(key)
                if let record = record {
                    dht.record = try Self.serialized(record)
                }

            case let .getProviders(cid, providerPeers, closerPeers):
                dht.type = .getProviders
                dht.key = Data(cid)
                dht.providerPeers = Self.bounded(providerPeers)
                dht.closerPeers = Self.bounded(closerPeers)

            case let .addProvider(cid, providerPeers):
                dht.type = .addProvider
                dht.key = Data(cid)
                dht.providerPeers = Self.bounded(providerPeers)

            case .ping:
                dht.type = .ping
            }

            /// Serialize the DHT.Message
            let payload = try dht.serializedData()

            /// add the uVarInt length prefix
            return putUVarInt(UInt64(payload.count)) + payload
        }

        /// The record carried by `dht`, if any, refusing anything over ``KadDHT/maxRecordSize``.
        private static func record(in dht: DHT.Message) throws -> DHT.Record? {
            guard dht.hasRecord else { return nil }
            guard dht.record.count <= KadDHT.Defaults.maxRecordSize else {
                throw Errors.recordTooLarge(bytes: dht.record.count, limit: KadDHT.Defaults.maxRecordSize)
            }
            return try DHT.Record(serializedBytes: dht.record)
        }

        static func decode(_ bytes: [UInt8]) throws -> Response {
            let prefix = uVarInt(bytes)
            guard prefix.value > 0, prefix.value == (bytes.count - prefix.bytesRead) else {
                print("Failed to decode bytes: \(bytes.toHexString())")
                print("Prefix Value: \(prefix.value)")
                print("Bytes Counts: \(bytes.count)")
                throw Errors.DecodingErrorInvalidLength
            }
            let payload: [UInt8] = [UInt8](bytes.dropFirst(prefix.bytesRead))

            guard let dht = try? DHT.Message(serializedBytes: payload) else { throw Errors.DecodingErrorInvalidType }

            //print(dht)

            switch dht.type {
            case .findNode:
                /// In the response closerPeers is set to the k closest Peers.
                //guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }
                //let id = try PeerID(fromBytesID: Array<UInt8>(dht.key))
                return Response.findNode(closerPeers: dht.closerPeers)

            case .getValue:
                /// In the response the record is set to the value for the given key (if found in the datastore) and closerPeers is set to the k closest peers.
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }

                return Response.getValue(
                    key: [UInt8](dht.key),
                    record: try Self.record(in: dht),
                    closerPeers: dht.closerPeers
                )

            case .putValue:
                /// In the response the target node validates record, and if it is valid, it stores it in the datastore and as a response echoes the request.
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }

                return Response.putValue(key: [UInt8](dht.key), record: try Self.record(in: dht))

            case .getProviders:
                /// In the response the target node returns the closest known providerPeers (if any) and the k closest known closerPeers.
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }

                return Response.getProviders(
                    cid: [UInt8](dht.key),
                    providerPeers: dht.providerPeers,
                    closerPeers: dht.closerPeers
                )

            case .addProvider:
                /// Do we receive a response from addProvider? Is it the list of providerPeers??
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }

                return Response.addProvider(cid: [UInt8](dht.key), providerPeers: dht.providerPeers)

            case .ping:
                /// Deprecated...
                return Response.ping

            default:
                throw Errors.DecodingErrorInvalidType
            }
        }
    }
}
