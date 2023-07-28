//
//  Network+Response.swift
//
//
//  Created by Brandon Toms on 7/24/23.
//

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

        func encode() throws -> [UInt8] {
            var dht = DHT.Message()

            switch self {
                case let .findNode(closerPeers):
                    dht.type = .findNode
                    //dht.key = Data(id)
                    dht.closerPeers = closerPeers

                case let .getValue(key, record, closerPeers):
                    dht.type = .getValue
                    dht.key = Data(key)
                    if let record = record {
                        dht.record = try record.serializedData()
                    }
                    /// Should we only set this if record is nil? Do we set it even if closerPeers is empty?
                    dht.closerPeers = closerPeers

                case let .putValue(key, record):
                    dht.type = .putValue
                    dht.key = Data(key)
                    if let record = record {
                        dht.record = try record.serializedData()
                    }

                case let .getProviders(cid, providerPeers, closerPeers):
                    dht.type = .getProviders
                    dht.key = Data(cid)
                    dht.providerPeers = providerPeers
                    dht.closerPeers = closerPeers

                case let .addProvider(cid, providerPeers):
                    dht.type = .addProvider
                    dht.key = Data(cid)
                    dht.providerPeers = providerPeers

                case .ping:
                    dht.type = .ping
            }

            /// Serialize the DHT.Message
            let payload = try dht.serializedData()

            /// add the uVarInt length prefix
            return putUVarInt(UInt64(payload.count)) + payload
        }

        static func decode(_ bytes: [UInt8]) throws -> Response {
            let prefix = uVarInt(bytes)
            guard prefix.value > 0, prefix.value == (bytes.count - prefix.bytesRead) else {
                print("Failed to decode bytes: \(bytes.toHexString())")
                print("Prefix Value: \(prefix.value)")
                print("Bytes Counts: \(bytes.count)")
                throw Errors.DecodingErrorInvalidLength
            }
            let payload: [UInt8] = Array<UInt8>(bytes.dropFirst(prefix.bytesRead))

            guard let dht = try? DHT.Message(contiguousBytes: payload) else { throw Errors.DecodingErrorInvalidType }

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

                    let rec: DHT.Record?
                    if dht.hasRecord {
                        rec = try DHT.Record(contiguousBytes: dht.record)
                    } else {
                        rec = nil
                    }

                    return Response.getValue(key: Array<UInt8>(dht.key), record: rec, closerPeers: dht.closerPeers)

                case .putValue:
                    /// In the response the target node validates record, and if it is valid, it stores it in the datastore and as a response echoes the request.
                    guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }

                    let rec: DHT.Record?
                    if dht.hasRecord {
                        rec = try DHT.Record(contiguousBytes: dht.record)
                    } else {
                        rec = nil
                    }

                    return Response.putValue(key: Array<UInt8>(dht.key), record: rec)

                case .getProviders:
                    /// In the response the target node returns the closest known providerPeers (if any) and the k closest known closerPeers.
                    guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }

                    return Response.getProviders(cid: Array<UInt8>(dht.key), providerPeers: dht.providerPeers, closerPeers: dht.closerPeers)

                case .addProvider:
                    /// Do we receive a response from addProvider? Is it the list of providerPeers??
                    guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }

                    return Response.addProvider(cid: Array<UInt8>(dht.key), providerPeers: dht.providerPeers)

                case .ping:
                    /// Deprecated...
                    if dht.hasKey {
                        let tic = UInt64(littleEndian: dht.key.withUnsafeBytes({ $0.pointee }))
                        print("Ping took \((DispatchTime.now().uptimeNanoseconds - tic) / 1_000_000)ms")
                    }
                    return Response.ping
                //throw Errors.DecodingErrorInvalidType

                default:
                    throw Errors.DecodingErrorInvalidType
            }
        }
    }
}
