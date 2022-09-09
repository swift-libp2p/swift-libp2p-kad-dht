//
//  Networking.swift
//  
//
//  Created by Brandon Toms on 4/29/22.
//

import LibP2P

extension KadDHT {
    
    struct DHTNodeMetrics {
        var history:[(date:TimeInterval, event:DHTEvent)] = []
        
        mutating func add(event:DHTEvent) {
            self.history.append((Date().timeIntervalSince1970, event))
        }
    }

    enum DHTEvent {
        case initialized
        case peerDiscovered(PeerInfo)
        case dialedPeer(Multiaddr, Bool)
        case addedPeer(PeerInfo)
        case droppedPeer(PeerInfo, DHTDropPeerReason)
        case queriedPeer(PeerInfo, DHTQuery)
        case queryResponse(PeerInfo, DHTResponse)
        case deinitialized
    }

    enum DHTDropPeerReason {
        case closerPeerFound
        case maxLatencyExceeded
        case brokenConnection
        case failedToAdd
    }
    
    enum Errors:Error {
        case AttemptedToStoreNonCodableValue
        case DecodingErrorInvalidLength
        case DecodingErrorInvalidType
        case connectionDropped
        case connectionTimedOut
        case unknownPeer
        case noCloserPeers
        case encodingError
        case noNetwork
        case maxLookupDepthExceeded
        case lookupPeersExhausted
        case alreadyPerformingLookup
        case cannotCallHeartbeatWhileNodeIsInAutoUpdateMode
        case noDialableAddressesForPeer
        case clientModeDoesNotAcceptInboundTraffic
        case cantPutValueWithoutExternallyDialableAddress
        case peerIDMultiaddrEncapsulationFailed
    }

    public struct DHTNodeOptions {
        let connectionTimeout:TimeAmount
        let maxConcurrentConnections:Int
        let bucketSize:Int
        let maxPeers:Int
        let maxKeyValueStoreSize:Int
        
        init(connectionTimeout:TimeAmount = .seconds(4), maxConcurrentConnections:Int = 4, bucketSize:Int = 20, maxPeers:Int = 100, maxKeyValueStoreEntries:Int = 100) {
            self.connectionTimeout = connectionTimeout
            self.maxConcurrentConnections = maxConcurrentConnections
            self.bucketSize = bucketSize
            self.maxPeers = maxPeers
            self.maxKeyValueStoreSize = maxKeyValueStoreEntries
        }
        
        static var `default`:DHTNodeOptions {
            return .init()
        }
    }
    
    enum DHTQuery {
        /// In the request key must be set to the binary PeerId of the node to be found
        case findNode(id:PeerID)
        /// In the request key is an unstructured array of bytes.
        case getValue(key:[UInt8])
        /// In the request record is set to the record to be stored and key on Message is set to equal key of the Record.
        case putValue(key:[UInt8], record:DHT.Record)
        /// In the request key is set to a CID.
        case getProviders(cid:[UInt8])
        /// In the request key is set to a CID.
        case addProvider(cid:[UInt8])
        /// Deprecated message type replaced by the dedicated ping protocol. Implementations may still handle incoming PING requests for backwards compatibility. Implementations must not actively send PING requests.
        case ping // Deprecated
        

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
            
            let payload = try Array<UInt8>(req.serializedData())
            //return payload
            return putUVarInt(UInt64(payload.count)) + payload
        }
        
        /// This is someone sending our node a query, the remote peer is the initiator, we're just reacting...
        static func decode(_ bytes:[UInt8]) throws -> DHTQuery {
            let prefix = uVarInt(bytes)
            guard prefix.value > 0, prefix.value == (bytes.count - prefix.bytesRead) else { throw Errors.DecodingErrorInvalidLength }
            let payload:[UInt8] = Array<UInt8>(bytes.dropFirst(prefix.bytesRead))
            
            guard let dht = try? DHT.Message(contiguousBytes: payload) else { throw Errors.DecodingErrorInvalidType }
            
            switch dht.type {
            case .findNode: /// .findNode
                /// In the request, key must be set to the binary PeerId of the node to be found
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }
                guard let cid = try? PeerID(fromBytesID: Array<UInt8>(dht.key)) else {
                    throw Errors.DecodingErrorInvalidType
                }
                return DHTQuery.findNode(id: cid)
                
                
            case .getValue: /// .findValue
                ///In the request, key is an unstructured array of bytes.
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }
                return DHTQuery.getValue(key: Array<UInt8>(dht.key))
                
                
            case .putValue: /// .store
                /// In the request, record is set to the record to be stored and key on Message is set to equal key of the Record.
                let rec = try DHT.Record(contiguousBytes: dht.record)
                guard rec.hasValue, rec.hasKey, !rec.value.isEmpty, !rec.key.isEmpty, dht.key == rec.key else { throw Errors.DecodingErrorInvalidType }
                //let providers = try dht.providerPeers.map { try $0.toPeerInfo() }
                return DHTQuery.putValue(key: Array<UInt8>(dht.key), record: rec)
                
                
            case .getProviders:
                /// In the request, key is set to a CID.
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }
                return DHTQuery.getProviders(cid: Array<UInt8>(dht.key))
                
                
            case .addProvider:
                /// In the request, key is set to a CID.
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }
                return DHTQuery.addProvider(cid: Array<UInt8>(dht.key))
                
                
            case .ping: /// .ping (deprecated)
                return DHTQuery.ping
                
                
            default:
                throw Errors.DecodingErrorInvalidType
            }
        }
    }
    
    enum DHTResponse {
        /// In the response closerPeers is set to the k closest Peers.
        case findNode(closerPeers:[DHT.Message.Peer])
        /// In the response the record is set to the value for the given key (if found in the datastore) and closerPeers is set to the k closest peers.
        case getValue(key:[UInt8], record:DHT.Record?, closerPeers:[DHT.Message.Peer])
        /// In the response the target node validates record, and if it is valid, it stores it in the datastore and as a response echoes the request.
        case putValue(key:[UInt8], record:DHT.Record?)
        /// In the response the target node returns the closest known providerPeers (if any) and the k closest known closerPeers.
        case getProviders(cid:[UInt8], providerPeers:[DHT.Message.Peer], closerPeers:[DHT.Message.Peer])
        /// Do we receive a response from addProvider? Is it the list of providerPeers??
        case addProvider(cid:[UInt8], providerPeers:[DHT.Message.Peer])
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
        
        static func decode(_ bytes:[UInt8]) throws -> DHTResponse {
            let prefix = uVarInt(bytes)
            guard prefix.value > 0, prefix.value == (bytes.count - prefix.bytesRead) else { throw Errors.DecodingErrorInvalidLength }
            let payload:[UInt8] = Array<UInt8>(bytes.dropFirst(prefix.bytesRead))
            
            guard let dht = try? DHT.Message(contiguousBytes: payload) else { throw Errors.DecodingErrorInvalidType }
            
            //print(dht)
            
            switch dht.type {
            case .findNode:
                /// In the response closerPeers is set to the k closest Peers.
                //guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }
                //let id = try PeerID(fromBytesID: Array<UInt8>(dht.key))
                return DHTResponse.findNode(closerPeers: dht.closerPeers)
            
            
            case .getValue:
                /// In the response the record is set to the value for the given key (if found in the datastore) and closerPeers is set to the k closest peers.
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }
                
                let rec:DHT.Record?
                if dht.hasRecord {
                    rec = try DHT.Record(contiguousBytes: dht.record)
                } else {
                    rec = nil
                }
                                
                return DHTResponse.getValue(key: Array<UInt8>(dht.key), record: rec, closerPeers: dht.closerPeers)
                
            
            case .putValue:
                /// In the response the target node validates record, and if it is valid, it stores it in the datastore and as a response echoes the request.
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }
                
                let rec:DHT.Record?
                if dht.hasRecord {
                    rec = try DHT.Record(contiguousBytes: dht.record)
                } else {
                    rec = nil
                }
                
                return DHTResponse.putValue(key: Array<UInt8>(dht.key), record: rec)
                
            
            case .getProviders:
                /// In the response the target node returns the closest known providerPeers (if any) and the k closest known closerPeers.
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }
                
                return DHTResponse.getProviders(cid: Array<UInt8>(dht.key), providerPeers: dht.providerPeers, closerPeers: dht.closerPeers)
            
                
            case .addProvider:
                /// Do we receive a response from addProvider? Is it the list of providerPeers??
                guard dht.hasKey, !dht.key.isEmpty else { throw Errors.DecodingErrorInvalidType }
                
                return DHTResponse.addProvider(cid: Array<UInt8>(dht.key), providerPeers: dht.providerPeers)
                
            case .ping:
                /// Deprecated...
                if dht.hasKey {
                    let tic = UInt64(littleEndian: dht.key.withUnsafeBytes { $0.pointee })
                    print("Ping took \((DispatchTime.now().uptimeNanoseconds - tic) / 1_000_000)ms")
                }
                return DHTResponse.ping
                //throw Errors.DecodingErrorInvalidType
                
            default:
                throw Errors.DecodingErrorInvalidType
            }
        }
    }
}
