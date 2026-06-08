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

/// KadDHT Route Endpoint - /ipfs/kad/1.0.0
func registerDHTRoute(_ app: Application) throws {
    app.group("ipfs", "kad") { kad in

        // Install a uvarint length-prefix frame decoder on the inbound
        // side of the `/ipfs/kad/1.0.0` stream. Canonical libp2p frames
        // every kad message as `uvarint(len) + protobuf`; `VarintFrameDecoder`
        // buffers across partial reads and emits exactly one complete,
        // length-prefix-stripped frame per message as `req.payload`.
        //
        // Without it the route fired per raw read and `Query.decode` assumed
        // each read was exactly one whole frame — which only held when the
        // peer flushed one message per yamux frame (swift↔swift). Peers that
        // chunk their writes differently (e.g. rust-libp2p) tripped the
        // length assertion, so we reset the stream and they saw an
        // UnexpectedEof.
        //
        // Only the inbound decoder is installed; the outbound response keeps
        // its own manual length prefix (`Response.encode`), so it is framed
        // exactly once.
        kad.on("1.0.0", handlers: [.varIntFrameDecoder]) { req -> EventLoopFuture<Response<ByteBuffer>> in

            req.application.dht.kadDHT.processRequest(req)

        }
    }
}

/// KadDHT Route Endpoint - /ipfs/kad/1.0.0
//func registerDHTRoute(_ app:Application) throws {
//    app.group("ipfs", "kad") { kad in
//
//        kad.group("1.0.0", handlers: []) { dht in
//
//            dht.namespace("pk", validator: DHT.PubKeyValidator()) { pk in
//                pk.get { req in
//                    req.logger.notice("Got a DHT request for a PK object")
//                    return req.application.dht.kadDHT.processRequest(req)
//                }
//                //pk.put { req in
//                //    req.logger.notice("Got a PUT request for a PK")
//                //    return req.application.dht.kadDHT.processPutRequest(req)
//                //}
//                //req.logger.notice("Got a DHT request for a PK")
//                //return req.application.dht.kadDHT.processRequest(req)
//            }
//
//            dht.namespace("ipns", validator: DHT.BaseValidator.AllowAll()) { ipns in
//                ipns.get { req in
//                    req.logger.notice("Got a DHT request for an IPNS object")
//                    return req.application.dht.kadDHT.processRequest(req)
//                }
//                //ipns.put { req in
//                //    req.logger.notice("Got a PUT request for an IPNS")
//                //    return req.application.dht.kadDHT.processPutRequest(req)
//                //}
//                //req.logger.notice("Got a GET request for an IPNS")
//                //return req.application.dht.kadDHT.processRequest(req)
//            }
//
//            dht.namespace("*", validator: DHT.BaseValidator.AllowAll()) { wildcard in
//            //    req.logger.notice("Got a wildcard request for `???`")
//            //    // Log Wildcard Namespace Events if you're curious...
//            }
//        }
//
//}
