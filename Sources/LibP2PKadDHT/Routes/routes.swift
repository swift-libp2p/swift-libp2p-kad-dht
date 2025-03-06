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
func registerDHTRoute(_ app:Application) throws {
    app.group("ipfs", "kad") { kad in

        kad.on("1.0.0", handlers: []) { req -> EventLoopFuture<Response<ByteBuffer>> in

            return req.application.dht.kadDHT.processRequest(req)

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

