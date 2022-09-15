//
//  routes.swift
//  
//
//  Created by Brandon Toms on 4/30/22.
//

import LibP2P

/// KadDHT Route Endpoint - /ipfs/kad/1.0.0
func registerDHTRoute(_ app:Application) throws {
    app.group("ipfs", "kad") { kad in
        
        kad.on("1.0.0", handlers: []) { req -> EventLoopFuture<Response<ByteBuffer>> in
            
            return req.application.dht.kadDHT.processRequest(req)
            
        }
    }
}

///// KadDHT Route Endpoint - /ipfs/kad/1.0.0
//func registerDHTRoute2(_ app:Application) throws {
//    app.group("ipfs", "kad") { kad in
//
//        kad.on("1.0.0", handlers: []) { dht in
//
//            dht.namespace("pk", validator: DHT.PubKeyValidator) { pk in
//                pk.onPutRequest { req in
//                    return req.application.dht.kadDHT.processRequest(req)
//                }
//                pk.onGetRequest { req in
//                    return req.application.dht.kadDHT.processRequest(req)
//                }
//            }
//
//            dht.namespace("ipns", validator: DHT.BaseValidator.defaultAllowAll) { ipns in
//                ipns.onPutRequest { req in
//                    return req.application.dht.kadDHT.processRequest(req)
//                }
//                ipns.onGetRequest { req in
//                    return req.application.dht.kadDHT.processRequest(req)
//                }
//            }
//
//            dht.namespace("*", validator: DHT.BaseValidator.defaultAllowAll) { wildcard in
//
//                // Log Wildcard Namespace Events if you're curious...
//
//
//            }
//
//        }
//    }
//}
