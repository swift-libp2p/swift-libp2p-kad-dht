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
        
        kad.on("1.0.0", handlers: []) { req -> EventLoopFuture<ResponseType<ByteBuffer>> in
            
            return req.application.dht.kadDHT.processRequest(req)
            
        }
    }
}
