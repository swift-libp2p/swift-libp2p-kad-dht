import XCTest
import LibP2P
import LibP2PNoise
import LibP2PMPLEX
@testable import LibP2PKadDHT

class LibP2PKadDHTTests: XCTestCase {

    override func setUpWithError() throws {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDownWithError() throws {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testExample() throws {
        // This is an example of a functional test case.
        // Use XCTAssert and related functions to verify your tests produce the correct results.
        // Any test you write for XCTest can be annotated as throws and async.
        // Mark your test throws to produce an unexpected failure when your test encounters an uncaught error.
        // Mark your test async to allow awaiting for asynchronous code to complete. Check the results with assertions afterwards.
    }

    func testPerformanceExample() throws {
        // This is an example of a performance test case.
        self.measure {
            // Put the code you want to measure the time of here.
        }
    }
    
    /// **************************************
    ///    Testing Internal KadDHT - Single Heartbeat
    /// **************************************
    ///
    /// This test manually triggers one KadDHT heartbeat that kicks off a FindNode lookup for our libp2p PeerID
    /// A heartbeat / node lookup takes about 22 secs and discovers about 21 peers...
    /// 2 heartbeats --> Time:  64 seconds,  Mem: 12.5 --> 13.8,  CPU: 0-12%,  Peers: 21
    /// 3 heartbeats --> Time:  59 seconds,  Mem: 12.5 --> 14.5,  CPU: 0-12%,  Peers: 27
    func testLibP2PKadDHT_SingleHeartbeat() throws {
        /// Init the libp2p node
        let lib = try makeHost()
        
        /// Prepare our expectations
        //let expectationNode1ReceivedNode2Subscription = expectation(description: "Node1 received fruit subscription from Node2")
        
        /// Start the node
        try lib.start()
                
        /// Do your test stuff ...
        XCTAssertTrue(lib.dht.kadDHT.state == .started)
        
        //let exp = expectation(description: "Wait for response")
        print("*** Before Lookup ***")
        print(lib.dht.kadDHT.peerstore)
        print("")
        
        print("*** Before Lookup ***")
        lib.peers.dumpAll()
        print("")
        
        try (0..<1).forEach { _ in
            /// Trigger a heartbeat (which will perform a peer lookup for our peerID)
            try lib.dht.kadDHT.heartbeat().wait()

            sleep(2)
        }
                
        print("*** After Lookup ***")
        print("(DHT Peerstore: \(lib.dht.kadDHT.peerstore.count)) - \(lib.dht.kadDHT.peerstore)")
        print("")
       
        print("*** After Lookup ***")
        let pAll = try lib.peers.all().wait()
        print("(Libp2p Peerstore: \(pAll.count)) - \(pAll.map { "\($0.id)\nMultiaddr: [\($0.addresses.map { $0.description }.joined(separator: ", "))]\nProtocols: [\($0.protocols.map { $0.stringValue }.joined(separator: ", "))]\nMetadata: \($0.metadata.map { "\($0.key): \(String(data: Data($0.value), encoding: .utf8) ?? "NIL")" }.joined(separator: ", "))" }.joined(separator: "\n\n"))")
        
        print("")
        lib.peers.dumpAll()
        print("")
        
        print("Connections: ")
        print(try lib.connections.getConnections(on: nil).wait())
        
        print("*** Metrics ***")
        lib.dht.kadDHT.metrics.history.forEach { print($0.event) }
        
        print("*** Routing Table ***")
        print(lib.dht.kadDHT.routingTable)
        
        //waitForExpectations(timeout: 10, handler: nil)
        sleep(2)
        
        /// Stop the node
        lib.shutdown()
        
        print("All Done!")
    }
    
    func testLibP2PKadDHT_SingleHeartbeat_Topology() throws {
        /// Init the libp2p node
        let lib = try makeHost()
        
        /// Prepare our expectations
        //let expectationNode1ReceivedNode2Subscription = expectation(description: "Node1 received fruit subscription from Node2")
        
        /// Start the node
        try lib.start()
                
        /// Do your test stuff ...
        XCTAssertTrue(lib.dht.kadDHT.state == .started)
        
        lib.topology.register(TopologyRegistration(protocol: "/floodsub/1.0.0", handler: TopologyHandler(onConnect: { peerID, conn in
            print("⭐️ Found a /floodsub/1.0.0 \(peerID)")
        })))
        
        //let exp = expectation(description: "Wait for response")
        print("*** Before Lookup ***")
        print(lib.dht.kadDHT.peerstore)
        print("")
        
        print("*** Before Lookup ***")
        lib.peers.dumpAll()
        print("")
        
        try (0..<1).forEach { _ in
            /// Trigger a heartbeat (which will perform a peer lookup for our peerID)
            try lib.dht.kadDHT.heartbeat().wait()

            sleep(2)
        }
                
        print("*** After Lookup ***")
        print("(DHT Peerstore: \(lib.dht.kadDHT.peerstore.count)) - \(lib.dht.kadDHT.peerstore)")
        print("")
       
        print("*** After Lookup ***")
        let pAll = try lib.peers.all().wait()
        print("(Libp2p Peerstore: \(pAll.count)) - \(pAll.map { "\($0.id)\nMultiaddr: [\($0.addresses.map { $0.description }.joined(separator: ", "))]\nProtocols: [\($0.protocols.map { $0.stringValue }.joined(separator: ", "))]\nMetadata: \($0.metadata.map { "\($0.key): \(String(data: Data($0.value), encoding: .utf8) ?? "NIL")" }.joined(separator: ", "))" }.joined(separator: "\n\n"))")
        
        print("")
        lib.peers.dumpAll()
        print("")
        
        print("Connections: ")
        print(try lib.connections.getConnections(on: nil).wait())
        
        print("*** Metrics ***")
        lib.dht.kadDHT.metrics.history.forEach { print($0.event) }
        
        print("*** Routing Table ***")
        print(lib.dht.kadDHT.routingTable)
        
        //waitForExpectations(timeout: 10, handler: nil)
        sleep(2)
        
        /// Stop the node
        lib.shutdown()
        
        print("All Done!")
    }
    
    var nextPort:Int = 10000
    private func makeHost() throws -> Application {
        let lib = try Application(.testing, peerID: PeerID(.Ed25519))
        lib.security.use(.noise)
        lib.muxers.use(.mplex)
        lib.dht.use(.kadDHT(mode: .client, options: .default, autoUpdate: false))
        lib.servers.use(.tcp(host: "127.0.0.1", port: nextPort))
        
        nextPort += 1
        
        lib.logger.logLevel = .debug
        
        return lib
    }

}
