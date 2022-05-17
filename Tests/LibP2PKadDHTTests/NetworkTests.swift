//
//  NetworkTests.swift
//  
//
//  Created by Brandon Toms on 4/30/22.
//

import XCTest
import LibP2P
import CryptoSwift
import LibP2PCrypto
@testable import LibP2PKadDHT

class NetworkTests: XCTestCase {

    override func setUpWithError() throws {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDownWithError() throws {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }
    
    func testDHTFauxNetworkQuery_Ping() throws {
        /// Test Query.ping and Response.ping
        let query = KadDHT.DHTQuery.ping
        let encodedQuery = try query.encode()

        /// Send it over the wire...

        let decodedQuery = try KadDHT.DHTQuery.decode(encodedQuery)
        print("Recovered Query: \(decodedQuery)")

        guard case .ping = decodedQuery else { return XCTFail() }

        let response = KadDHT.DHTResponse.ping
        let encodedResponse = try response.encode()

        /// Send the response back over the wire...

        let decodedResponse = try KadDHT.DHTResponse.decode(encodedResponse)

        guard case .ping = decodedResponse else { return XCTFail() }
    }

    func testDHTFauxNetworkQuery_Store_Success() throws {
        /// Test Query.store and Response.stored
        let thingToStore:(key:String, value:String) = (key: "test", value: "Kademlia DHT")
        let query = KadDHT.DHTQuery.putValue(key: thingToStore.key.bytes, record: DHT.Record())
        let encodedQuery = try query.encode()

        /// Send it over the wire...

        let decodedQuery = try KadDHT.DHTQuery.decode(encodedQuery)
        print("Recovered Query: \(decodedQuery)")

        guard case .putValue(let qKey, let qRecord) = decodedQuery else { return XCTFail() }

        /// Assume we can store the value
        print("Storing Value: '\(qRecord)' for Key: '\(qKey)'")
        let response = KadDHT.DHTResponse.putValue(key: qKey, record: qRecord)
        let encodedResponse = try response.encode()

        /// Send the response back over the wire...

        let decodedResponse = try KadDHT.DHTResponse.decode(encodedResponse)

        guard case .putValue(let key, let record) = decodedResponse else { return XCTFail() }

        XCTAssertEqual(key, thingToStore.key.bytes)
        XCTAssertEqual(record, DHT.Record())
    }

//    func testDHTFauxNetworkQuery_Store_Failure() throws {
//        /// Test Query.store and Response.stored
//        let thingToStore:(key:String, value:String) = (key: "test", value: "Kademlia DHT")
//        let query = KadDHT.Query.store(key: thingToStore.key, value: thingToStore.value)
//        let encodedQuery = try query.encode()
//
//        /// Send it over the wire...
//
//        let decodedQuery = try KadDHT.Query.decode(encodedQuery)
//        print("Recovered Query: \(decodedQuery)")
//
//        guard case .store(let key, let value) = decodedQuery else { return XCTFail() }
//
//        /// Assume we can't store the value for some reason
//        print("Failed to store Value: '\(value)' for Key: '\(key)'")
//        let response = KadDHT.Response.stored(false)
//        let encodedResponse = try response.encode()
//
//        /// Send the response back over the wire...
//
//        let decodedResponse = try KadDHT.Response.decode(encodedResponse)
//
//        guard case .stored(let wasStored) = decodedResponse else { return XCTFail() }
//
//        XCTAssertEqual(wasStored, false)
//    }
//
//    func testDHTFauxNetworkQuery_FindNode() throws {
//        let testAddresses = try (0..<10).map { i -> Multiaddr in
//            let pid = try PeerID(.Ed25519)
//            return try Multiaddr("/ip4/127.0.0.1/tcp/\(1000 + i)/p2p/\(pid.b58String)")
//        }
//
//        try testAddresses.forEach  {
//            guard let cid = $0.getPeerID() else { return XCTFail("Failed to extract PeerID from multiaddress") }
//            XCTAssertNoThrow(try PeerID(cid: cid))
//        }
//
//        /// Test Query.findNode and Response.nodeSearch
//        let query = KadDHT.Query.findNode(testAddresses.first!)
//        let encodedQuery = try query.encode()
//
//        /// Send it over the wire...
//
//        let decodedQuery = try KadDHT.Query.decode(encodedQuery)
//        print("Recovered Query: \(decodedQuery)")
//
//        guard case .findNode(let ma) = decodedQuery else { return XCTFail() }
//
//        let response = KadDHT.Response.nodeSearch(peers: testAddresses.filter({ $0 != ma }))
//        let encodedResponse = try response.encode()
//
//        /// Send the response back over the wire...
//
//        let decodedResponse = try KadDHT.Response.decode(encodedResponse)
//
//        guard case .nodeSearch(let peers) = decodedResponse else { return XCTFail() }
//
//        print(peers)
//
//        XCTAssertEqual(peers.count, testAddresses.count - 1)
//        XCTAssertFalse(peers.contains(where: { $0 == ma } ))
//    }
//
//    func testDHTFauxNetworkQuery_FindValue_NoValue() throws {
//
//        /// Test Query.findValue and Response.keySearch
//        let query = KadDHT.Query.findValue(key: "test")
//        let encodedQuery = try query.encode()
//
//        /// Send it over the wire...
//
//        let decodedQuery = try KadDHT.Query.decode(encodedQuery)
//        print("Recovered Query: \(decodedQuery)")
//
//        guard case .findValue(let key) = decodedQuery else { return XCTFail() }
//
//        print("Key: \(key)")
//        XCTAssertEqual(key, "test")
//        /// Assume we don't have the key their looking for, so return a set of peers that are closer...
//        let testAddresses = try (0..<3).map {
//            try Multiaddr("/ip4/127.0.0.1/tcp/\(1000 + $0)")
//        }
//
//        let response = KadDHT.Response.keySearch(found: nil, closerPeers: testAddresses)
//        let encodedResponse = try response.encode()
//
//        /// Send the response back over the wire...
//
//        let decodedResponse = try KadDHT.Response.decode(encodedResponse)
//
//        guard case .keySearch(let found, let peers) = decodedResponse else { return XCTFail() }
//
//        XCTAssertNil(found)
//        XCTAssertEqual(peers.count, testAddresses.count)
//    }
//
//    func testDHTFauxNetworkQuery_FindValue_FoundValue() throws {
//
//        /// Test Query.findValue and Response.keySearch
//        let query = KadDHT.Query.findValue(key: "test")
//        let encodedQuery = try query.encode()
//
//        /// Send it over the wire...
//
//        let decodedQuery = try KadDHT.Query.decode(encodedQuery)
//        print("Recovered Query: \(decodedQuery)")
//
//        guard case .findValue(let key) = decodedQuery else { return XCTFail() }
//
//        print("Key: \(key)")
//        XCTAssertEqual(key, "test")
//        /// Assume we have the key their looking for
//        let value = "Kademlia DHT"
//
//        let response = KadDHT.Response.keySearch(found: (key: key, value: value), closerPeers: [])
//        let encodedResponse = try response.encode()
//
//        /// Send the response back over the wire...
//
//        let decodedResponse = try KadDHT.Response.decode(encodedResponse)
//
//        guard case .keySearch(let found, let peers) = decodedResponse else { return XCTFail() }
//
//        XCTAssertNotNil(found)
//        XCTAssertEqual(found?.key, "test")
//        XCTAssertEqual(found?.value, "Kademlia DHT")
//        XCTAssertEqual(peers.count, 0)
//    }
//
//    func testDHTNetworkNode1DiscoversNode2() throws {
//
//        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
//
//        let network = KadDHT.BasicNetwork()
//
//        let node1 = KadDHT.Node(
//            eventLoop: elg.next(),
//            network: network,
//            address: try Multiaddr("/ip4/127.0.0.1/tcp/1000"),
//            peerID: try PeerID(.Ed25519),
//            bootstrapedPeers: [],
//            options: KadDHT.DHTNodeOptions(
//                connection: .gigabit,
//                connectionTimeout: .seconds(3),
//                maxConcurrentConnections: 4,
//                bucketSize: 5,
//                maxPeers: 5,
//                maxKeyValueStoreSize: 5
//            )
//        )
//
//        let node2 = KadDHT.Node(
//            eventLoop: elg.next(),
//            network: network,
//            address: try Multiaddr("/ip4/127.0.0.1/tcp/1001"),
//            peerID: try PeerID(.Ed25519),
//            bootstrapedPeers: [node1.address],
//            options: KadDHT.DHTNodeOptions(
//                connection: .gigabit,
//                connectionTimeout: .seconds(3),
//                maxConcurrentConnections: 4,
//                bucketSize: 5,
//                maxPeers: 5,
//                maxKeyValueStoreSize: 5
//            )
//        )
//
//        network.addNode(node1.address, node: node1)
//        network.addNode(node2.address, node: node2)
//
//        node1.start()
//        node2.start()
//
//        sleep(5)
//
//        node1.stop()
//        node2.stop()
//
//        print("Node 1 History")
//        node1.metrics.history.forEach { print($0) }
//        print("--------------")
//
//        print("Node 2 History")
//        node2.metrics.history.forEach { print($0) }
//        print("--------------")
//
//        XCTAssertEqual(node1.peerstore.count, 1)
//        XCTAssertEqual(node2.peerstore.count, 1)
//    }
//
//    func testDHTNetworkThreeNodesDiscoverEachOther() throws {
//
//        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
//
//        let network = KadDHT.FauxBasicNetwork()
//
//        let node1 = KadDHT.Node(
//            eventLoop: elg.next(),
//            network: network,
//            address: try Multiaddr("/ip4/127.0.0.1/tcp/1001"),
//            peerID: try PeerID(.Ed25519),
//            bootstrapedPeers: [],
//            options: KadDHT.DHTNodeOptions(
//                connection: .gigabit,
//                connectionTimeout: .seconds(3),
//                maxConcurrentConnections: 4,
//                bucketSize: 5,
//                maxPeers: 5,
//                maxKeyValueStoreSize: 5
//            )
//        )
//
//        let node2 = KadDHT.Node(
//            eventLoop: elg.next(),
//            network: network,
//            address: try Multiaddr("/ip4/127.0.0.1/tcp/1002"),
//            peerID: try PeerID(.Ed25519),
//            bootstrapedPeers: [node1.address],
//            options: KadDHT.DHTNodeOptions(
//                connection: .gigabit,
//                connectionTimeout: .seconds(3),
//                maxConcurrentConnections: 4,
//                bucketSize: 5,
//                maxPeers: 5,
//                maxKeyValueStoreSize: 5
//            )
//        )
//
//        let node3 = KadDHT.Node(
//            eventLoop: elg.next(),
//            network: network,
//            address: try Multiaddr("/ip4/127.0.0.1/tcp/1003"),
//            peerID: try PeerID(.Ed25519),
//            bootstrapedPeers: [node2.address],
//            options: KadDHT.DHTNodeOptions(
//                connection: .gigabit,
//                connectionTimeout: .seconds(3),
//                maxConcurrentConnections: 4,
//                bucketSize: 5,
//                maxPeers: 5,
//                maxKeyValueStoreSize: 5
//            )
//        )
//
//        network.addNode(node1.address, node: node1)
//        network.addNode(node2.address, node: node2)
//        network.addNode(node3.address, node: node3)
//
//        node1.start()
//        node2.start()
//        node3.start()
//
//        sleep(5)
//
//        node1.stop()
//        node2.stop()
//        node3.stop()
//
//        print("Node 1 History")
//        node1.metrics.history.forEach { print($0) }
//        print("--------------")
//
//        print("Node 2 History")
//        node2.metrics.history.forEach { print($0) }
//        print("--------------")
//
//        print("Node 3 History")
//        node3.metrics.history.forEach { print($0) }
//        print("--------------")
//
//        XCTAssertEqual(node1.peerstore.count, 3)
//        XCTAssertEqual(node2.peerstore.count, 3)
//        XCTAssertEqual(node2.peerstore.count, 3)
//    }
//
//    func testDHTNetworkMorePeersThanKBucketSize() throws {
//
//        /// Construct the EventLoopGroup that will spawn all of the eventloops that our nodes will execute on
//        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
//
//        /// Generates 10 nodes and adds them to the returned network object
//        let (network, nodes) = try generateFauxNetworkWith(10, nodesOnGroup: elg)
//
//        /// Change the log level of our first node to .info so we can track it's behavior
//        nodes.first!.logger.logLevel = .info
//
//        /// Start each node with a slight offset so their heartbeats don't all happen at the same time
//        nodes.forEach {
//            $0.start()
//            usleep(10_000)
//        }
//
//        /// Give the network 60 seconds to settle out
//        sleep(60)
//
//        /// Stop the nodes
//        nodes.forEach { $0.stop() }
//
//        /// Stopping is an async task, so lets wait for a second before beginning our prints
//        sleep(1)
//
//        /// Print out some info on each node
//        nodes.forEach {
//            print("\($0.peerID): Knows of \($0.peerstore.count) peers")
//            print($0.routingTable)
//            XCTAssertEqual(try! $0.routingTable.totalPeers().wait(), $0.maxPeers)
//        }
//
//    }
//
//    func testDHTNetworkAddPeerToExistingNetwork() throws {
//
//        /// Construct the EventLoopGroup that will spawn all of the eventloops that our nodes will execute on
//        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
//
//        /// Generates 10 nodes and adds them to the returned network object
//        var (network, nodes) = try generateFauxNetworkWith(10, nodesOnGroup: elg)
//
//        /// Change the log level of our first node to .info so we can track it's behavior
//        nodes.first!.logger.logLevel = .info
//
//        /// Remove the last node from the group, so we can add it in later... (the network doesn't know about the last node with out current linking/seeding setup)
//        let lateNode = nodes.removeLast()
//
//        /// Change the log level of our late-to-the-party node to .info so we can track it's behavior
//        lateNode.logger.logLevel = .info
//
//        /// Start each node with a slight offset so their heartbeats don't all happen at the same time
//        nodes.forEach {
//            $0.start()
//            usleep(10_000)
//        }
//
//        /// Give the network 60 seconds to settle out
//        sleep(30)
//
//        /// Add in our late-to-the-party node
//        lateNode.start()
//
//        /// Wait for the node to find peers and settle into it's location
//        sleep(30)
//
//        /// Stop the nodes
//        nodes.forEach { $0.stop() }
//        lateNode.stop()
//
//        /// Stopping is an async task, so lets wait for a second before beginning our prints
//        sleep(1)
//
//        /// Print out some info on each node
//        nodes.forEach {
//            print("\($0.peerID): Knows of \($0.peerstore.count) peers")
//            print($0.routingTable)
//            XCTAssertGreaterThanOrEqual(try! $0.routingTable.totalPeers().wait(), $0.routingTable.bucketSize)
//        }
//        print("\(lateNode.peerID): Knows of \(lateNode.peerstore.count) peers")
//        print(lateNode.routingTable)
//        XCTAssertGreaterThanOrEqual(try! lateNode.routingTable.totalPeers().wait(), lateNode.routingTable.bucketSize)
//    }
//
//    func testDHTNetworkAddKeyToExistingNetwork() throws {
//
//        /// Construct the EventLoopGroup that will spawn all of the eventloops that our nodes will execute on
//        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
//
//        /// Generates 10 nodes and adds them to the returned network object
//        var (network, nodes) = try generateFauxNetworkWith(10, nodesOnGroup: elg)
//
//        /// Remove the last node from the group, so we can add it in later... (the network doesn't know about the last node with out current linking/seeding setup)
//        let firstNode = nodes.removeFirst()
//        firstNode.logger.logLevel = .info
//
//        /// Start each node with a slight offset so their heartbeats don't all happen at the same time
//        firstNode.start()
//        nodes.forEach {
//            $0.start()
//            usleep(10_000)
//        }
//
//        /// Give the network 30 seconds to settle out
//        sleep(10)
//
//        print(try firstNode.store("test", value: "123").wait())
//
//        /// Give the network some time to sort the new values
//        sleep(2)
//
//        /// Make sure each node in the network can access the key:value pair
//        try nodes.forEach {
//            let value = try $0.getWithTrace("test").wait()
//            print(value)
//            XCTAssertEqual(value.0, "123")
//        }
//
//        /// Stop the nodes
//        nodes.forEach { $0.stop() }
//        firstNode.stop()
//
//        /// Stopping is an async task, so lets wait for a second before beginning our prints
//        sleep(1)
//
//        /// Print out some info on each node
//        nodes.forEach {
//            print("\($0.peerID): Knows of \($0.peerstore.count) peers")
//            print($0.routingTable)
//            XCTAssertGreaterThanOrEqual(try! $0.routingTable.totalPeers().wait(), $0.routingTable.bucketSize)
//        }
//        print("\(firstNode.peerID): Knows of \(firstNode.peerstore.count) peers")
//        print(firstNode.routingTable)
//        XCTAssertGreaterThanOrEqual(try! firstNode.routingTable.totalPeers().wait(), firstNode.routingTable.bucketSize)
//    }
//
//    /// Replacement Strategies (16 Nodes, Bucket Size == 4, 10 KVs in through 1 node)
//    /// OldestPeer => 7 Nodes with 0 KV Pairs, 14 Lookup Errors
//    /// FurtherThanReplacement => 10 Nodes with 0 KV Pairs (Hard Split), 100 Lookup Errors
//    /// FurthestPeer => 7 Nodes with 0 KV Pairs (mostly middle), 62 Lookup Errors
//    /// AnyReplaceable => 7 Nodes with 0 KV Pairs (spread out), 1 Lookup Error
//    /// 42 LookupErrors with Trace and old store()
//    func testDHTNetworkAddABunchOfKeysToExistingNetwork() throws {
//
//        /// Construct the EventLoopGroup that will spawn all of the eventloops that our nodes will execute on
//        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
//
//        /// Generates 10 nodes and adds them to the returned network object
//        var (network, nodes) = try generateFauxNetworkWith(10, nodesOnGroup: elg, bucketSize: 4, maxPeers: 5, dhtSize: 5)
//
//        /// List the nodes in their current order (random)
//        print( nodes.map { $0.peerID.description }.joined(separator: " | "))
//        /// List the nodes in their absolute kad order
//        print( nodes.sortedAbsolutely().map { $0.peerID.description }.joined(separator: " | "))
//        /// For each node, find the 3 closest peers to them
//        nodes.forEach { node in
//            print("Node: \(node.peerID) -> \(nodes.sorted(closestTo: node).dropFirst().prefix(4).map { $0.peerID.description }.joined(separator: ", "))")
//        }
//
//        /// Remove the last node from the group, so we can add it in later... (the network doesn't know about the last node with out current linking/seeding setup)
//        let firstNode = nodes.removeFirst()
//        print("The chosen one: \(firstNode.peerID)")
//        firstNode.logger.logLevel = .warning
//        //firstNode.theChosenOne = false
//
//        //nodes.forEach { $0.theChosenOne = false }
//
//        /// Start each node with a slight offset so their heartbeats don't all happen at the same time
//        nodes.forEach {
//            $0.start()
//            usleep(10_000)
//        }
//        firstNode.start()
//
//        /// Give the network 30 seconds to settle out
//        (0..<10).forEach {
//            sleep(3)
//            let seconds = $0 * 3
//            if seconds > 10 { print("\(seconds)s - ", terminator: "")}
//            else { print(" \(seconds)s - ", terminator: "") }
//            printNormalizedCurrentOrderingEstimate(nodes + [firstNode])
//        }
//
//        /// For each node, find the 3 closest peers to them
//        try nodes.forEach { node in
//            print("Node: \(node.peerID) -> \(try node.routingTable.getPeerInfos().wait().map({ $0.id }).sorted(closestTo: node.peerID).prefix(4).map { $0.description }.joined(separator: ", "))")
//        }
//
//        let kvs = [
//            "test1": "🌭",
//            "test2": "🍔",
//            "test3": "🍕",
//            "test4": "🧀",
//            "test5": "🥞",
//            "test6": "🥧",
//            "test7": "🍍",
//            "test8": "🍇",
//            "test9": "🍎",
//            "test10": "🌯",
//        ]
//
//        /// Add a couple key:value's to the dht (through the first node)
////        for (key, value) in kvs {
////            XCTAssertTrue(try firstNode.store(key, value: value).wait())
////            sleep(1)
////        }
//
//        /// Add a couple key:value pairs to the dht (through randomly selected nodes)
//        for (key, value) in kvs {
//            XCTAssertTrue(try nodes.randomElement()!.storeNew(key, value: value).wait())
//            sleep(1)
//            //XCTAssertTrue(try nodes.randomElement()!.store(key, value: value).wait())
//            //sleep(1)
//        }
//
//        /// Give the network some time to sort the new values
//        sleep(12)
//
//        /// For KV Pair, pick a random node
//        for (key, val) in kvs {
//            /// Pick a random node and attempt to fetch the keys value
//            if let node = nodes.randomElement() {
//                do {
//                    //let value = try node.getWithTrace(key).wait()
//                    let value = try node.getUsingLookupList(key).wait()
//                    print(value ?? "NIL")
//                    XCTAssertEqual(value, val)
//                } catch {
//                    print("Lookup Error: \(error)")
//                }
//            } else {
//                XCTFail("Failed to pick random node from nodes list")
//            }
//        }
//
//        /// For each node, make sure they can access each kv pair
//        nodes.forEach { node in
//            print("Ensuring \(node.peerID) can access KVs")
//            kvs.forEach { k, v in
//                do {
//                    //let value = try node.getWithTrace(k).wait()
//                    let value = try node.getUsingLookupList(k).wait()
//                    print(value ?? "NIL")
//                    XCTAssertEqual(value, v)
//                } catch {
//                    print("Lookup Error: \(error)")
//                }
//            }
//            print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
//        }
//
//        /// Stop the nodes
//        nodes.forEach { $0.stop() }
//        firstNode.stop()
//
//        /// Stopping is an async task, so lets wait for a second before beginning our prints
//        sleep(1)
//
//        /// Print out some info on each node
//        (nodes + [firstNode]).sortedAbsolutely().forEach {
//            print("\($0.peerID): Knows of \($0.peerstore.count) peers")
//            print($0.routingTable)
//            $0.dht.forEach { print($0.value) }
//            XCTAssertGreaterThanOrEqual(try! $0.routingTable.totalPeers().wait(), $0.routingTable.bucketSize)
//            print("\n")
//        }
//
//        /// Print the KVs in absolute ordering
//        print( kvs.sorted(by: { lhs, rhs in
//            KadDHT.Key.ZeroKey.compareDistancesFromSelf(to: KadDHT.Key(lhs.key.bytes, keySpace: .xor), and: KadDHT.Key(rhs.key.bytes, keySpace: .xor)) == 1
//        }).map({ "       \($0.value)       " }).joined(separator: " | "))
//        print( String(repeating: "-", count: 18 * (nodes.count + 1) + 9) )
//        /// Print the DHTs for each peer
//        let positions = normalizedCurrentOrdering(nodes + [firstNode])
//        (0..<5).forEach { index in
//            print(positions.map { "       \($0.dht.popFirst()?.value ?? "❌")       " }.joined(separator: " | "))
//        }
//
//        /// Print Network ordering and stuff
//        printNetwork(nodes + [firstNode])
//
//        try elg.syncShutdownGracefully()
//    }
//
    /**
     Random: <peer.ID VjkkUA> | <peer.ID c6pwnA> | <peer.ID ZaJryd> | <peer.ID XLgSQp> | <peer.ID dDYwpQ> | <peer.ID WZJsdG> | <peer.ID RHANJy> | <peer.ID QFqTsP> | <peer.ID dtgxUa> | <peer.ID cjyW2u>
     Absolute: <peer.ID cjyW2u> | <peer.ID VjkkUA> | <peer.ID c6pwnA> | <peer.ID XLgSQp> | <peer.ID WZJsdG> | <peer.ID dDYwpQ> | <peer.ID ZaJryd> | <peer.ID RHANJy> | <peer.ID dtgxUa> | <peer.ID QFqTsP>
     Node: <peer.ID VjkkUA> -> <peer.ID c6pwnA>, <peer.ID XLgSQp>, <peer.ID cjyW2u>, <peer.ID ZaJryd>
     Node: <peer.ID c6pwnA> -> <peer.ID VjkkUA>, <peer.ID XLgSQp>, <peer.ID cjyW2u>, <peer.ID RHANJy>
     Node: <peer.ID ZaJryd> -> <peer.ID RHANJy>, <peer.ID dDYwpQ>, <peer.ID WZJsdG>, <peer.ID XLgSQp>
     Node: <peer.ID XLgSQp> -> <peer.ID VjkkUA>, <peer.ID c6pwnA>, <peer.ID cjyW2u>, <peer.ID ZaJryd>
     Node: <peer.ID dDYwpQ> -> <peer.ID WZJsdG>, <peer.ID ZaJryd>, <peer.ID RHANJy>, <peer.ID cjyW2u>
     Node: <peer.ID WZJsdG> -> <peer.ID dDYwpQ>, <peer.ID RHANJy>, <peer.ID ZaJryd>, <peer.ID cjyW2u>
     Node: <peer.ID RHANJy> -> <peer.ID ZaJryd>, <peer.ID dDYwpQ>, <peer.ID WZJsdG>, <peer.ID XLgSQp>
     Node: <peer.ID QFqTsP> -> <peer.ID dtgxUa>, <peer.ID ZaJryd>, <peer.ID RHANJy>, <peer.ID dDYwpQ>
     Node: <peer.ID dtgxUa> -> <peer.ID QFqTsP>, <peer.ID dDYwpQ>, <peer.ID WZJsdG>, <peer.ID RHANJy>
     Node: <peer.ID cjyW2u> -> <peer.ID XLgSQp>, <peer.ID c6pwnA>, <peer.ID VjkkUA>, <peer.ID dDYwpQ>

      0s - <peer.ID ZaJryd> | <peer.ID dDYwpQ> | <peer.ID XLgSQp> | <peer.ID dtgxUa> | <peer.ID WZJsdG> | <peer.ID c6pwnA> | <peer.ID RHANJy> | <peer.ID VjkkUA> | <peer.ID QFqTsP> | <peer.ID cjyW2u>
      3s - <peer.ID ZaJryd> | <peer.ID RHANJy> | <peer.ID XLgSQp> | <peer.ID c6pwnA> | <peer.ID cjyW2u> | <peer.ID VjkkUA> | <peer.ID dDYwpQ> | <peer.ID WZJsdG> | <peer.ID QFqTsP> | <peer.ID dtgxUa>
      6s - <peer.ID cjyW2u> | <peer.ID XLgSQp> | <peer.ID c6pwnA> | <peer.ID RHANJy> | <peer.ID ZaJryd> | <peer.ID VjkkUA> | <peer.ID dDYwpQ> | <peer.ID WZJsdG> | <peer.ID dtgxUa> | <peer.ID QFqTsP>
      9s - <peer.ID cjyW2u> | <peer.ID XLgSQp> | <peer.ID c6pwnA> | <peer.ID VjkkUA> | <peer.ID RHANJy> | <peer.ID ZaJryd> | <peer.ID dDYwpQ> | <peer.ID WZJsdG> | <peer.ID dtgxUa> | <peer.ID QFqTsP>
     ...
     ...
     ...
     27s - <peer.ID cjyW2u> | <peer.ID XLgSQp> | <peer.ID c6pwnA> | <peer.ID VjkkUA> | <peer.ID RHANJy> | <peer.ID ZaJryd> | <peer.ID dDYwpQ> | <peer.ID WZJsdG> | <peer.ID dtgxUa> | <peer.ID QFqTsP>

     Node: <peer.ID c6pwnA> -> <peer.ID VjkkUA>, <peer.ID XLgSQp>, <peer.ID cjyW2u>, <peer.ID RHANJy>
     Node: <peer.ID ZaJryd> -> <peer.ID RHANJy>, <peer.ID dDYwpQ>, <peer.ID WZJsdG>, <peer.ID XLgSQp>
     Node: <peer.ID XLgSQp> -> <peer.ID VjkkUA>, <peer.ID c6pwnA>, <peer.ID cjyW2u>, <peer.ID ZaJryd>
     Node: <peer.ID dDYwpQ> -> <peer.ID WZJsdG>, <peer.ID ZaJryd>, <peer.ID RHANJy>, <peer.ID cjyW2u>
     Node: <peer.ID WZJsdG> -> <peer.ID dDYwpQ>, <peer.ID RHANJy>, <peer.ID ZaJryd>, <peer.ID cjyW2u>
     Node: <peer.ID RHANJy> -> <peer.ID ZaJryd>, <peer.ID dDYwpQ>, <peer.ID WZJsdG>, <peer.ID XLgSQp>
     Node: <peer.ID QFqTsP> -> <peer.ID dtgxUa>, <peer.ID ZaJryd>, <peer.ID RHANJy>, <peer.ID dDYwpQ>
     Node: <peer.ID dtgxUa> -> <peer.ID QFqTsP>, <peer.ID dDYwpQ>, <peer.ID WZJsdG>, <peer.ID RHANJy>
     Node: <peer.ID cjyW2u> -> <peer.ID XLgSQp>, <peer.ID c6pwnA>, <peer.ID VjkkUA>, <peer.ID dDYwpQ>

     */
    ///          ⎡⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎤
    /// (Actual) | PIDX ... PIDY ... PIDZ |
    ///          |------------------------|
    /// (Sorted) | PID0 ... PID1 ... PIDN |
    ///          ⎣⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎦
    ///
    /// To determine the actual ordering of the network we can ask each node for the peers to it's left and the peers to it's right in it's dht
    private func printNetwork(_ nodes:[KadDHT.Node]) {
        printNormalizedCurrentOrderingEstimate(nodes)

        //let nodeCount = nodes.count
        let target = KadDHT.Key(preHashedBytes: Array<UInt8>(repeating: 0, count: 32))
        let sorted = nodes.sorted(by: { lhs, rhs in
            target.compareDistancesFromSelf(to: KadDHT.Key(lhs.peerID), and: KadDHT.Key(rhs.peerID)) == 1
        })
        print( sorted.map { $0.peerID.description }.joined(separator: " | ") )
        print( sorted.map { "<kad.Key \(KadDHT.Key($0.peerID).bytes.asString(base: .base58btc).prefix(6))>" }.joined(separator: " | "))
        print( sorted.map { "<kad.Key \(KadDHT.Key($0.peerID).bytes.asString(base: .base16).prefix(6))>" }.joined(separator: " | "))

        //let actualOrdering
        let farLeftNodes = nodes.filter { node in
            if let peers = try? node.routingTable.getPeerInfos().wait() {
                if let leftMostPeer = peers.sortedAbsolutely().first {
                    if KadDHT.Key(node.peerID) < leftMostPeer.dhtID {
                        return true
                    }
                }
            }
            return false
        }
        print("Nodes that are left of all their peers: \(farLeftNodes.map { $0.peerID.description }.joined(separator: ", "))")
        //print("Furthest Left Node: \(farLeftNode?.peerID.description ?? "NIL")")

        let farRightNodes = nodes.filter { node in
            if let peers = try? node.routingTable.getPeerInfos().wait() {
                if let rightMostPeer = peers.sortedAbsolutely().last {
                    if KadDHT.Key(node.peerID) > rightMostPeer.dhtID {
                        return true
                    }
                }
            }
            return false
        }

        print("Nodes that are right of all their peers: \(farRightNodes.map { $0.peerID.description }.joined(separator: ", "))")
        //print("Furthest Right Node: \(farRightNode?.peerID.description ?? "NIL")")
    }

////    func printCurrentOrderingEstimate(_ nodes:[KadDHT.FauxNode]) {
////        let actualOrdering = nodes.sorted { lhs, rhs in
////            let lhsSummation = try! lhs.routingTable.getPeerInfos().wait().reduce(Array<UInt8>(), { partialResult, peer in
////                sumByteArrays(partialResult, bytes2: peer.id.absoluteDistance())
////            })
////            let rhsSummation = try! rhs.routingTable.getPeerInfos().wait().reduce(Array<UInt8>(), { partialResult, peer in
////                sumByteArrays(partialResult, bytes2: peer.id.absoluteDistance())
////            })
////            if lhsSummation.count < rhsSummation.count {
////                return true
////            } else if lhsSummation.count > rhsSummation.count {
////                return false
////            } else {
////                return KadDHT.Key(preHashedBytes: Array<UInt8>(repeating: 0, count: lhsSummation.count)).compareDistancesFromSelf(to: KadDHT.Key(preHashedBytes: lhsSummation), and: KadDHT.Key(preHashedBytes: rhsSummation)) > 0
////            }
////        }
////
////        print(actualOrdering.map { $0.peerID.description }.joined(separator: " | ") )
////    }
//
    func normalizedCurrentOrdering(_ nodes:[KadDHT.Node]) -> [KadDHT.Node] {
        return nodes.sorted { lhs, rhs in
            let maxCommonPeers = try! min(lhs.routingTable.totalPeers().wait(), rhs.routingTable.totalPeers().wait())
            let lhsSummation = try! lhs.routingTable.getPeerInfos().wait().sortedAbsolutely().prefix(maxCommonPeers).reduce(Array<UInt8>(), { partialResult, peer in
                sumByteArrays(partialResult, bytes2: peer.id.absoluteDistance())
            })
            let rhsSummation = try! rhs.routingTable.getPeerInfos().wait().sortedAbsolutely().prefix(maxCommonPeers).reduce(Array<UInt8>(), { partialResult, peer in
                sumByteArrays(partialResult, bytes2: peer.id.absoluteDistance())
            })
            if lhsSummation.count < rhsSummation.count {
                return true
            } else if lhsSummation.count > rhsSummation.count {
                return false
            } else {
                return KadDHT.Key(preHashedBytes: Array<UInt8>(repeating: 0, count: lhsSummation.count)).compareDistancesFromSelf(to: KadDHT.Key(preHashedBytes: lhsSummation), and: KadDHT.Key(preHashedBytes: rhsSummation)) > 0
            }
        }
    }

    func printNormalizedCurrentOrderingEstimate(_ nodes:[KadDHT.Node]) {
        print(normalizedCurrentOrdering(nodes).map { $0.peerID.description }.joined(separator: " | ") )
    }

    

    
//
//    private func generateFauxNetworkWith(_ nodeCount:Int, nodesOnGroup group:EventLoopGroup, bucketSize:Int = 20, maxConcurrentConnections:Int = 4, maxPeers:Int = 10, dhtSize:Int = 5) throws -> (KadDHT.FauxBasicNetwork, [KadDHT.Node]) {
//        let network = KadDHT.FauxBasicNetwork()
//        var lastNodeMultiaddress:Multiaddr? = nil
//        let nodes = try (0..<nodeCount).map { i -> KadDHT.Node in
//            let node = KadDHT.Node(
//                eventLoop: group.next(),
//                network: network,
//                address: try Multiaddr("/ip4/127.0.0.1/tcp/\(1000 + i)"),
//                peerID: try PeerID(.Ed25519),
//                bootstrapedPeers: lastNodeMultiaddress != nil ? [lastNodeMultiaddress!] : [],
//                options: KadDHT.DHTNodeOptions(
//                    connection: .mobile(.G5),
//                    connectionTimeout: .seconds(3),
//                    maxConcurrentConnections: maxConcurrentConnections,
//                    bucketSize: bucketSize,
//                    maxPeers: maxPeers,
//                    maxKeyValueStoreSize: dhtSize
//                )
//            )
//            lastNodeMultiaddress = node.address
//            return node
//        }
//
//        /// Add the nodes to our FauxNetwork
//        nodes.forEach { network.addNode($0.address, node: $0) }
//
//        /// Set the nodes log level
//        nodes.forEach { $0.logger.logLevel = Logger.Level.warning }
//
//        return (network, nodes)
//    }
//

}
