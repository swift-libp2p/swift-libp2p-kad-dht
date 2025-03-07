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

import CID
import CryptoSwift
import LibP2P
import LibP2PMPLEX
import LibP2PNoise
import Multihash
import XCTest

@testable import LibP2PKadDHT

class LibP2PKadDHTTests: XCTestCase {

    override func setUpWithError() throws {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDownWithError() throws {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testEventLoopArray() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { try! group.syncShutdownGracefully() }
        let arr = EventLoopArray(String.self, on: group.next())

        for val in (0..<10) {
            let _ = group.next().submit {
                arr.append("Item[\(val)]")
                arr.append("Thing[\(val)]")
            }
        }

        sleep(1)

        XCTAssertEqual(try arr.count().wait(), 20)

        print(try arr.all().wait())

        try arr.removeAll(where: { str in
            str.contains("Thing")
        }).wait()

        XCTAssertEqual(try arr.count().wait(), 10)

        print(try arr.all().wait())

        for _ in (0..<10) {
            group.next().scheduleTask(
                in: .milliseconds(Int64.random(in: 0...100)),
                {
                    arr.remove(at: 0)
                }
            )
        }

        sleep(1)

        XCTAssertEqual(try arr.count().wait(), 0)

        print(try arr.all().wait())
    }

    func testEventLoopDictionary() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { try! group.syncShutdownGracefully() }
        let d = EventLoopDictionary<String, String>(on: group.next())

        for (idx, val) in (0..<10).enumerated() {
            let _ = group.next().submit {
                d.append(key: "Item[\(val)]", value: "Thing[\(idx)]")
                d.append(key: "Item2[\(val)]", value: "Thing2[\(idx)]")
            }
        }

        sleep(1)

        XCTAssertEqual(try d.count().wait(), 20)

        print(try d.all().wait())

        try d.removeAll(where: { key, val in
            val.contains("Thing2")
        }).wait()

        XCTAssertEqual(try d.count().wait(), 10)

        print(try d.all().wait())

        for idx in (0..<10) {
            group.next().scheduleTask(
                in: .milliseconds(Int64.random(in: 0...100)),
                {
                    d.removeValue(forKey: "Item[\(idx)]")
                }
            )
        }

        sleep(1)

        XCTAssertEqual(try d.count().wait(), 0)

        print(try d.all().wait())
    }

    func testPubKeyRecordValidator() throws {
        let dhtRecordData = Data(
            hex:
                "0a262f706b2f1220b04a57d40eca138809f139a76b12044333c3740391c9bf1ce9d8e21a79210bfd12ab04080012a60430820222300d06092a864886f70d01010105000382020f003082020a0282020100a1f5c0e7c0d5e556afc0e84566f8c565773adb548ddc219ca9688613a0096c2dfd069804c84968545b9c9df19dd131cc8408b7781df7ddfaf208a42a821523ce03955164a62dcab6bd10dd26f8507517567ca128f00a056d8636b9549ddb59ca727628775c90bd91d6251adbdfd36bf68a09c3bfe69e1b1587e8f31a4b55afc8095e7b6f6683165f9c0ef0ad1b22d8b73749ee02aa46566cd5f7a9ff6eb1099fe36b363abd4e1293108a6d473a349e77aca15e49b20ffe61b4222eb3a634e8481d71a7fdceea88a2044fa5cedde1dee314e27880bc713ca578814684e85e0d21cff40e23c341f13ee1a06452f284664999862973e51d692b578cd9b7de89d786ad6baebcf8dfc343db8eda434a15929591917c52bf16741359149d0e7092bc919928f1d5b25cb48b0f90a7a05b0eb29adca993f893c6fb137a53a5c470a8a309b574bb4fd80879bde7dcc237eaf2ce9a17b9193032df99c8bf551987561ee264a09730f9029610571625e0d0e1e2a7f90469a6a480ed08cf9b4c3af0567bfe9abf470079d8cc7d7f22efc83598f86c9e0678caf79e2299a99c47c8d057e7f3b8af40185c8dd499a1c167c358d7ab83af6581944ce0b8b6bd2cfe4bf80c8c9e7f61fe94816df79e12ae5e82c588f894b86fd599da5912f8754de2a23f2d1529845a5570a72d8d8537325b95dd3c69d9ca30b8186c20170d10955b7da216822c7302030100012a1e323032322d30392d31325431343a34363a35322e3839323937333034325a"
        )

        // Ensure we can instantiate a DHT.Record Protobuf
        let dhtRecord = try DHT.Record(contiguousBytes: dhtRecordData)

        let pubKeyRecordValidator = KadDHT.PubKeyValidator()
        XCTAssertNoThrow(try pubKeyRecordValidator.validate(key: dhtRecord.key.bytes, value: dhtRecordData.bytes))
    }

    func testPeerRecord() throws {
        let recordData = Data(
            hex:
                "080012a60430820222300d06092a864886f70d01010105000382020f003082020a0282020100a1f5c0e7c0d5e556afc0e84566f8c565773adb548ddc219ca9688613a0096c2dfd069804c84968545b9c9df19dd131cc8408b7781df7ddfaf208a42a821523ce03955164a62dcab6bd10dd26f8507517567ca128f00a056d8636b9549ddb59ca727628775c90bd91d6251adbdfd36bf68a09c3bfe69e1b1587e8f31a4b55afc8095e7b6f6683165f9c0ef0ad1b22d8b73749ee02aa46566cd5f7a9ff6eb1099fe36b363abd4e1293108a6d473a349e77aca15e49b20ffe61b4222eb3a634e8481d71a7fdceea88a2044fa5cedde1dee314e27880bc713ca578814684e85e0d21cff40e23c341f13ee1a06452f284664999862973e51d692b578cd9b7de89d786ad6baebcf8dfc343db8eda434a15929591917c52bf16741359149d0e7092bc919928f1d5b25cb48b0f90a7a05b0eb29adca993f893c6fb137a53a5c470a8a309b574bb4fd80879bde7dcc237eaf2ce9a17b9193032df99c8bf551987561ee264a09730f9029610571625e0d0e1e2a7f90469a6a480ed08cf9b4c3af0567bfe9abf470079d8cc7d7f22efc83598f86c9e0678caf79e2299a99c47c8d057e7f3b8af40185c8dd499a1c167c358d7ab83af6581944ce0b8b6bd2cfe4bf80c8c9e7f61fe94816df79e12ae5e82c588f894b86fd599da5912f8754de2a23f2d1529845a5570a72d8d8537325b95dd3c69d9ca30b8186c20170d10955b7da216822c730203010001"
        )

        let pub = try PeerID(marshaledPublicKey: recordData)
        print(pub)
        print(pub.b58String)
        print(pub.cidString)
    }

    func testExtractNamespaceFromKey() throws {
        let peerID = "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
        //let peerID = "QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"

        let key = try "/pk/".bytes + PeerID(cid: peerID).id  // or CID(...).multihash.value
        print(key)
        print("/pk/\(peerID)".bytes)

        print(self.extractNamespace(key) ?? [])

        print(String(data: Data(self.extractNamespace(key)!), encoding: .utf8) ?? "NIL")

        XCTAssertEqual(
            key,
            [
                47, 112, 107, 47, 18, 32, 176, 74, 87, 212, 14, 202, 19, 136, 9, 241, 57, 167, 107, 18, 4, 67, 51, 195,
                116, 3, 145, 201, 191, 28, 233, 216, 226, 26, 121, 33, 11, 253,
            ]
        )
    }

    func testTimeRecievedStringToDate1() throws {
        let timeReceived1 = "2022-09-12T14:46:52.892973042Z"
        let timeReceived2 = "2022-09-12T14:46:52.892973041Z"
        let timeReceived3 = "2022-09-12T14:46:52.892973043Z"
        let timeReceivedSame = "2022-09-12T14:46:52.892973042Z"

        let date1 = try RFC3339Date(string: timeReceived1)
        let date2 = try RFC3339Date(string: timeReceived2)
        let date3 = try RFC3339Date(string: timeReceived3)
        let dateSame = try RFC3339Date(string: timeReceivedSame)

        XCTAssertEqual(date1.string, timeReceived1)
        XCTAssertEqual(date2.string, timeReceived2)
        XCTAssertEqual(date3.string, timeReceived3)
        XCTAssertEqual(dateSame.string, timeReceivedSame)

        XCTAssertEqual(date1, dateSame)
        XCTAssertLessThan(date2, date1)
        XCTAssertGreaterThan(date3, date1)

        let now1 = RFC3339Date()
        print(now1.string)
        let now2 = RFC3339Date()
        print(now2.string)

        /// The RFC3339Dates will NOT be equal as long as we keep the print statements between the initializers (this is consistant with how Date() works, two immediate calls can result in equal values)
        XCTAssertLessThan(now1, now2)
        XCTAssertNotEqual(now1.string, now2.string)
    }

    /// Extracts a namespace from the front of a Key if one exists...
    ///
    /// - Note: "/" in utf8 == 47
    private func extractNamespace(_ key: [UInt8]) -> [UInt8]? {
        guard key.first == UInt8(47) else { return nil }
        guard let idx = key.dropFirst().firstIndex(of: UInt8(47)) else { return nil }
        return Array(key[1..<idx])
    }

    func testInternalNetwork_PeerRouting() throws {
        let dhtParams = KadDHT.NodeOptions(
            connectionTimeout: .milliseconds(150),
            maxConcurrentConnections: 3,
            bucketSize: 5,
            maxPeers: 15,
            maxKeyValueStoreEntries: 10,
            supportLocalNetwork: true
        )

        let group: MultiThreadedEventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { try! group.syncShutdownGracefully() }

        let node1 = try makeHost(
            mode: .server,
            options: dhtParams,
            bootstrapPeers: [],
            autoHeartbeat: false,
            usingGroup: .shared(group)
        )
        let node2 = try makeHost(
            mode: .server,
            options: dhtParams,
            bootstrapPeers: [node1.peerInfo],
            autoHeartbeat: false,
            usingGroup: .shared(group)
        )
        let node3 = try makeHost(
            mode: .server,
            options: dhtParams,
            bootstrapPeers: [node2.peerInfo],
            autoHeartbeat: false,
            usingGroup: .shared(group)
        )
        let node4 = try makeHost(
            mode: .server,
            options: dhtParams,
            bootstrapPeers: [node3.peerInfo],
            autoHeartbeat: false,
            usingGroup: .shared(group)
        )

        try node1.start()
        try node2.start()
        try node3.start()
        try node4.start()

        // Ensure Node4 can find Node1 via Node2 & Node3
        let peer = try? node4.dht.kadDHT.findPeer(peer: node1.peerID).wait()  //peerRouting.findPeer(peer: node1.peerID)
        XCTAssertNotNil(peer)
        XCTAssertEqual(peer?.peer, node1.peerID)
        XCTAssertEqual(peer?.addresses, node1.listenAddresses)

        node1.shutdown()
        node2.shutdown()
        node3.shutdown()
        node4.shutdown()

        print("All Done!")
    }

    //    func testInternalNetwork_ContentRouting() throws {
    //        let dhtParams = KadDHT.NodeOptions(connectionTimeout: .seconds(5), maxConcurrentConnections: 3, bucketSize: 5, maxPeers: 15, maxKeyValueStoreEntries: 10)
    //
    //        let node1 = try makeHost(mode: .server, options: dhtParams, bootstrapPeers: [], autoHeartbeat: false)
    //        let node2 = try makeHost(mode: .server, options: dhtParams, bootstrapPeers: [PeerInfo(peer: node1.peerID, addresses: node1.listenAddresses)], autoHeartbeat: false)
    //        let node3 = try makeHost(mode: .server, options: dhtParams, bootstrapPeers: [PeerInfo(peer: node2.peerID, addresses: node2.listenAddresses)], autoHeartbeat: false)
    //
    //        try node1.start()
    //        try node2.start()
    //        try node3.start()
    //
    //        sleep(1)
    //
    //        let provide = try node1.contentRouting.provide(CID()).wait()
    //
    //        // Ensure Node3 can find Node1
    //        let found = try node3.contentRouting.findProviders(cid: CID())
    //
    //        sleep(2)
    //
    //        beaconNode.shutdown()
    //        nodes.forEach { $0.shutdown() }
    //
    //        print("All Done!")
    //    }

    func testInternalNetwork() throws {
        throw XCTSkip("Internal Network Tests Skipped By Default")
        let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { try! group.syncShutdownGracefully() }

        let numberOfNodes = 4
        let dhtParams = KadDHT.NodeOptions(
            connectionTimeout: .milliseconds(150),
            maxConcurrentConnections: 3,
            bucketSize: 3,
            maxPeers: 8,
            maxKeyValueStoreEntries: 10,
            supportLocalNetwork: true
        )
        var nodes: [Application] = try [
            makeHost(
                mode: .server,
                options: dhtParams,
                bootstrapPeers: [],
                autoHeartbeat: false,
                usingGroup: .shared(group)
            )
        ]
        for i in 1..<numberOfNodes {
            try nodes.append(
                self.makeHost(
                    mode: .server,
                    options: dhtParams,
                    bootstrapPeers: [nodes[i - 1].peerInfo],
                    autoHeartbeat: false,
                    usingGroup: .shared(group)
                )
            )
        }

        // Register the `fruit` namespace with each node
        for node in nodes {
            try node.dht.kadDHT.handle(
                namespace: "fruit",
                validator: KadDHT.BaseValidator(
                    validationFunction: { key, value in
                        guard !key.isEmpty else { throw NSError(domain: "Invalid Fruit Message", code: 0) }
                    },
                    selectFunction: { key, values in
                        0
                    }
                )
            ).wait()
        }

        // Boot each node
        for node in nodes { try node.start() }

        // Add the last nodes info to the first node
        try nodes[0].peers.add(peerInfo: nodes.last!.peerInfo).wait()

        for node in nodes {
            try node.dht.kadDHT.heartbeat().wait()
        }

        //printNetwork(nodes.map { $0.dht.kadDHT })

        let item1Key = "/fruit/".bytes + Digest.sha256("apple".bytes)
        let item1Record = DHT.Record.with {
            $0.key = Data(item1Key)
            $0.value = Data("🍎".utf8)
        }

        let storeAttempt1 = try nodes[0].dht.kadDHT.storeNew(item1Key, value: item1Record).wait()
        XCTAssertTrue(storeAttempt1)

        for node in nodes {
            try node.dht.kadDHT.heartbeat().wait()
        }

        let storeAttempt2 = try nodes[0].dht.kadDHT.storeNew(item1Key, value: item1Record).wait()
        XCTAssertTrue(storeAttempt2)

        // Ensure the other nodes can retrieve the value
        var successes = 0
        for i in 0..<numberOfNodes {
            let getAttempt2 = try nodes[i].dht.kadDHT.getUsingLookupList(item1Key).wait()
            //print(getAttempt2 ?? "NIL")
            if getAttempt2 != nil { successes += 1 }
            XCTAssertNotNil(getAttempt2)
            XCTAssertEqual(getAttempt2?.key, Data(item1Key))
            XCTAssertEqual(getAttempt2?.value, Data("🍎".utf8))
        }
        XCTAssertEqual(
            successes,
            numberOfNodes,
            "\(numberOfNodes - successes)/\(numberOfNodes) Nodes were unable to retrieve the value"
        )

        //for i in (0..<numberOfNodes) {
        //let peerCount = try nodes[i].peers.count().wait()
        //print("Node[\(i)]::PeerCount == \(peerCount)")
        //}

        //printNetwork(nodes.map { $0.dht.kadDHT })

        for node in nodes { node.shutdown() }

        print("All Done!")
    }

    func testInternalNetwork_SortingTest() throws {
        throw XCTSkip("Internal Network Tests Skipped By Default")
        let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { try! group.syncShutdownGracefully() }

        let numberOfNodes = 20
        let dhtParams = KadDHT.NodeOptions(
            connectionTimeout: .milliseconds(150),
            maxConcurrentConnections: 3,
            bucketSize: 8,
            maxPeers: 20,
            maxKeyValueStoreEntries: 10,
            supportLocalNetwork: true
        )
        var nodes: [Application] = try [
            makeHost(
                mode: .server,
                options: dhtParams,
                bootstrapPeers: [],
                autoHeartbeat: false,
                usingGroup: .shared(group)
            )
        ]
        for i in 1..<numberOfNodes {
            try nodes.append(
                self.makeHost(
                    mode: .server,
                    options: dhtParams,
                    bootstrapPeers: [nodes[i - 1].peerInfo],
                    autoHeartbeat: false,
                    usingGroup: .shared(group)
                )
            )
        }

        // Boot each node
        for node in nodes { try node.start() }

        // Add the last nodes info to the first node
        try nodes[0].peers.add(peerInfo: nodes.last!.peerInfo).wait()

        for _ in 0..<5 {
            for node in nodes {
                try node.dht.kadDHT.heartbeat().wait()
            }

            printNetwork(nodes.map { $0.dht.kadDHT })
        }

        for i in 0..<numberOfNodes {
            let peerCount = try nodes[i].peers.count().wait()
            print("Node[\(i)]::PeerCount == \(peerCount)")
        }

        for node in nodes { node.shutdown() }

        print("All Done!")
    }

    func testInternalNetwork_Beacon() throws {
        throw XCTSkip("Internal Network Tests Skipped By Default")
        let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { try! group.syncShutdownGracefully() }

        let numberOfNodes = 4
        let dhtParams = KadDHT.NodeOptions(
            connectionTimeout: .milliseconds(150),
            maxConcurrentConnections: 3,
            bucketSize: 3,
            maxPeers: 8,
            maxKeyValueStoreEntries: 10,
            supportLocalNetwork: true
        )
        let beaconNode = try makeHost(
            mode: .server,
            options: dhtParams,
            bootstrapPeers: [],
            autoHeartbeat: false,
            usingGroup: .shared(group)
        )
        let nodes = try (0..<numberOfNodes).map { _ in
            try makeHost(
                mode: .server,
                options: dhtParams,
                bootstrapPeers: [beaconNode.peerInfo],
                autoHeartbeat: false,
                usingGroup: .shared(group)
            )
        }

        /// Register the `fruit` namespace with each node
        for node in ([beaconNode] + nodes) {
            try node.dht.kadDHT.handle(
                namespace: "fruit",
                validator: KadDHT.BaseValidator(
                    validationFunction: { key, value in
                        guard !key.isEmpty else { throw NSError(domain: "Invalid Fruit Message", code: 0) }
                    },
                    selectFunction: { key, values in
                        0
                    }
                )
            ).wait()
        }

        try beaconNode.start()
        for node in nodes { try node.start() }

        for node in nodes {
            try node.dht.kadDHT.heartbeat().wait()
        }

        try beaconNode.dht.kadDHT.heartbeat().wait()

        let item1Key = "/fruit/".bytes + Digest.sha256("apple".bytes)
        let item1Record = DHT.Record.with {
            $0.key = Data(item1Key)
            $0.value = Data("🍎".utf8)
        }

        let storeAttempt1 = try nodes[0].dht.kadDHT.storeNew(item1Key, value: item1Record).wait()
        XCTAssertTrue(storeAttempt1)

        for node in nodes {
            try node.dht.kadDHT.heartbeat().wait()
        }

        try beaconNode.dht.kadDHT.heartbeat().wait()

        let storeAttempt2 = try nodes[0].dht.kadDHT.storeNew(item1Key, value: item1Record).wait()
        XCTAssertTrue(storeAttempt2)

        // Ensure the beacon node can retrieve the value
        let getAttempt1 = try beaconNode.dht.kadDHT.getUsingLookupList(item1Key).wait()
        XCTAssertNotNil(getAttempt1)
        XCTAssertEqual(getAttempt1?.key, Data(item1Key))
        XCTAssertEqual(getAttempt1?.value, Data("🍎".utf8))

        // Ensure the other nodes can retrieve the value
        var successes = getAttempt1 == nil ? 0 : 1
        for i in 1..<numberOfNodes {
            let getAttempt2 = try nodes[i].dht.kadDHT.getUsingLookupList(item1Key).wait()
            if getAttempt2 != nil { successes += 1 }
            XCTAssertNotNil(getAttempt2)
            XCTAssertEqual(getAttempt2?.key, Data(item1Key))
            XCTAssertEqual(getAttempt2?.value, Data("🍎".utf8))
        }
        XCTAssertEqual(
            successes,
            numberOfNodes,
            "\(numberOfNodes - successes)/\(numberOfNodes) Nodes were unable to retrieve the value"
        )

        //let beaconNodePeerCount = try beaconNode.peers.count().wait()
        //print("BeaconNode::PeerCount == \(beaconNodePeerCount)")
        //for i in (0..<numberOfNodes) {
        //    let peerCount = try nodes[i].peers.count().wait()
        //    print("Node[\(i)]::PeerCount == \(peerCount)")
        //}

        beaconNode.shutdown()
        for node in nodes { node.shutdown() }

        //sleep(1)

        print("All Done!")
    }

    var nextPort: Int = 10000
    private func makeHost(
        mode: KadDHT.Mode = .client,
        options: KadDHT.NodeOptions = .default,
        bootstrapPeers: [PeerInfo] = BootstrapPeerDiscovery.IPFSBootNodes,
        autoHeartbeat: Bool = false,
        usingGroup: Application.EventLoopGroupProvider = .createNew
    ) throws -> Application {
        let lib = try Application(.testing, peerID: PeerID(.Ed25519), eventLoopGroupProvider: usingGroup)
        lib.logger.logLevel = .notice
        lib.security.use(.noise)
        lib.muxers.use(.mplex)
        lib.dht.use(.kadDHT(mode: mode, options: options, bootstrapPeers: bootstrapPeers, autoUpdate: autoHeartbeat))
        lib.servers.use(.tcp(host: "127.0.0.1", port: self.nextPort))

        self.nextPort += 1
        return lib
    }
}

extension ComprehensivePeer: CustomStringConvertible {
    public var description: String {
        let header = "--- 👥 \(self.id) 👥 ---"
        return """
            \(header)
            ☎️ Addresses:
            \t- \(self.addresses.map { $0.description }.joined(separator: "\n\t- "))
            📒 Protocols:
            \t- \(self.protocols.map { $0.stringValue }.joined(separator: "\n\t- "))
            ℹ️ MetaData:
            \t- \(self.metadata.map { "\($0.key) - \(String(data: Data($0.value), encoding: .utf8) ?? $0.value.description)" }.joined(separator: "\n\t- "))
            📜 Records:
            \t\(self.records.map { "\($0.description.replacingOccurrences(of: "\n", with: "\n\t"))" }.joined(separator: "\n\t"))
            \(String(repeating: "-", count: header.count + 2))
            """
    }
}
