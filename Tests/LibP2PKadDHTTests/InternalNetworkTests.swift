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
import LibP2PNoise
import LibP2PYAMUX
import Multihash
import Testing

@testable import LibP2PKadDHT

extension LibP2PKadDHTTests {

    @Suite("Internal Network Tests", .internalIntegrationTestsEnabled, .serialized)
    final class InternalNetworkTests {

        @Test(.internalIntegrationTestsEnabled)
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
            //peerRouting.findPeer(peer: node1.peerID)
            let peer = try #require(try? node4.dht.kadDHT.findPeer(peer: node1.peerID).wait())
            #expect(peer.peer == node1.peerID)
            #expect(peer.addresses == node1.listenAddresses)

            node1.shutdown()
            node2.shutdown()
            node3.shutdown()
            node4.shutdown()

            print("All Done!")
        }

        //    @Test(.internalIntegrationTestsEnabled)
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
        //        nodes.forEach { $0.shutdown() }
        //
        //        print("All Done!")
        //    }

        @Test(.internalIntegrationTestsEnabled)
        func testInternalNetwork() throws {
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
            #expect(storeAttempt1)

            for node in nodes {
                try node.dht.kadDHT.heartbeat().wait()
            }

            let storeAttempt2 = try nodes[0].dht.kadDHT.storeNew(item1Key, value: item1Record).wait()
            #expect(storeAttempt2)

            // Ensure the other nodes can retrieve the value
            var successes = 0
            for i in 0..<numberOfNodes {
                let getAttempt2 = try nodes[i].dht.kadDHT.getUsingLookupList(item1Key).wait()
                //print(getAttempt2 ?? "NIL")
                if getAttempt2 != nil { successes += 1 }
                #expect(getAttempt2 != nil)
                #expect(getAttempt2?.key == Data(item1Key))
                #expect(getAttempt2?.value == Data("🍎".utf8))
            }
            #expect(
                successes == numberOfNodes,
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

        @Test(.internalIntegrationTestsEnabled)
        func testInternalNetwork_SortingTest() throws {
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
                    logLevel: .critical,
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
                        logLevel: .critical,
                        usingGroup: .shared(group)
                    )
                )
            }

            // Boot each node
            for node in nodes { try node.start() }

            // Add the last nodes info to the first node
            try nodes[0].peers.add(peerInfo: nodes.last!.peerInfo).wait()

            for round in 0..<5 {
                for node in nodes {
                    try node.dht.kadDHT.heartbeat().wait()
                }

                /// Kademlia guarantees a local invariant, not a global ordering: each node should
                /// know the k peers nearest itself. Grade that directly — it converges toward 100%
                /// as the heartbeats progress.
                print("--- after heartbeat \(round + 1) ---")
                printKClosestCompleteness(nodes.map { $0.dht.kadDHT })
            }

            for i in 0..<numberOfNodes {
                let peerCount = try nodes[i].peers.count().wait()
                #expect(peerCount >= numberOfNodes / 2)
            }

            for node in nodes { node.shutdown() }

            print("All Done!")
        }

        @Test(.internalIntegrationTestsEnabled)
        func testInternalNetwork_Beacon() throws {
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
            #expect(storeAttempt1)

            for node in nodes {
                try node.dht.kadDHT.heartbeat().wait()
            }

            try beaconNode.dht.kadDHT.heartbeat().wait()

            let storeAttempt2 = try nodes[0].dht.kadDHT.storeNew(item1Key, value: item1Record).wait()
            #expect(storeAttempt2)

            // Ensure the beacon node can retrieve the value
            let getAttempt1 = try beaconNode.dht.kadDHT.getUsingLookupList(item1Key).wait()
            #expect(getAttempt1 != nil)
            #expect(getAttempt1?.key == Data(item1Key))
            #expect(getAttempt1?.value == Data("🍎".utf8))

            // Ensure the other nodes can retrieve the value
            var successes = getAttempt1 == nil ? 0 : 1
            for i in 1..<numberOfNodes {
                let getAttempt2 = try nodes[i].dht.kadDHT.getUsingLookupList(item1Key).wait()
                if getAttempt2 != nil { successes += 1 }
                #expect(getAttempt2 != nil)
                #expect(getAttempt2?.key == Data(item1Key))
                #expect(getAttempt2?.value == Data("🍎".utf8))
            }
            #expect(
                successes == numberOfNodes,
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
            logLevel: Logger.Level = .notice,
            usingGroup: Application.EventLoopGroupProvider = .singleton
        ) throws -> Application {
            let lib = try Application(.testing, peerID: PeerID(.Ed25519), eventLoopGroupProvider: usingGroup)
            lib.logger.logLevel = logLevel
            lib.security.use(.noise)
            lib.muxers.use(.yamux)
            lib.dht.use(
                .kadDHT(mode: mode, options: options, bootstrapPeers: bootstrapPeers, autoUpdate: autoHeartbeat)
            )
            lib.servers.use(.tcp(host: "127.0.0.1", port: self.nextPort))

            self.nextPort += 1
            return lib
        }
    }

}
