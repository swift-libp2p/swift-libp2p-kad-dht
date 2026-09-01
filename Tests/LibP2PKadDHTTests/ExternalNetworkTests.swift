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
import LibP2P
import LibP2PYAMUX
import Testing

@testable import LibP2PKadDHT

extension LibP2PKadDHTTests {

    @Suite("External Network Tests", .externalIntegrationTestsEnabled, .serialized)
    final class ExternalNetworkTests {

        /// ********************************************
        ///    Testing External KadDHT - Heartbeat
        /// ********************************************
        ///
        /// This test manually triggers one KadDHT heartbeat that kicks off a FindNode lookup for our libp2p PeerID
        /// A heartbeat / node lookup takes about 22 secs and discovers about 20 peers...
        /// 📒 --------------------------------- 📒
        /// Routing Table [<peer.ID PiV5bE>]
        /// Bucket Count: 1 buckets of size: 20
        /// Total Peers: 20
        /// b[0] = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0]
        /// ---------------------------------------
        /// 2 heartbeats --> Time:  48 seconds,  Mem: 12.5 --> 13.8,  CPU: 0-12%,  Peers: 28
        /// 📒 --------------------------------- 📒
        /// Routing Table [<peer.ID PiV5bE>]
        /// Bucket Count: 2 buckets of size: 20
        /// Total Peers: 28
        /// b[0] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        /// b[1] = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        /// ---------------------------------------
        /// 3 heartbeats --> Time:  59 seconds,  Mem: 12.5 --> 14.5,  CPU: 0-12%,  Peers: 27
        @Test(.disabled())
        func testLibP2PKadDHT_SingleHeartbeat() throws {
            /// Init the libp2p node
            let lib = try makeHost()

            /// Start the node
            try lib.start()

            /// Do your test stuff ...
            #expect(lib.dht.kadDHT.state == .started)

            //let exp = expectation(description: "Wait for response")
            print("*** Before Lookup ***")
            print(lib.dht.kadDHT.peerstore)
            print("")

            print("*** Before Lookup ***")
            lib.peers.dumpAll()
            print("")

            for _ in (0..<1) {
                /// Trigger a heartbeat (which will perform a peer lookup for our peerID)
                try lib.dht.kadDHT.heartbeat().wait()

                sleep(2)
            }

            print("*** After Lookup ***")
            print("(DHT Peerstore: \(try lib.dht.kadDHT.peerstore.count().wait()) - \(lib.dht.kadDHT.peerstore)")
            print("")

            print("")
            lib.peers.dumpAll()
            print("")

            print("Connections: ")
            print(try lib.connections.getConnections(on: nil).wait())

            print("*** History ***")
            lib.connections.dumpConnectionHistory()

            print("*** Metrics ***")
            for hist in lib.dht.kadDHT.metrics.history { print(hist.event) }

            print("*** Routing Table ***")
            print(lib.dht.kadDHT.routingTable)

            sleep(2)

            /// Stop the node
            lib.shutdown()

            print("All Done!")
        }

        /// 20 heartbeats --> Time:  77.6 seconds,  Mem: 18.8,  CPU: 0-20%,  Peers: 258
        /// 📒 --------------------------------- 📒
        /// Routing Table [<peer.ID AK2Ay9>]
        /// Bucket Count: 9 buckets of size: 20
        /// Total Peers: 97
        /// b[0] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        /// b[1] = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        /// b[2] = [2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]
        /// b[3] = [3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3]
        /// b[4] = [4, 4, 4]
        /// b[5] = []
        /// b[6] = [6]
        /// b[7] = [7, 7, 7, 7, 7, 7, 7]
        /// b[8] = [8, 8, 8, 8, 8, 8, 8, 9, 11, 9, 9, 12, 13, 16]
        /// ---------------------------------------
        @Test(.disabled())
        func testLibP2PKadDHT_SingleHeartbeat_Async() async throws {
            /// Init the libp2p node
            let lib = try makeHost()

            /// Start the node
            try await lib.startup()

            /// Do your test stuff ...
            #expect(lib.dht.kadDHT.state == .started)

            print("*** Before Lookup ***")
            print(lib.dht.kadDHT.peerstore)
            print("")

            print("*** Before Lookup ***")
            lib.peers.dumpAll()
            print("")

            for _ in (0..<5) {
                /// Trigger a heartbeat (which will perform a peer lookup for our peerID)
                try await lib.dht.kadDHT.heartbeat().get()

                try await Task.sleep(for: .seconds(1))
            }

            print("*** After Lookup ***")
            print("(DHT Peerstore: \(try await lib.dht.kadDHT.peerstore.count().get()) - \(lib.dht.kadDHT.peerstore)")
            print("")

            print("Connections: ")
            for conn in try await lib.connections.getConnections(on: nil).get() {
                print("\(conn)")
            }

            print("*** History ***")
            lib.connections.dumpConnectionHistory()

            print("*** Metrics ***")
            for hist in lib.dht.kadDHT.metrics.history { print(hist.event) }

            print("*** Routing Table ***")
            print(lib.dht.kadDHT.routingTable)

            try await Task.sleep(for: .milliseconds(50))

            /// Stop the node
            try await lib.asyncShutdown()
        }

        /// ******************************************************
        ///    Testing External KadDHT - Single Query - GetValue
        /// ******************************************************
        ///
        /// - For getValue(key: )
        ///   - let key = try "/pk/".bytes + CID("QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ").multihash.value
        @Test(.disabled())
        func testLibP2PKadDHT_DirectPing() throws {
            /// Init the libp2p node
            let lib = try makeHost()

            /// Start the node
            try lib.start()

            /// Do your test stuff ...
            #expect(lib.dht.kadDHT.state == .started)

            let bootstrapPeer = PeerInfo(
                peer: try PeerID(cid: "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"),
                addresses: [
                    try Multiaddr("/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ")
                ]
            )

            let response = try lib.dht.kadDHT._sendQuery(.ping, to: bootstrapPeer).wait()
            print(response)

            sleep(2)

            /// Stop the node
            lib.shutdown()

            print("All Done!")
        }

        /// ******************************************************
        ///    Testing External KadDHT - Single Query - GetValue
        /// ******************************************************
        ///
        /// - For getValue(key: )
        ///   - let key = try "/pk/".bytes + CID("QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ").multihash.value
        @Test(.disabled())
        func testLibP2PKadDHT_GetValueQuery() throws {
            /// Init the libp2p node
            let lib = try makeHost()

            /// Start the node
            try lib.start()

            /// Do your test stuff ...
            #expect(lib.dht.kadDHT.state == .started)

            // This doesn't work... we need to find an actual value to query...
            //let key = try "/ipfs/".bytes + CID("QmXuNFLZc6Nb5akB4sZsxK3doShsFKT1sZFvxLXJvZQwAW").multihash.value // Doesnt work
            //let key = try "/ipfs/".bytes + CID("QmSnuWmxptJZdLJpKRarxBMS2Ju2oANVrgbr2xWbie9b2D").multihash.value // Doesnt work
            let key = try "/ipfs/".bytes + CID("QmdmQXB2mzChmMeKY47C43LxUdg1NDJ5MWcKMKxDu7RgQm").multihash.value

            //let key = try "/pk/".bytes + CID(peerID).multihash.value

            let val = try lib.dht.kadDHT.get(key).wait()
            print(val ?? "NIL")

            sleep(2)

            /// Stop the node
            lib.shutdown()

            print("All Done!")
        }

        /// ******************************************************
        ///    Testing External KadDHT - Single Query - GetValue
        /// ******************************************************
        ///
        /// - For getValue(key: )
        ///   - let key = try "/pk/".bytes + CID("QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ").multihash.value
        @Test(.disabled())
        func testLibP2PKadDHT_GetValueQuery_PeerRecord() throws {
            /// Init the libp2p node
            let lib = try makeHost()

            /// Start the node
            try lib.start()

            /// Do your test stuff ...
            #expect(lib.dht.kadDHT.state == .started)

            //let peerID = "QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN" // nil
            //let peerID = "QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt" // nil
            //let peerID = "QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa" // nil
            let peerID = "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"  // Success
            //let peerID = "QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb" // nil

            let key = try "/pk/".bytes + PeerID(cid: peerID).id

            print("/pk/ => \("/pk/".bytes)")
            print("\(peerID) => \(try PeerID(cid: peerID).id)")

            let val = try lib.dht.kadDHT.get(key).wait()

            #expect(val != nil)
            if let val = val {
                print(try val.toProtobuf().serializedData().toHexString())
                print("DHT Record")
                print("Key (Hex): \(val.key.byteArray)")
                print("Value (Hex): \(val.value.byteArray)")
                print("Time Received: \(val.timeReceived)")
                #expect(try PeerID(marshaledPublicKey: val.value).b58String == peerID)
            } else {
                print("NIL")
            }

            sleep(2)

            /// Stop the node
            lib.shutdown()

            print("All Done!")
        }

        /// ******************************************************
        ///    Testing External KadDHT - Single Query - FindNode
        /// ******************************************************
        ///
        /// let val = try lib.dht.kadDHT.findPeer(peer: PeerID(cid: "QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN")).wait()
        @Test(
            .disabled(),
            .serialized,
            arguments: [
                "QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
                "QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
                "QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
                "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
                "QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
            ]
        )
        func testLibP2PKadDHT_FindNode(_ peerID: String) throws {
            /// Init the libp2p node
            let lib = try makeHost()

            /// Start the node
            try lib.start()

            #expect(lib.dht.kadDHT.state == .started)

            #expect(throws: Never.self) {
                let val = try lib.dht.kadDHT.findPeer(peer: PeerID(cid: peerID)).wait()
                #expect(val.peer.b58String == peerID)
                #expect(val.peer.type == .idOnly)
                #expect(val.addresses.count >= 1)
            }

            /// Stop the node
            lib.shutdown()
        }

        /// **********************************************************
        ///    Testing Internal KadDHT - Single Query - FindProvider
        /// **********************************************************
        ///
        /// - For findProvider(cid: )
        ///   - let key = try CID("QmXuNFLZc6Nb5akB4sZsxK3doShsFKT1sZFvxLXJvZQwAW").multihash.value (results in found providers)
        @Test(.disabled())
        func testLibP2PKadDHT_FindProviderQuery() throws {
            /// Init the libp2p node
            let lib = try makeHost()

            /// Prepare our expectations
            //let expectationNode1ReceivedNode2Subscription = expectation(description: "Node1 received fruit subscription from Node2")

            /// Start the node
            try lib.start()

            /// Do your test stuff ...
            #expect(lib.dht.kadDHT.state == .started)

            /// Attempt to find providers of the following CID
            //let key = try CID("QmXuNFLZc6Nb5akB4sZsxK3doShsFKT1sZFvxLXJvZQwAW").multihash.value
            //let key = try CID("QmdSn5nS2toXqj5jKGvpsoNJjk2rofY6ctk7RY86t6KeMS").multihash.value
            let key = try CID("QmdmQXB2mzChmMeKY47C43LxUdg1NDJ5MWcKMKxDu7RgQm").multihash.value  // XKCD Archives
            //let key = try CID("Qmdp4pcmePccsVHedMC4CsSnkEtXLXT2N3go7S8qeLg3RY").multihash.value  // 101 - Laser Scope
            let val = try lib.dht.kadDHT.lookupProviders(key, count: 0).wait()
            print("--- Providers For \(key.toBase64()) ---")
            print(val)
            print("----------------------------")
            #expect(val.isEmpty == false)

            /// Stop the node
            lib.shutdown()

            print("All Done!")
        }

        /// **********************************************************
        ///    Testing Internal KadDHT - Single Query - Provide
        /// **********************************************************
        ///
        /// - For findProvider(cid: )
        ///   - let key = try CID("QmXuNFLZc6Nb5akB4sZsxK3doShsFKT1sZFvxLXJvZQwAW").multihash.value (results in found providers)
        @Test(.disabled())
        func testLibP2PKadDHT_Provide() throws {
            /// Init the libp2p node
            let lib = try makeHost()

            /// Start the node
            try lib.start()

            /// Do your test stuff ...
            #expect(lib.dht.kadDHT.state == .started)

            /// Create a Public Key Record using our nodes PeerID

            //let df = ISO8601DateFormatter()
            //df.formatOptions.insert(.withFractionalSeconds)

            let key = "/pk/".bytes + lib.peerID.id
            //        let record = try DHT.Record.with { rec in
            //            rec.key = Data(key)
            //            rec.value = try Data(lib.peerID.marshalPublicKey())
            //            rec.timeReceived = df.string(from: Date())
            //        }

            let record = try KadDHT.createPubKeyRecord(peerID: lib.peerID)

            /// Attempt to store the Public Key Record on the DHT
            let val = try lib.dht.kadDHT.storeNew(key, value: record).wait()
            print(val)
            /// Peer: 12D3KooWRo5HnaS7zJJH9MfaZUJE7UQB4FPVNQYiGT3xbdzPTm6V
            /// Peer: QmPxcrHKPUDQtP9EKsvCkDdvFvt2iiY7AnWygumZP9kzhc
            /// 12D3KooWGirPF2sNqCFYaXuWhPF27hSLdCYER725kLfz2rxer4Sy
            /// QmZ9JkSfELePivfpG6fXk7cU1Zu95VCQhKbQafpZt6ZkDX

            sleep(5)

            /// Attempt to retrieve the Public Key Record from the DHT
            let pubKeyRecord = try lib.dht.kadDHT.get(key).wait()
            print(pubKeyRecord ?? "NIL")
            /// peer.ID Ro5Hna
            /// peer.ID PxcrHK
            /// peer.ID GirPF2
            /// peer.ID Z9JkSf

            sleep(2)

            /// Stop the node
            lib.shutdown()

            print("All Done!")
        }

        /// **************************************************************
        ///    Testing External KadDHT - Single Heartbeat - w/ Topology
        /// **************************************************************
        ///
        @Test(.disabled())
        func testLibP2PKadDHT_SingleHeartbeat_Topology() throws {
            /// Init the libp2p node
            let lib = try makeHost()

            /// Start the node
            try lib.start()

            /// Do your test stuff ...
            #expect(lib.dht.kadDHT.state == .started)

            lib.topology.register(
                TopologyRegistration(
                    protocol: "/meshsub/1.0.0",
                    handler: TopologyHandler(onConnect: { peerID, conn in
                        print("⭐️ Found a /meshsub/1.0.0 \(peerID)")
                    })
                )
            )

            //let exp = expectation(description: "Wait for response")
            print("*** Before Lookup ***")
            print(lib.dht.kadDHT.peerstore)
            print("")

            print("*** Before Lookup ***")
            lib.peers.dumpAll()
            print("")

            for _ in (0..<3) {
                /// Trigger a heartbeat (which will perform a peer lookup for our peerID)
                try lib.dht.kadDHT.heartbeat().wait()

                sleep(2)
            }

            print("*** After Lookup ***")
            print("(DHT Peerstore: \(try lib.dht.kadDHT.peerstore.count().wait()) - \(lib.dht.kadDHT.peerstore)")
            print("")

            print("*** After Lookup ***")
            let pAll = try lib.peers.all().wait()
            //print("(Libp2p Peerstore: \(pAll.count)) - \(pAll.map { "\($0.id)\nMultiaddr: [\($0.addresses.map { $0.description }.joined(separator: ",\n"))]\nProtocols: [\($0.protocols.map { $0.stringValue }.joined(separator: ",\n"))]\nMetadata: \($0.metadata.map { "\($0.key): \(String(data: Data($0.value), encoding: .utf8) ?? "NIL")" }.joined(separator: ",\n"))" }.joined(separator: "\n\n"))")
            print(pAll.map { "\($0)" }.joined(separator: "\n"))
            print("")
            print("Total Peers in PeerStore: \(try lib.peers.count().wait())")
            //lib.peers.dumpAll()
            print("")

            print("Connections: ")
            print(try lib.connections.getConnections(on: nil).wait())

            print("*** Metrics ***")
            for hist in lib.dht.kadDHT.metrics.history { print(hist.event) }

            print("*** Routing Table ***")
            print(lib.dht.kadDHT.routingTable)

            //waitForExpectations(timeout: 10, handler: nil)
            sleep(2)

            /// Stop the node
            lib.shutdown()

            print("All Done!")
        }

        var nextPort: Int = 10000
        private func makeHost(
            mode: KadDHT.Mode = .client,
            options: KadDHT.NodeOptions = .default,
            bootstrapPeers: [PeerInfo] = BootstrapPeerDiscovery.IPFSBootNodes,
            autoHeartbeat: Bool = false,
            usingGroup: Application.EventLoopGroupProvider = .singleton
        ) throws -> Application {
            let lib = try Application(.testing, peerID: PeerID(.Ed25519), eventLoopGroupProvider: usingGroup)
            lib.security.use(.noise)
            lib.muxers.use(.yamux)
            lib.dht.use(
                .kadDHT(mode: mode, options: options, bootstrapPeers: bootstrapPeers, autoUpdate: autoHeartbeat)
            )
            lib.servers.use(.tcp(host: "127.0.0.1", port: nextPort))

            nextPort += 1

            lib.logger.logLevel = .notice

            return lib
        }
    }

}
