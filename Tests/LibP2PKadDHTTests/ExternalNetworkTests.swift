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
import XCTest

@testable import LibP2PKadDHT

final class ExternalNetworkTests: XCTestCase {

    override func setUpWithError() throws {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDownWithError() throws {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

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
    func testLibP2PKadDHT_SingleHeartbeat() throws {
        throw XCTSkip("External Network Tests Skipped By Default")
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

        for _ in (0..<1) {
            /// Trigger a heartbeat (which will perform a peer lookup for our peerID)
            try lib.dht.kadDHT.heartbeat().wait()

            sleep(2)
        }

        print("*** After Lookup ***")
        print("(DHT Peerstore: \(try lib.dht.kadDHT.peerstore.count().wait()) - \(lib.dht.kadDHT.peerstore)")
        print("")

        print("*** After Lookup ***")
        let pAll = try lib.peers.all().wait()
        print(
            "(Libp2p Peerstore: \(pAll.count)) - \(pAll.map { "\($0.id)\nMultiaddr: [\($0.addresses.map { $0.description }.joined(separator: ", "))]\nProtocols: [\($0.protocols.map { $0.stringValue }.joined(separator: ", "))]\nMetadata: \($0.metadata.map { "\($0.key): \(String(data: Data($0.value), encoding: .utf8) ?? "NIL")" }.joined(separator: ", "))" }.joined(separator: "\n\n"))"
        )

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

        //waitForExpectations(timeout: 10, handler: nil)
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
    func testLibP2PKadDHT_DirectPing() throws {
        throw XCTSkip("External Network Tests Skipped By Default")
        /// Init the libp2p node
        let lib = try makeHost()

        /// Start the node
        try lib.start()

        /// Do your test stuff ...
        XCTAssertTrue(lib.dht.kadDHT.state == .started)

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
    func testLibP2PKadDHT_GetValueQuery() throws {
        throw XCTSkip("External Network Tests Skipped By Default")
        /// Init the libp2p node
        let lib = try makeHost()

        /// Start the node
        try lib.start()

        /// Do your test stuff ...
        XCTAssertTrue(lib.dht.kadDHT.state == .started)

        // This doesn't work... we need to find an actual value to query...
        //let key = try "/ipfs/".bytes + CID("QmXuNFLZc6Nb5akB4sZsxK3doShsFKT1sZFvxLXJvZQwAW").multihash.value // Doesnt work
        //let key = try "/ipfs/".bytes + CID("QmSnuWmxptJZdLJpKRarxBMS2Ju2oANVrgbr2xWbie9b2D").multihash.value // Doesnt work
        let key = try "/ipfs/".bytes + CID("QmdmQXB2mzChmMeKY47C43LxUdg1NDJ5MWcKMKxDu7RgQm").multihash.value

        //let key = try "/pk/".bytes + CID(peerID).multihash.value

        let val = try lib.dht.kadDHT.getUsingLookupList(key).wait()
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
    func testLibP2PKadDHT_GetValueQuery_PeerRecord() throws {
        throw XCTSkip("External Network Tests Skipped By Default")
        /// Init the libp2p node
        let lib = try makeHost()

        /// Start the node
        try lib.start()

        /// Do your test stuff ...
        XCTAssertTrue(lib.dht.kadDHT.state == .started)

        //let peerID = "QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN" // nil
        //let peerID = "QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt" // nil
        //let peerID = "QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa" // nil
        let peerID = "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"  // Success
        //let peerID = "QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb" // nil

        let key = try "/pk/".bytes + PeerID(cid: peerID).id

        print("/pk/ => \("/pk/".bytes)")
        print("\(peerID) => \(try PeerID(cid: peerID).id)")

        let val = try lib.dht.kadDHT.getUsingLookupList(key).wait()
        //let _ = try lib.identify.ping(peer: PeerID(cid: "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ")).wait()

        XCTAssertNotNil(val)
        if let val = val {
            print(try val.toProtobuf().serializedData().toHexString())
            print("DHT Record")
            print("Key (Hex): \(val.key.bytes)")
            print("Value (Hex): \(val.value.bytes)")
            print("Time Received: \(val.timeReceived)")
            XCTAssertEqual(try PeerID(marshaledPublicKey: val.value).b58String, peerID)
        } else {
            print("NIL")
        }

        sleep(2)

        lib.peers.dumpAll()

        sleep(2)

        /// Stop the node
        lib.shutdown()

        print("All Done!")
    }

    /// **********************************************************
    ///    Testing Internal KadDHT - Single Query - FindProvider
    /// **********************************************************
    ///
    /// - For findProvider(cid: )
    ///   - let key = try CID("QmXuNFLZc6Nb5akB4sZsxK3doShsFKT1sZFvxLXJvZQwAW").multihash.value (results in found providers)
    func testLibP2PKadDHT_FindProviderQuery() throws {
        throw XCTSkip("External Network Tests Skipped By Default")
        /// Init the libp2p node
        let lib = try makeHost()

        /// Prepare our expectations
        //let expectationNode1ReceivedNode2Subscription = expectation(description: "Node1 received fruit subscription from Node2")

        /// Start the node
        try lib.start()

        /// Do your test stuff ...
        XCTAssertTrue(lib.dht.kadDHT.state == .started)

        /// Attempt to find providers of the following CID
        //let key = try CID("QmXuNFLZc6Nb5akB4sZsxK3doShsFKT1sZFvxLXJvZQwAW").multihash.value
        //let key = try CID("QmdSn5nS2toXqj5jKGvpsoNJjk2rofY6ctk7RY86t6KeMS").multihash.value
        //let key = try CID("QmdmQXB2mzChmMeKY47C43LxUdg1NDJ5MWcKMKxDu7RgQm").multihash.value // XKCD Archives
        let key = try CID("Qmdp4pcmePccsVHedMC4CsSnkEtXLXT2N3go7S8qeLg3RY").multihash.value  // 101 - Laser Scope
        let val = try lib.dht.kadDHT.getProvidersUsingLookupList(key).wait()
        print("--- Providers For \(key.toBase64()) ---")
        print(val)
        print("----------------------------")
        XCTAssertFalse(val.isEmpty)

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
    func testLibP2PKadDHT_Provide() throws {
        throw XCTSkip("External Network Tests Skipped By Default")
        /// Init the libp2p node
        let lib = try makeHost()

        /// Start the node
        try lib.start()

        /// Do your test stuff ...
        XCTAssertTrue(lib.dht.kadDHT.state == .started)

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
        let pubKeyRecord = try lib.dht.kadDHT.getUsingLookupList(key).wait()
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
    func testLibP2PKadDHT_SingleHeartbeat_Topology() throws {
        throw XCTSkip("External Network Tests Skipped By Default")
        /// Init the libp2p node
        let lib = try makeHost()

        /// Start the node
        try lib.start()

        /// Do your test stuff ...
        XCTAssertTrue(lib.dht.kadDHT.state == .started)

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
        usingGroup: Application.EventLoopGroupProvider = .createNew
    ) throws -> Application {
        let lib = try Application(.testing, peerID: PeerID(.Ed25519), eventLoopGroupProvider: usingGroup)
        lib.security.use(.noise)
        lib.muxers.use(.mplex)
        lib.dht.use(.kadDHT(mode: mode, options: options, bootstrapPeers: bootstrapPeers, autoUpdate: autoHeartbeat))
        lib.servers.use(.tcp(host: "127.0.0.1", port: nextPort))

        //try lib.peers.add(peerInfo: PeerInfo(
        //    peer: PeerID(cid: "QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"),
        //    addresses: [Multiaddr("/ip4/139.178.91.71/tcp/4001/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN")]
        //))

        nextPort += 1

        lib.logger.logLevel = .notice

        return lib
    }
}
