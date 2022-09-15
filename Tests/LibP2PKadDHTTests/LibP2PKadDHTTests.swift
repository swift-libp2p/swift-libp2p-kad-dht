import XCTest
import LibP2P
import LibP2PNoise
import LibP2PMPLEX
import CID
import CryptoSwift
@testable import LibP2PKadDHT
import Multihash

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
  
    
    /// ******************************************************
    ///    Testing External KadDHT - Single Query - GetValue
    /// ******************************************************
    ///
    /// - For getValue(key: )
    ///   - let key = try "/pk/".bytes + CID("QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ").multihash.value
    func testLibP2PKadDHT_DirectPing() throws {
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
        /// Init the libp2p node
        let lib = try makeHost()
        
        /// Start the node
        try lib.start()
                
        /// Do your test stuff ...
        XCTAssertTrue(lib.dht.kadDHT.state == .started)
      
        // This doesn't work... we need to find an actual value to query...
        let key = try "/ipfs/".bytes + CID("QmXuNFLZc6Nb5akB4sZsxK3doShsFKT1sZFvxLXJvZQwAW").multihash.value
        
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
        /// Init the libp2p node
        let lib = try makeHost()
        
        /// Start the node
        try lib.start()
                
        /// Do your test stuff ...
        XCTAssertTrue(lib.dht.kadDHT.state == .started)
      
        //let peerID = "QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN" // nil
        //let peerID = "QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt" // nil
        //let peerID = "QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa" // nil
        let peerID = "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ" // Success
        //let peerID = "QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb" // nil
        
        let key = try "/pk/".bytes + PeerID(cid: peerID).id
        
        let val = try lib.dht.kadDHT.getUsingLookupList(key).wait()
      
        XCTAssertNotNil(val)
        if let val = val {
            print(try val.toProtobuf().serializedData().toHexString())
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
  
    func testPubKeyRecordValidator() throws {
        let dhtRecordData = Data(hex: "0a262f706b2f1220b04a57d40eca138809f139a76b12044333c3740391c9bf1ce9d8e21a79210bfd12ab04080012a60430820222300d06092a864886f70d01010105000382020f003082020a0282020100a1f5c0e7c0d5e556afc0e84566f8c565773adb548ddc219ca9688613a0096c2dfd069804c84968545b9c9df19dd131cc8408b7781df7ddfaf208a42a821523ce03955164a62dcab6bd10dd26f8507517567ca128f00a056d8636b9549ddb59ca727628775c90bd91d6251adbdfd36bf68a09c3bfe69e1b1587e8f31a4b55afc8095e7b6f6683165f9c0ef0ad1b22d8b73749ee02aa46566cd5f7a9ff6eb1099fe36b363abd4e1293108a6d473a349e77aca15e49b20ffe61b4222eb3a634e8481d71a7fdceea88a2044fa5cedde1dee314e27880bc713ca578814684e85e0d21cff40e23c341f13ee1a06452f284664999862973e51d692b578cd9b7de89d786ad6baebcf8dfc343db8eda434a15929591917c52bf16741359149d0e7092bc919928f1d5b25cb48b0f90a7a05b0eb29adca993f893c6fb137a53a5c470a8a309b574bb4fd80879bde7dcc237eaf2ce9a17b9193032df99c8bf551987561ee264a09730f9029610571625e0d0e1e2a7f90469a6a480ed08cf9b4c3af0567bfe9abf470079d8cc7d7f22efc83598f86c9e0678caf79e2299a99c47c8d057e7f3b8af40185c8dd499a1c167c358d7ab83af6581944ce0b8b6bd2cfe4bf80c8c9e7f61fe94816df79e12ae5e82c588f894b86fd599da5912f8754de2a23f2d1529845a5570a72d8d8537325b95dd3c69d9ca30b8186c20170d10955b7da216822c7302030100012a1e323032322d30392d31325431343a34363a35322e3839323937333034325a")
        
        // Ensure we can instantiate a DHT.Record Protobuf
        let dhtRecord = try DHT.Record(contiguousBytes: dhtRecordData)
        
        let pubKeyRecordValidator = DHT.PubKeyValidator()
        let isValid = try pubKeyRecordValidator.validate(key: dhtRecord.key.bytes, value: dhtRecordData.bytes)
        print(isValid)
        
    }
    
    func testPeerRecord() throws {
        let recordData = Data(hex: "080012a60430820222300d06092a864886f70d01010105000382020f003082020a0282020100a1f5c0e7c0d5e556afc0e84566f8c565773adb548ddc219ca9688613a0096c2dfd069804c84968545b9c9df19dd131cc8408b7781df7ddfaf208a42a821523ce03955164a62dcab6bd10dd26f8507517567ca128f00a056d8636b9549ddb59ca727628775c90bd91d6251adbdfd36bf68a09c3bfe69e1b1587e8f31a4b55afc8095e7b6f6683165f9c0ef0ad1b22d8b73749ee02aa46566cd5f7a9ff6eb1099fe36b363abd4e1293108a6d473a349e77aca15e49b20ffe61b4222eb3a634e8481d71a7fdceea88a2044fa5cedde1dee314e27880bc713ca578814684e85e0d21cff40e23c341f13ee1a06452f284664999862973e51d692b578cd9b7de89d786ad6baebcf8dfc343db8eda434a15929591917c52bf16741359149d0e7092bc919928f1d5b25cb48b0f90a7a05b0eb29adca993f893c6fb137a53a5c470a8a309b574bb4fd80879bde7dcc237eaf2ce9a17b9193032df99c8bf551987561ee264a09730f9029610571625e0d0e1e2a7f90469a6a480ed08cf9b4c3af0567bfe9abf470079d8cc7d7f22efc83598f86c9e0678caf79e2299a99c47c8d057e7f3b8af40185c8dd499a1c167c358d7ab83af6581944ce0b8b6bd2cfe4bf80c8c9e7f61fe94816df79e12ae5e82c588f894b86fd599da5912f8754de2a23f2d1529845a5570a72d8d8537325b95dd3c69d9ca30b8186c20170d10955b7da216822c730203010001")

        let pub = try PeerID(marshaledPublicKey: recordData)
        print(pub)
        print(pub.b58String)
        print(pub.cidString)
    }
    
    func testExtractNamespaceFromKey() throws {
        let peerID = "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
        //let peerID = "QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"
        
        let key = try "/pk/".bytes + PeerID(cid: peerID).id // or CID(...).multihash.value
        print(key)
        print("/pk/\(peerID)".bytes)
        
        print(extractNamespace(key) ?? [])
        
        print(String(data: Data(extractNamespace(key)!), encoding: .utf8) ?? "NIL")
        
        XCTAssertEqual(key, [47, 112, 107, 47, 18, 32, 176, 74, 87, 212, 14, 202, 19, 136, 9, 241, 57, 167, 107, 18, 4, 67, 51, 195, 116, 3, 145, 201, 191, 28, 233, 216, 226, 26, 121, 33, 11, 253])
    }
    
    func testTimeRecievedStringToDate() throws {
        let timeReceived = "2022-09-12T14:46:52.892973042Z"
        
        let df = ISO8601DateFormatter()
        // Insert .withFractionalSeconds to the current format.
        df.formatOptions.insert(.withFractionalSeconds)
        let new = df.date(from: timeReceived)
        print(new ?? "NIL")
        
        let output = df.string(from: new!)
        print(output)
    }
    
    func testTimeRecievedStringToDate2() throws {
        let timeReceived = "2022-09-12T14:46:52.892973042Z"
                
        let df = DateFormatter()
        df.locale = Locale(identifier: "en_US_POSIX")
        df.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"

        if let parsed = df.date(from: timeReceived) {
            print(parsed)
            // try and recover it
            let recovered = df.string(from: parsed)
            print(recovered)
            // "2022-09-12T14:46:52.892973042Z" is not equal to "2022-09-12T14:46:52.892000000Z"
            XCTAssertEqual(timeReceived, recovered)
        } else {
            XCTFail("Failed to parse RFC3339 Nanosecond Precision Date String")
        }
    }
    
    func testTimeRecievedStringToDate3() throws {
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
        let now2 = RFC3339Date()
        
        XCTAssertLessThan(now1, now2)
        XCTAssertEqual(now1.string, now2.string)
    }
    
    struct RFC3339Date:Equatable, Comparable {
        private static var strFormat:String = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"
        private static var locale:Locale = Locale(identifier: "en_US_POSIX")
        
        /// The original RFC3339 String that was parsed if available
        private var originalString:String?
        public private(set) var date:Date
        public var string:String {
            originalString ?? formatter.string(from: date)
        }
        
        /// The Stored Date Formatter
        private let formatter:DateFormatter
        
        public init(string: String) throws {
            self.formatter = DateFormatter()
            self.formatter.locale = RFC3339Date.locale
            self.formatter.dateFormat = RFC3339Date.strFormat
            
            guard let date = self.formatter.date(from: string) else {
                throw NSError(domain: "Invalid RFC3339 Date String", code: 0)
            }
            
            let nanoString = string[string.lastIndex(of: ".")!...].dropFirst().dropLast()
            guard nanoString.count == 9 else { throw NSError(domain: "Invalid RFC3339 Date String", code: 0) }
            
            self.originalString = string
            self.date = date
        }
        
        public init() {
            self.formatter = DateFormatter()
            self.formatter.locale = RFC3339Date.locale
            self.formatter.dateFormat = RFC3339Date.strFormat
            
            self.originalString = nil
            self.date = Date()
        }
        
        public init(date: Date) {
            self.formatter = DateFormatter()
            self.formatter.locale = RFC3339Date.locale
            self.formatter.dateFormat = RFC3339Date.strFormat
            
            self.originalString = nil
            self.date = Date()
        }
        
        static func == (lhs:RFC3339Date, rhs:RFC3339Date) -> Bool {
            switch (lhs.originalString, rhs.originalString) {
            case (.some(let ogL), .some(let ogR)):
                return ogL == ogR
            default:
                return lhs.date == rhs.date
            }
        }
        
        static func < (lhs: LibP2PKadDHTTests.RFC3339Date, rhs: LibP2PKadDHTTests.RFC3339Date) -> Bool {
            switch (lhs.originalString, rhs.originalString) {
            case (.some(let ogL), .some(let ogR)):
                guard lhs.date == rhs.date else {
                    return lhs.date < rhs.date
                }
                // Parse out the nanoseconds from the original strings and compare them...
                let nanoLeftString = ogL[ogL.lastIndex(of: ".")!...].dropFirst().dropLast()
                let nanoLeft = UInt64(nanoLeftString)!
                
                let nanoRightString = ogR[ogR.lastIndex(of: ".")!...].dropFirst().dropLast()
                let nanoRight = UInt64(nanoRightString)!
                
                return nanoLeft < nanoRight
            default:
                return lhs.date < rhs.date
            }
        }
    }
    
    /// Extracts a namespace from the front of a Key if one exists...
    ///
    /// - Note: "/" in utf8 == 47
    private func extractNamespace(_ key:[UInt8]) -> [UInt8]? {
        guard key.first == UInt8(47) else { return nil }
        guard let idx = key.dropFirst().firstIndex(of: UInt8(47)) else { return nil }
        return Array(key[1..<idx])
    }
  
    /// **********************************************************
    ///    Testing Internal KadDHT - Single Query - FindProvider
    /// **********************************************************
    ///
    /// - For findProvider(cid: )
    ///   - let key = try CID("QmXuNFLZc6Nb5akB4sZsxK3doShsFKT1sZFvxLXJvZQwAW").multihash.value (results in found providers)
    func testLibP2PKadDHT_FindProviderQuery() throws {
        /// Init the libp2p node
        let lib = try makeHost()
        
        /// Prepare our expectations
        //let expectationNode1ReceivedNode2Subscription = expectation(description: "Node1 received fruit subscription from Node2")
        
        /// Start the node
        try lib.start()
                
        /// Do your test stuff ...
        XCTAssertTrue(lib.dht.kadDHT.state == .started)
        
        /// Attempt to find providers of the following CID
        let key = try CID("QmXuNFLZc6Nb5akB4sZsxK3doShsFKT1sZFvxLXJvZQwAW").multihash.value
        //let key = try CID("QmdSn5nS2toXqj5jKGvpsoNJjk2rofY6ctk7RY86t6KeMS").multihash.value
        let val = try lib.dht.kadDHT.getProvidersUsingLookupList(key).wait()
        print(val)
        XCTAssertFalse(val.isEmpty)
        
        sleep(2)
//        for provider in val {
//            if provider.addresses.isEmpty { continue }
//            let dialableAddresses = try lib.dialableAddress(provider.addresses, externalAddressesOnly: true, on: lib.eventLoopGroup.any()).wait()
//            guard let addy = dialableAddresses.first else { continue }
//            print("Attempting to get value from provider \(provider.peer) via \(addy)")
//            let value = lib.dht.kadDHT._sendQuery(.getValue(key: key), to: provider)
//        }

        /// Providers are meant to be used with Bitswap (not with dht.getValue)
        if let first = val.first(where: { !$0.addresses.isEmpty }) {
            let value = try? lib.dht.kadDHT._sendQuery(.getValue(key: key), to: first).wait()
            print(value)
        }
        
        sleep(2)
        
        lib.peers.dumpAll()
        
        sleep(2)
        
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
        /// Init the libp2p node
        let lib = try makeHost()
        
        /// Start the node
        try lib.start()
                
        /// Do your test stuff ...
        XCTAssertTrue(lib.dht.kadDHT.state == .started)
        
        /// Create a Public Key Record using our nodes PeerID
        
        let df = ISO8601DateFormatter()
        df.formatOptions.insert(.withFractionalSeconds)
        
        let key = "/pk/".bytes + lib.peerID.id
        let record = try DHT.Record.with { rec in
            rec.key = Data(key)
            rec.value = try Data(lib.peerID.marshalPublicKey())
            rec.timeReceived = df.string(from: Date())
        }
        
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
        /// Init the libp2p node
        let lib = try makeHost()
        
        /// Start the node
        try lib.start()
                
        /// Do your test stuff ...
        XCTAssertTrue(lib.dht.kadDHT.state == .started)
        
        lib.topology.register(TopologyRegistration(protocol: "/meshsub/1.0.0", handler: TopologyHandler(onConnect: { peerID, conn in
            print("⭐️ Found a /meshsub/1.0.0 \(peerID)")
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
    
//    func testInternalNetwork_PeerRouting() throws {
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
//        // Ensure Node3 can find Node1
//        let peer = node3.peerRouting.findPeer(peer: node1.peerID)
//
//        sleep(2)
//
//        beaconNode.shutdown()
//        nodes.forEach { $0.shutdown() }
//
//        print("All Done!")
//    }
    
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
        let numberOfNodes = 10
        let dhtParams = KadDHT.NodeOptions(connectionTimeout: .seconds(5), maxConcurrentConnections: 3, bucketSize: 5, maxPeers: 15, maxKeyValueStoreEntries: 10)
        let beaconNode = try makeHost(mode: .server, options: dhtParams, bootstrapPeers: [], autoHeartbeat: false)
        let beaconPeerInfo = PeerInfo(peer: beaconNode.peerID, addresses: beaconNode.listenAddresses)
        let nodes = try (0..<numberOfNodes).map { _ in try makeHost(mode: .server, options: dhtParams, bootstrapPeers: [beaconPeerInfo], autoHeartbeat: false) }
        
        try beaconNode.start()
        try nodes.forEach { try $0.start() }
        
//        nodes.forEach {
//            let _ = $0.dht.kadDHT.heartbeat()
//            usleep(200_000)
//        }
//
//        sleep(15)
        
        let item1Key = Digest.sha256("apple".bytes)
        let item1Record = DHT.Record.with {
            $0.key = Data(item1Key)
            $0.value = Data("🍎".utf8)
        }
        
        let storeAttempt1 = try nodes[0].dht.kadDHT.storeNew(item1Key, value: item1Record).wait()
        print(storeAttempt1)
        
        sleep(5)
        
        let _ = try beaconNode.dht.kadDHT.heartbeat().wait()
        
        sleep(2)
        
//        nodes.forEach {
//            let _ = $0.dht.kadDHT.heartbeat()
//            usleep(200_000)
//        }
//
//        sleep(15)
        
        //print(beaconNode.dht.kadDHT.dht)
        //print(nodes[0].dht.kadDHT.dht)
        //print(nodes[1].dht.kadDHT.dht)
        
        // Ensure the beacon node can retrieve the value
        let getAttempt1 = try beaconNode.dht.kadDHT.getUsingLookupList(item1Key).wait()
        print(getAttempt1 ?? "NIL")
        XCTAssertNotNil(getAttempt1)
        XCTAssertEqual(getAttempt1?.key, Data(item1Key))
        XCTAssertEqual(getAttempt1?.value, Data("🍎".utf8))
        
        // Ensure the other node can retrieve the value
        for i in (1..<numberOfNodes) {
            let getAttempt2 = try nodes[i].dht.kadDHT.getUsingLookupList(item1Key).wait()
            print(getAttempt2 ?? "NIL")
            XCTAssertNotNil(getAttempt2)
            XCTAssertEqual(getAttempt2?.key, Data(item1Key))
            XCTAssertEqual(getAttempt2?.value, Data("🍎".utf8))
        }
        
        sleep(2)
        
        let beaconNodePeerCount = try beaconNode.peers.count().wait()
        print("BeaconNode::PeerCount == \(beaconNodePeerCount)")
        for i in (0..<numberOfNodes) {
            let peerCount = try nodes[i].peers.count().wait()
            print("Node[\(i)]::PeerCount == \(peerCount)")
        }
        
        beaconNode.shutdown()
        nodes.forEach { $0.shutdown() }
        
        print("All Done!")
    }
    
    var nextPort:Int = 10000
    private func makeHost(mode:KadDHT.Mode = .client, options: KadDHT.NodeOptions = .default, bootstrapPeers:[PeerInfo] = BootstrapPeerDiscovery.IPFSBootNodes, autoHeartbeat:Bool = false) throws -> Application {
        let lib = try Application(.testing, peerID: PeerID(.Ed25519))
        lib.security.use(.noise)
        lib.muxers.use(.mplex)
        lib.dht.use(.kadDHT(mode: mode, options: options, bootstrapPeers: bootstrapPeers, autoUpdate: autoHeartbeat))
        lib.servers.use(.tcp(host: "127.0.0.1", port: nextPort))
        
        nextPort += 1
        
        lib.logger.logLevel = .warning
        
        return lib
    }

}
