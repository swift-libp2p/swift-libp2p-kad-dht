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
        let query = KadDHT.Query.ping
        let encodedQuery = try query.encode()

        /// Send it over the wire...

        let decodedQuery = try KadDHT.Query.decode(encodedQuery)
        print("Recovered Query: \(decodedQuery)")

        guard case .ping = decodedQuery else { return XCTFail() }

        let response = KadDHT.Response.ping
        let encodedResponse = try response.encode()

        /// Send the response back over the wire...

        let decodedResponse = try KadDHT.Response.decode(encodedResponse)

        guard case .ping = decodedResponse else { return XCTFail() }
    }

    func testDHTFauxNetworkQuery_Store_Success() throws {
        /// Test Query.store and Response.stored
        let recordToStore = DHT.Record.with({ rec in
            rec.key = Data("test".utf8)
            rec.value = Data("Kademlia DHT".utf8)
        })
        let query = KadDHT.Query.putValue(key: "test".bytes, record: recordToStore)
        let encodedQuery = try query.encode()

        /// Send it over the wire...

        let decodedQuery = try KadDHT.Query.decode(encodedQuery)
        print("Recovered Query: \(decodedQuery)")

        guard case .putValue(let qKey, let qRecord) = decodedQuery else { return XCTFail() }

        /// Assume we can store the value
        print("Storing Value: '\(qRecord)' for Key: '\(qKey)'")
        let response = KadDHT.Response.putValue(key: qKey, record: qRecord)
        let encodedResponse = try response.encode()

        /// Send the response back over the wire...

        let decodedResponse = try KadDHT.Response.decode(encodedResponse)

        guard case .putValue(let key, let record) = decodedResponse else { return XCTFail() }

        XCTAssertEqual(key, recordToStore.key.bytes)
        XCTAssertEqual(record, recordToStore)
    }

    func testDHTFauxNetworkQuery_Store_Failure() throws {
        /// Test Query.store and Response.stored
        let recordToStore = DHT.Record.with({ rec in
            rec.key = Data("test".utf8)
            rec.value = Data("hello!".utf8)
        })
        let query = KadDHT.Query.putValue(key: "test".bytes, record: recordToStore)
        let encodedQuery = try query.encode()

        /// Send it over the wire...

        let decodedQuery = try KadDHT.Query.decode(encodedQuery)
        print("Recovered Query: \(decodedQuery)")

        guard case .putValue(let key, let value) = decodedQuery else { return XCTFail() }

        /// Assume we can't store the value for some reason
        print("Failed to store Value: '\(value)' for Key: '\(key)'")
        let response = KadDHT.Response.putValue(key: "test".bytes, record: nil)
        let encodedResponse = try response.encode()

        /// Send the response back over the wire...

        let decodedResponse = try KadDHT.Response.decode(encodedResponse)

        guard case .putValue(let key, let value) = decodedResponse else { return XCTFail() }

        XCTAssertEqual(key, "test".bytes)
        XCTAssertEqual(value, nil)
    }
    
    func testDHTFauxNetworkQuery_FindNode() throws {
        let testAddresses = try (0..<10).map { i -> PeerInfo in
            let pid = try PeerID(.Ed25519)
            return try PeerInfo(peer: pid, addresses: [Multiaddr("/ip4/127.0.0.1/tcp/\(1000 + i)/p2p/\(pid.b58String)")])
        }

        try testAddresses.forEach  {
            guard let cid = $0.addresses.first?.getPeerID() else { return XCTFail("Failed to extract PeerID from multiaddress") }
            XCTAssertNoThrow(try PeerID(cid: cid))
        }

        /// Test Query.findNode and Response.nodeSearch
        let query = KadDHT.Query.findNode(id: testAddresses.first!.peer)
        let encodedQuery = try query.encode()

        /// Send it over the wire...
        let decodedQuery = try KadDHT.Query.decode(encodedQuery)
        print("Recovered Query: \(decodedQuery)")

        guard case .findNode(let peer) = decodedQuery else { return XCTFail() }

        let response = try KadDHT.Response.findNode(closerPeers: testAddresses.filter({ $0.peer != peer }).map { try DHT.Message.Peer($0) })
        let encodedResponse = try response.encode()

        /// Send the response back over the wire...
        let decodedResponse = try KadDHT.Response.decode(encodedResponse)

        guard case .findNode(let closerPeers) = decodedResponse else { return XCTFail() }

        print(closerPeers)

        XCTAssertEqual(closerPeers.count, testAddresses.count - 1)
        XCTAssertFalse(try closerPeers.contains(where: { try $0.toPeerInfo().peer == peer } ))
    }

    func testDHTFauxNetworkQuery_GetValue_NoValue() throws {

        /// Test Query.findValue and Response.keySearch
        let query = KadDHT.Query.getValue(key: "test".bytes)
        let encodedQuery = try query.encode()

        /// Send it over the wire...

        let decodedQuery = try KadDHT.Query.decode(encodedQuery)
        print("Recovered Query: \(decodedQuery)")

        guard case .getValue(let key) = decodedQuery else { return XCTFail() }

        print("Key: \(key)")
        XCTAssertEqual(key, "test".bytes)
        /// Assume we don't have the key their looking for, so return a set of peers that are closer...
        let testAddresses = try (0..<3).map {
            let pid = try PeerID()
            return try PeerInfo(
                peer: pid,
                addresses: [Multiaddr("/ip4/127.0.0.1/tcp/\(1000 + $0)/p2p/\(pid.b58String)")]
            )
        }

        let response = try KadDHT.Response.getValue(key: "test".bytes, record: nil, closerPeers: testAddresses.map { try DHT.Message.Peer($0) })
        let encodedResponse = try response.encode()

        /// Send the response back over the wire...

        let decodedResponse = try KadDHT.Response.decode(encodedResponse)

        guard case .getValue(let key, let record, let closerPeers) = decodedResponse else { return XCTFail() }

        XCTAssertEqual(key, "test".bytes)
        XCTAssertNil(record)
        XCTAssertEqual(closerPeers.count, testAddresses.count)
    }

    func testDHTFauxNetworkQuery_FindValue_FoundValue() throws {

        /// Test Query.findValue and Response.keySearch
        let query = KadDHT.Query.getValue(key: "test".bytes)
        let encodedQuery = try query.encode()

        /// Send it over the wire...

        let decodedQuery = try KadDHT.Query.decode(encodedQuery)
        print("Recovered Query: \(decodedQuery)")

        guard case .getValue(let key) = decodedQuery else { return XCTFail() }

        print("Key: \(key)")
        XCTAssertEqual(key, "test".bytes)
        /// Assume we have the key their looking for
        let value = DHT.Record.with({ rec in
            rec.key = Data("test".utf8)
            rec.value = Data("Kademlia DHT".utf8)
        })

        let response = KadDHT.Response.getValue(key: "test".bytes, record: value, closerPeers: [])
        let encodedResponse = try response.encode()

        /// Send the response back over the wire...

        let decodedResponse = try KadDHT.Response.decode(encodedResponse)

        guard case .getValue(let key, let record, let closerPeers) = decodedResponse else { return XCTFail() }

        XCTAssertEqual(key, "test".bytes)
        XCTAssertNotNil(record)
        XCTAssertEqual(record?.key, Data("test".utf8))
        XCTAssertEqual(record?.value, Data("Kademlia DHT".utf8))
        XCTAssertEqual(closerPeers.count, 0)
    }
    
    // An example of a large response (>4kb) that gets chunked via our inbound stream window buffer.
    // Make sure we can decode the message in it's entirety
    func testDecodeDHTMessage() throws {
        let dhtMessage = Array(hex: "f637080442f6020a2600240801122064d6be62f0d67bf0a0a1d232d24badf66219c6ad14bb865b177b8e54a287a6071217290000000000000000000000000000000191020fa1cd03120b04c2f2399991020fa1cd031257047f00000191020fa1cd03d103d2032212200c0599ed33aa478dff2c0b7be3e40fb9f16c4ae2df843e772eea26ae1fd15db5d203221220faf5a659fd020440845c8a58c4ec099d7e03d54a7896bc45a3a5922e54722a3b1217290000000000000000000000000000000191020fa1cc03120804c2f23999060fa1120b047f00000191020fa1cc03120b04c2f2399991020fa1cc03120b047f00000191020fa1cd031263290000000000000000000000000000000191020fa1cd03d103d2032212200c0599ed33aa478dff2c0b7be3e40fb9f16c4ae2df843e772eea26ae1fd15db5d203221220faf5a659fd020440845c8a58c4ec099d7e03d54a7896bc45a3a5922e54722a3b12142900000000000000000000000000000001060fa11208047f000001060fa142f6020a26002408011220d0f66e25d1d87f42164273ad3f509776c989519c282fb7072d6e1b8dde2e8354120b047f00000191020fa1cd031257047f00000191020fa1cd03d103d2032212201462f5f8561d1a5b316278bc8b04e0c275b480f47563e038dc00809b0bdc23cfd20322122060a54b3a3fd85dcc5f4c5f4e2f7c337380086ea019f0aab2c99ed1ac3f0a93e712080468ee984d060fa1120b047f00000191020fa1cc03120b0468ee984d91020fa1cd03120b0468ee984d91020fa1cc031263290000000000000000000000000000000191020fa1cd03d103d2032212201462f5f8561d1a5b316278bc8b04e0c275b480f47563e038dc00809b0bdc23cfd20322122060a54b3a3fd85dcc5f4c5f4e2f7c337380086ea019f0aab2c99ed1ac3f0a93e71208047f000001060fa11217290000000000000000000000000000000191020fa1cd0312142900000000000000000000000000000001060fa11217290000000000000000000000000000000191020fa1cc0342a0050a260024080112208c65b9d3dfb7fba42c9a8e08835795a96e472a4de92faa31ef3c79da53d15888120b04ac1a065591020fa1cd031263290000000000000000000000000000000191020fa1cd03d103d203221220d9e268835631bd78774dd1a4ddcca806bd4a533a6cbc591826a92b66cc49606ad203221220e07f60f86b172ccd5d19c889da586a4339b078049162e7ecd01db371dc3bd1301257047f00000191020fa1cd03d103d203221220d9e268835631bd78774dd1a4ddcca806bd4a533a6cbc591826a92b66cc49606ad203221220e07f60f86b172ccd5d19c889da586a4339b078049162e7ecd01db371dc3bd1301217290000000000000000000000000000000191020fa1cd031217292406da1401b44d00a63d4d0362f1c84491020fa1cd03120b047f00000191020fa1cd0312080412b5b15e060fa1120b0412b5b15e91020fa1cc03120b0412b5b15e91020fa1cd031214292406da1401b44d00a63d4d0362f1c844060fa112142900000000000000000000000000000001060fa11208047f000001060fa11217292406da1401b44d00a63d4d0362f1c84491020fa1cc031217290000000000000000000000000000000191020fa1cc03125704ac1a065591020fa1cd03d103d203221220d9e268835631bd78774dd1a4ddcca806bd4a533a6cbc591826a92b66cc49606ad203221220e07f60f86b172ccd5d19c889da586a4339b078049162e7ecd01db371dc3bd130120804ac1a0655060fa1120b047f00000191020fa1cc03120b04ac1a065591020fa1cc031263292406da1401b44d00a63d4d0362f1c84491020fa1cd03d103d203221220d9e268835631bd78774dd1a4ddcca806bd4a533a6cbc591826a92b66cc49606ad203221220e07f60f86b172ccd5d19c889da586a4339b078049162e7ecd01db371dc3bd13042cf030a26002408011220575e3ace49bc995ac5b2eb5c8c297e834d1e28cdb437fa64a5e601c815c850bd120b048fc6608a91020fa1cc031257048fc6608a91020fa1cd03d103d20322122016e32acbeeb22dde4801df0966a1948d35593ccbcf13756d421af5334ab2840fd203221220cf569c33d875d0d45e2c2a74e2cd224dd5668440494e523c50083fbe90f991d91208047f000001060fa11217290000000000000000000000000000000191020fa1cd031257047f00000191020fa1cd03d103d20322122016e32acbeeb22dde4801df0966a1948d35593ccbcf13756d421af5334ab2840fd203221220cf569c33d875d0d45e2c2a74e2cd224dd5668440494e523c50083fbe90f991d91217290000000000000000000000000000000191020fa1cc031263290000000000000000000000000000000191020fa1cd03d103d20322122016e32acbeeb22dde4801df0966a1948d35593ccbcf13756d421af5334ab2840fd203221220cf569c33d875d0d45e2c2a74e2cd224dd5668440494e523c50083fbe90f991d9120b047f00000191020fa1cd0312142900000000000000000000000000000001060fa1120b047f00000191020fa1cc03120b048fc6608a91020fa1cd031208048fc6608a060fa142f6020a26002408011220228fd5775eec3088959e751d695a74bdacdc2b7dc9d31edebe9413879955028e120b04425e739f91020fa1cd031257047f00000191020fa1cd03d103d203221220ae3bf8e89b681a413c18ab5a1cf44792a734924f0bc70754690af458dd4cb7a5d203221220ef3f5909fb3ec4cf2036eb29bd00e5058389a08c8c14fa4248e72b31714ad22a1217290000000000000000000000000000000191020fa1cc0312142900000000000000000000000000000001060fa1120b04425e739f91020fa1cc03120b047f00000191020fa1cc031217290000000000000000000000000000000191020fa1cd03120804425e739f060fa1120b047f00000191020fa1cd031208047f000001060fa11263290000000000000000000000000000000191020fa1cd03d103d203221220ae3bf8e89b681a413c18ab5a1cf44792a734924f0bc70754690af458dd4cb7a5d203221220ef3f5909fb3ec4cf2036eb29bd00e5058389a08c8c14fa4248e72b31714ad22a42560a260024080112207dce2cdc07849bd25c2e1591e26cbc7e6208ec0ca133867a5b530d9370c3bd541208047f000001060fa1120b047f00000191020fa1cc03120b04339e3d5b91020fa1cc03120804339e3d5b060fa14283030a260024080112201b998427c1492b3453a77ec6349e1464bc2acce02d5d5c68b413dc39a9627633120b045ee7cd1491020fa1cd03120b045ee7cd1491020fa1cc03120b047f00000191020fa1cc031257047f00000191020fa1cd03d103d20322122080a8de3a1493c63aaafef7e4e03db946cd77ca0daaeaba4de3dc4ce56a7325a8d203221220de2326be8c3437c8ea378cc1759df0571bbd30842eacf74b2a42d1919a6a31bd1263290000000000000000000000000000000191020fa1cd03d103d20322122080a8de3a1493c63aaafef7e4e03db946cd77ca0daaeaba4de3dc4ce56a7325a8d203221220de2326be8c3437c8ea378cc1759df0571bbd30842eacf74b2a42d1919a6a31bd1217290000000000000000000000000000000191020fa1cc03120b047f00000191020fa1cd03120b045ee7cd14910206fecd031208045ee7cd14060fa11208047f000001060fa11217290000000000000000000000000000000191020fa1cd0312142900000000000000000000000000000001060fa142f6020a260024080112209afa2952a052536c477605db4f0e0ef6f2e76ea7fd867a7c1b151d517801cd5e1217290000000000000000000000000000000191020fa1cc03120b048f6ea85891020fa1cc031208048f6ea858060fa1120b048f6ea85891020fa1cd03120b047f00000191020fa1cc03120b047f00000191020fa1cd0312142900000000000000000000000000000001060fa11257047f00000191020fa1cd03d103d2032212206e38c8a586b513fbb58885bb0db90f8568a4b10030a3feb443403751efe78bf2d2032212208d3a9aab747a1bb051a3b1ada166ab8ab1720f9148ccf2bfe891227ffb4d9bc01208047f000001060fa11217290000000000000000000000000000000191020fa1cd031263290000000000000000000000000000000191020fa1cd03d103d2032212206e38c8a586b513fbb58885bb0db90f8568a4b10030a3feb443403751efe78bf2d2032212208d3a9aab747a1bb051a3b1ada166ab8ab1720f9148ccf2bfe891227ffb4d9bc042f6020a26002408011220754ea0e6154b95002f3bb8d40c914861f80fafe6d05bc75161f4b5f49a80ce87120b04c2a383a691020fa1cd031263290000000000000000000000000000000191020fa1cd03d103d203221220002295e47dd7760a9de05b910f104da0279e5c91d0c2960e2c18a9005313dbb9d2032212203130af0a77116b9525df787434c294b51c79e56c9ad44d710d2017087ec24b8a120b04c2a383a691020fa1cc031217290000000000000000000000000000000191020fa1cc03120804c2a383a6060fa11208047f000001060fa1120b047f00000191020fa1cc031217290000000000000000000000000000000191020fa1cd0312142900000000000000000000000000000001060fa1120b047f00000191020fa1cd031257047f00000191020fa1cd03d103d203221220002295e47dd7760a9de05b910f104da0279e5c91d0c2960e2c18a9005313dbb9d2032212203130af0a77116b9525df787434c294b51c79e56c9ad44d710d2017087ec24b8a42f6020a260024080112206d8265230407073f299684d1e32b59523bee2674b5efd4bd48612472c33bc28c120b047f00000191020fa1cc031217290000000000000000000000000000000191020fa1cc031208047f000001060fa11263290000000000000000000000000000000191020fa1cd03d103d20322122078beb0b84978854048de3439586e3263c5e856e5159770ee2b4ec82ad24efbe9d203221220b8ecdef19989425dfff7765e946b195dda850e0fdad4f21491cd485993e7d073120b0495f834d691020fa1cc0312080495f834d6060fa112142900000000000000000000000000000001060fa11217290000000000000000000000000000000191020fa1cd03120b047f00000191020fa1cd03120b0495f834d691020fa1cd031257047f00000191020fa1cd03d103d20322122078beb0b84978854048de3439586e3263c5e856e5159770ee2b4ec82ad24efbe9d203221220b8ecdef19989425dfff7765e946b195dda850e0fdad4f21491cd485993e7d07342f6020a26002408011220ba75521c18d0174e8d057e5068c62cec73519333fe9a4d0d1a66601939cb5566120b0417ef138e91020fa1cc0312080417ef138e060fa11263290000000000000000000000000000000191020fa1cd03d103d203221220f5cec935074cdd748188d2e6492a597adf6d601e7147ed79c75d7b423b0d5e0bd203221220119667c7bb880c61048b28e011bac8d7f20cfe0d28417060d06441bb6cd0f570120b047f00000191020fa1cd031257047f00000191020fa1")
        
        let anotherMessage = Array(hex: "cd03d103d203221220f5cec935074cdd748188d2e6492a597adf6d601e7147ed79c75d7b423b0d5e0bd203221220119667c7bb880c61048b28e011bac8d7f20cfe0d28417060d06441bb6cd0f570120b047f00000191020fa1cc0312142900000000000000000000000000000001060fa11217290000000000000000000000000000000191020fa1cc03120b0417ef138e91020fa1cd031217290000000000000000000000000000000191020fa1cd031208047f000001060fa142b4010a26002408011220f6f0fb5fd04d5b621a326fbfc4a49864056726a45a375d521c9cc292e0fb2d50120b047f00000191020fa1cc0312172920014ba0ffa400be000000000000000191020fa1cc031217290000000000000000000000000000000191020fa1cc0312080459a390c9060fa11208047f000001060fa112142920014ba0ffa400be0000000000000001060fa112142900000000000000000000000000000001060fa1120b0459a390c991020fa1cc0342f6020a2600240801122004837fafbab4c2cc90e9cfaf22587ca501006911bd35e1c64c64a5fb1f98cd7a1208047f000001060fa1120b047f00000191020fa1cd0312142900000000000000000000000000000001060fa11217290000000000000000000000000000000191020fa1cc03120b048c5232b091020fa1cc031208048c5232b0060fa11217290000000000000000000000000000000191020fa1cd03120b047f00000191020fa1cc031263290000000000000000000000000000000191020fa1cd03d103d203221220e93e0e8f51021b7cb352a9205a1aae5ce401b38178bd065c69edf1b41ad78e97d203221220a71a0005582dd24c286034700fb35f5dc718b19f2bc21d390de02b8e1a5068501257047f00000191020fa1cd03d103d203221220e93e0e8f51021b7cb352a9205a1aae5ce401b38178bd065c69edf1b41ad78e97d203221220a71a0005582dd24c286034700fb35f5dc718b19f2bc21d390de02b8e1a506850120b048c5232b091020fa1cd0342f6020a2600240801122099936b6b39e572ae6a739aa1b71d8564732ed374a8e3a914ab207bc9c679f7361217290000000000000000000000000000000191020fa1cd031208045fb3f069060fa1120b047f00000191020fa1cd031257047f00000191020fa1cd03d103d2032212206d1f969798c290fbea3030b17107691acbe17bc65d61228b1f3c3abaa6bf1515d2032212204d975ea6c839f2e10be43574a0ac2c0b6cc81889c2d36506e5edc6064afd833a1263290000000000000000000000000000000191020fa1cd03d103d2032212206d1f969798c290fbea3030b17107691acbe17bc65d61228b1f3c3abaa6bf1515d2032212204d975ea6c839f2e10be43574a0ac2c0b6cc81889c2d36506e5edc6064afd833a1208047f000001060fa1120b045fb3f06991020fa1cc03120b045fb3f06991020fa1cd031217290000000000000000000000000000000191020fa1cc03120b047f00000191020fa1cc0312142900000000000000000000000000000001060fa142560a26002408011220355b4d3353d427b0ff9fb24c514f265ed049eef5ba6b1ffc9588d32b5b330cd11208047f000001060fa1120b047f00000191020fa1cc03120b04593a26f491020fa1cc03120804593a26f4060fa142f6020a2600240801122052737ecce609b58c3cb783265cd093089350f9b2c800da94e79fb1ae283e1fe01217290000000000000000000000000000000191020fa1cd0312142900000000000000000000000000000001060fa1120b04907e8be891020fa1cc031263290000000000000000000000000000000191020fa1cd03d103d203221220150b744122239e08afd3d1a74043d3cf7337a0a0a178bc80ed9d11d5561535e9d203221220737a90ef3f49d95693f1d46eec712c690063650a9423a24e784e4f3c5a8678b51208047f000001060fa11217290000000000000000000000000000000191020fa1cc03120b047f00000191020fa1cc03120b047f00000191020fa1cd03120b04907e8be891020fa1cd03120804907e8be8060fa11257047f00000191020fa1cd03d103d203221220150b744122239e08afd3d1a74043d3cf7337a0a0a178bc80ed9d11d5561535e9d203221220737a90ef3f49d95693f1d46eec712c690063650a9423a24e784e4f3c5a8678b54285010a26002408011220c89dc9e0c45aeb2246083e54b3d5640f41abbda3bd8329b16a684299077c46ac1217290000000000000000000000000000000191020fa1cc031208048233b4a9060fa11208047f000001060fa112142900000000000000000000000000000001060fa1120b048233b4a991020fa1cc03120b047f00000191020fa1cc034283030a260024080112205de9dc2d624fe9c0bac5a20f91b0994006b9abf49b9e21552fb6c0282947ba73120b047f00000191020fa1cc031217290000000000000000000000000000000191020fa1cc03120b044b778d3491020fa1cd031257047f00000191020fa1cd03d103d203221220fb566b0c4d153378afb25f2de4b48f97302aa039c7a7b8906dd95007d97e6abad203221220b93ae1fba91242d96757195a4856a23f3836c76a2053165b6610c723647739a8120b044b778d3491020739cd0312142900000000000000000000000000000001060fa11208047f000001060fa1120b044b778d3491020fa1cc03120b047f00000191020fa1cd031217290000000000000000000000000000000191020fa1cd031208044b778d34060fa11263290000000000000000000000000000000191020fa1cd03d103d203221220fb566b0c4d153378afb25f2de4b48f97302aa039c7a7b8906dd95007d97e6abad203221220b93ae1fba91242d96757195a4856a23f3836c76a2053165b6610c723647739a842f6020a260024080112201cb485da92cd339c0c9221118a666d94dbeb9c055e37720ea140d836eeb14860120b047f00000191020fa1cc03120804acf54460060fa1120b047f00000191020fa1cd031217290000000000000000000000000000000191020fa1cd03120b04acf5446091020fa1cc031208047f000001060fa11263290000000000000000000000000000000191020fa1cd03d103d203221220c0e6d78a2f06047638f6aff26d251e147ac72e01e612bcd7c9a45de676eb758dd203221220f1e3cdea2cc7534dbfeb00324b45442bd366ea140d151d6beba043b7d29641741257047f00000191020fa1cd03d103d203221220c0e6d78a2f06047638f6aff26d251e147ac72e01e612bcd7c9a45de676eb758dd203221220f1e3cdea2cc7534dbfeb00324b45442bd366ea140d151d6beba043b7d29641741217290000000000000000000000000000000191020fa1cc0312142900000000000000000000000000000001060fa1120b04acf5446091020fa1cd0342cc040a26002408011220b707e30439f54fedc1f5f50bf04912af3c62304f849e7a84a0da3a6c54f9d004120b0408d2dce291020fa1cd03120b047f00000191020fa1cc0312080408d2dce2060fa11208047f000001060fa1120b04ac13298f91020fa1cc0312142900000000000000000000000000000001060fa11217290000000000000000000000000000000191020fa1cc031257047f00000191020fa1cd03d103d203221220a5be5592ab0f885ea77b84f990cea5b6dbc3ddde292530add0d70cf95df9a3e6d2032212207ce1ccc1a9ae0c6e9a2dcf72ac2fe80d0e583fb54b2300d81449852bd5041451120b04ac13298f91020fa1cd03120b0408d2dce291020fa1cc0312570408d2dce291020fa1cd03d103d203221220a5be5592ab0f885ea77b84f990cea5b6dbc3ddde292530add0d70cf95df9a3e6d2032212207ce1ccc1a9ae0c6e9a2dcf72ac2fe80d0e583fb54b2300d81449852bd5041451125704ac13298f91020fa1cd03d103d203221220a5be5592ab0f885ea77b84f990cea5b6dbc3ddde292530add0d70cf95df9a3e6d2032212207ce1ccc1a9ae0c6e9a2dcf72ac2fe80d0e583fb54b2300d81449852bd50414511263290000000000000000000000000000000191020fa1cd03d103d203221220a5be5592ab0f885ea77b84f990cea5b6dbc3ddde292530add0d70cf95df9a3e6d2032212207ce1ccc1a9ae0c6e9a2dcf72ac2fe80d0e583fb54b2300d81449852bd5041451120b047f00000191020fa1cd03120804ac13298f060fa11217290000000000000000000000000000000191020fa1cd035001")
        
        let prefix = uVarInt(dhtMessage)
        print(prefix)
        
        print(dhtMessage.count + anotherMessage.count)
        
        do {
            let dht = try DHT.Message(contiguousBytes: dhtMessage.dropFirst(prefix.bytesRead) + anotherMessage)
            for peer in dht.closerPeers {
                let peerID = try PeerID(fromBytesID: peer.id.bytes)
                let addresses = try peer.addrs.map { try Multiaddr($0) }
                print(PeerInfo(peer: peerID, addresses: addresses))
            }
        } catch {
            print(error)
        }
    }
}


/// Attempts to print the current shape of the netwrok
/// ```
///          ⎡⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎺⎤
/// (Actual) | PIDX ... PIDY ... PIDZ |
///          |------------------------|
/// (Sorted) | PID0 ... PID1 ... PIDN |
///          ⎣⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎽⎦
///
/// ```
/// To determine the actual ordering of the network we can ask each node for the peers to it's left and the peers to it's right in it's dht
internal func printNetwork(_ nodes:[KadDHT.Node]) {
    printNormalizedCurrentOrderingEstimate(nodes)

    //let nodeCount = nodes.count
    //let target = KadDHT.Key(preHashedBytes: Array<UInt8>(repeating: 0, count: 32))
    let sorted = nodes.sortedAbsolutely()
    print( "Sorted | " + sorted.map { $0.peerID.description }.joined(separator: " | ") )
    //print( sorted.map { "<kad.Key \(KadDHT.Key($0.peerID).bytes.asString(base: .base58btc).prefix(6))>" }.joined(separator: " | "))
    //print( sorted.map { "<kad.Key \(KadDHT.Key($0.peerID).bytes.asString(base: .base16).prefix(6))>" }.joined(separator: " | "))

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
    
    // Determine Network Order Score
    var score = 0
    let currentOrdering = normalizedCurrentOrdering(nodes)
    for i in 0..<currentOrdering.count {
        let node = currentOrdering[i]
        for j in 0..<sorted.count {
            if node.peerID == sorted[j].peerID {
                score += abs(i - j)
                break
            }
        }
    }
    
    print("Total Network Order Score: \(score) (lower is better)")
    
}

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
            return KadDHT.Key(preHashedBytes: Array<UInt8>(repeating: 0, count: lhsSummation.count)).compareDistancesFromSelf(to: KadDHT.Key(preHashedBytes: lhsSummation), and: KadDHT.Key(preHashedBytes: rhsSummation)) == .firstKey
        }
    }.reversed()
}

func printNormalizedCurrentOrderingEstimate(_ nodes:[KadDHT.Node]) {
    print("Actual | " + normalizedCurrentOrdering(nodes).map { $0.peerID.description }.joined(separator: " | ") )
}
