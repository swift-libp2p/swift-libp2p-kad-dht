//
//  LookupListTests.swift
//  
//
//  Created by Brandon Toms on 4/30/22.
//

import XCTest
import LibP2P
import CryptoSwift
import LibP2PCrypto
@testable import LibP2PKadDHT

class LookupListTests: XCTestCase {

    override func setUpWithError() throws {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDownWithError() throws {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    /// Lookup Lists
    func testLookupListInit() throws {
        let list = try LookupList(id: PeerID(), capacity: 20)

        XCTAssertEqual(list.capacity, 20)
        XCTAssertEqual(list.all().count, 0)
    }

    func testLookupListInsert() throws {
        let ourID = try PeerID(.Ed25519)
        let list = LookupList(id: ourID, capacity: 20)
        let peerCount = 100
        let randomPeers = try (0..<peerCount).map { _ in try generateRandomPeerInfo() }

        for i in 0..<10 {
            XCTAssertTrue(list.insert(randomPeers[i]))
        }

        /// All ten addresses should've been inserted into the list due to excess capacity
        XCTAssertEqual(list.all().count, 10)

        for i in 10..<peerCount {
            list.insert(randomPeers[i])
        }

        /// The list should only contain the max number of peers (capcaity == 20)
        XCTAssertEqual(list.all().count, 20)

        /// And they should be the closest peers to ourselves
        let contacts = list.all()
        for i in 0..<contacts.count - 1 {
            XCTAssertEqual(ourID.compareDistancesFromSelf(to: contacts[i].peer, and: contacts[i + 1].peer), 1)
        }
    }

    func testLookupListNext() throws {
        let list = LookupList(id: KadDHT.Key.ZeroKey, capacity: 20)

        /// Insert a few keys
        let randomPeers = try (0..<5).map { _ in try generateRandomPeerInfo() }
        let sorted = randomPeers.map { $0.peer }.sortedAbsolutely()

        /// Insert the peers in random order
        randomPeers.forEach {
            XCTAssertTrue(list.insert($0))
        }

        /// Ask for the first peer using .next()
        /// This should return the closest peer to our target id which is 0 and therefore the first index in our `sorted` array
        let next = list.next()
        XCTAssertEqual(next!.peer, sorted[0])

        /// If we try and insert that same peer again, we return true to indicate that the peer is in the list, but we shouldn't be handed that peer twice when calling .next()
        list.insert( next! )

        /// Calling next repeatedly should return results in sorted order until each has been processed
        XCTAssertEqual(list.next()!.peer, sorted[1])
        XCTAssertEqual(list.next()!.peer, sorted[2])
        XCTAssertEqual(list.next()!.peer, sorted[3])
        XCTAssertEqual(list.next()!.peer, sorted[4])
        XCTAssertNil( list.next() )
    }
    
}
