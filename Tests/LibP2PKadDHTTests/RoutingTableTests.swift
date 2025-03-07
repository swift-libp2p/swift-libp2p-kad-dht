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

import CryptoSwift
import LibP2P
import LibP2PCrypto
import XCTest

@testable import LibP2PKadDHT

class RoutingTableTests: XCTestCase {

    override func setUpWithError() throws {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDownWithError() throws {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testBucketMinimum() {
        var bucket = Bucket()

        let pid3 = RandomDHTPeer()  // We initialize pid3 first so it has the oldest LastUsefulAt value
        let pid1 = RandomDHTPeer()  // Then pid1
        let pid2 = RandomDHTPeer()  // Then pid2

        /// pid1 should be first
        bucket.pushFront(pid1)
        /// Sorting LastUsefulAt from oldest to newest, pid1 should be the oldest at index 0
        XCTAssertEqual(
            pid1,
            bucket.min(by: { lhs, rhs in
                lhs.lastUsefulAt! < rhs.lastUsefulAt!
            })
        )

        /// pid1 should still be first
        bucket.pushFront(pid2)
        /// Sorting LastUsefulAt from oldest to newest, pid1 should still be the oldest at index 0
        XCTAssertEqual(
            pid1,
            bucket.min(by: { lhs, rhs in
                lhs.lastUsefulAt! < rhs.lastUsefulAt!
            })
        )

        /// pid3 should be first now...
        bucket.pushFront(pid3)
        /// Sorting LastUsefulAt from oldest to newest, pid3 should now be the oldest at index 0
        XCTAssertEqual(
            pid3,
            bucket.min(by: { lhs, rhs in
                lhs.lastUsefulAt! < rhs.lastUsefulAt!
            })
        )
    }

    func testBucketUpdateAllWith() {
        var bucket = Bucket()

        let pid1 = RandomDHTPeer()
        let pid2 = RandomDHTPeer()
        let pid3 = RandomDHTPeer()

        /// Peer1
        bucket.pushFront(pid1)
        XCTAssertFalse(bucket[0].replaceable)
        bucket.updateAllWith { peer in
            peer.replaceable = true
        }
        XCTAssertTrue(bucket[0].replaceable)

        /// Peer2
        bucket.pushFront(pid2)
        XCTAssertFalse(bucket[0].replaceable)
        bucket.updateAllWith { peer in
            if peer.id == pid1.id {
                peer.replaceable = false
            } else {
                peer.replaceable = true
            }
        }
        XCTAssertTrue(bucket.getPeer(pid2.id)!.replaceable)
        XCTAssertFalse(bucket.getPeer(pid1.id)!.replaceable)

        /// Peer3
        bucket.pushFront(pid3)
        XCTAssertFalse(bucket.getPeer(pid3.id)!.replaceable)
        bucket.updateAllWith { peer in
            peer.replaceable = true
        }
        XCTAssertTrue(bucket.getPeer(pid1.id)!.replaceable)
        XCTAssertTrue(bucket.getPeer(pid2.id)!.replaceable)
        XCTAssertTrue(bucket.getPeer(pid3.id)!.replaceable)
    }

    func testRoutingTablePrintDescription() throws {
        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let routingTable = try RoutingTable(
            eventloop: elg.next(),
            bucketSize: 1,
            localPeerID: PeerID(.Ed25519),
            latency: .hours(1),
            peerstoreMetrics: [:],
            usefulnessGracePeriod: .hours(1)
        )

        print(routingTable)
        try elg.syncShutdownGracefully()
    }

    func testBasicBucketFunctions() throws {
        let peerCount = 100
        var bucket = Bucket()

        let local = try PeerID(.Ed25519)
        let localID = KadDHT.Key(local)

        let time1 = Date().timeIntervalSince1970
        let time2 = Date().timeIntervalSince1970 + 10

        let peers: [PeerID] = (0..<peerCount).map { _ in try! PeerID(.Ed25519) }
        for peer in peers {
            bucket.pushFront(
                DHTPeerInfo(
                    id: peer,
                    lastUsefulAt: time1,
                    lastSuccessfulOutboundQueryAt: time2,
                    addedAt: time1,
                    dhtID: KadDHT.Key(peer),
                    replaceable: false
                )
            )
        }

        /// Assert that all 100 peers were added to the bucket
        XCTAssertEqual(bucket.peers().count, peerCount)

        /// Modifying a peer in a bucket via unsafeMutablePointer
        let newTime2 = Date().timeIntervalSince1970 + 3600
        let newTime3 = newTime2 + 3600

        /// Select a random peer and make sure it was instantiated correctly
        let randomIndex = Int.random(in: 0..<peerCount)
        bucket.getPeer(peers[randomIndex]) { randomPeer in
            print("Running Modifier on \(randomPeer.id)")
            XCTAssertNotNil(randomPeer)
            XCTAssertEqual(peers[randomIndex], randomPeer.id)
            XCTAssertEqual(KadDHT.Key(peers[randomIndex]), randomPeer.dhtID)
            XCTAssertEqual(randomPeer.lastUsefulAt, time1)
            XCTAssertEqual(randomPeer.lastSuccessfulOutboundQueryAt, time2)

            /// Modify the peer in place...
            randomPeer.lastSuccessfulOutboundQueryAt = newTime2
            randomPeer.lastUsefulAt = newTime3
        }

        /// Ensure our modification were realized
        let randomPeer = bucket.getPeer(peers[randomIndex])
        print("Checking Modification on \(randomPeer!.id)")
        XCTAssertEqual(randomPeer?.lastSuccessfulOutboundQueryAt, newTime2)
        XCTAssertEqual(randomPeer?.lastUsefulAt, newTime3)

        let split = bucket.split(commonPrefixLength: 0, targetID: localID.bytes)

        print("Bucket Count Post Split: \(bucket.count)")
        print("New Bucket (non 0 cpls) Count: \(split.count)")

        print(localID.bytes)

        /// Bucket should be peers with a CPL == 0
        for dhtPeer in bucket.peers() {
            let cpl = local.commonPrefixLength(with: dhtPeer.id)
            //print(dhtPeer.dhtID.bytes)
            XCTAssertEqual(cpl, 0)
        }
        /// Split should contain only peers who's CPL > 0
        for dhtPeer in split.peers() {
            let cpl = local.commonPrefixLength(with: dhtPeer.id)
            print(dhtPeer.dhtID.bytes)
            XCTAssertGreaterThan(cpl, 0)
        }
    }

    func testRandomDHTKeyCPL() throws {
        let local = RandomDHTKey()

        let similar0 = RandomDHTKey(withCPL: 0, wrt: local)
        XCTAssertEqual(local.commonPrefixLength(with: similar0), 0)

        let similar1 = RandomDHTKey(withCPL: 1, wrt: local)
        XCTAssertEqual(local.commonPrefixLength(with: similar1), 1)

        let similar2 = RandomDHTKey(withCPL: 2, wrt: local)
        XCTAssertEqual(local.commonPrefixLength(with: similar2), 2)

        let similar3 = RandomDHTKey(withCPL: 3, wrt: local)
        XCTAssertEqual(local.commonPrefixLength(with: similar3), 3)

        let similar4 = RandomDHTKey(withCPL: 4, wrt: local)
        XCTAssertEqual(local.commonPrefixLength(with: similar4), 4)

        let similar5 = RandomDHTKey(withCPL: 5, wrt: local)
        XCTAssertEqual(local.commonPrefixLength(with: similar5), 5)

        let similar6 = RandomDHTKey(withCPL: 6, wrt: local)
        XCTAssertEqual(local.commonPrefixLength(with: similar6), 6)

        let similar7 = RandomDHTKey(withCPL: 7, wrt: local)
        XCTAssertEqual(local.commonPrefixLength(with: similar7), 7)

        let similar8 = RandomDHTKey(withCPL: 8, wrt: local)
        XCTAssertEqual(local.commonPrefixLength(with: similar8), 8)

        let similar9 = RandomDHTKey(withCPL: 9, wrt: local)
        XCTAssertEqual(local.commonPrefixLength(with: similar9), 9)
    }

    /// - TODO: We're confusing DHTPeer and PeerIDs at the moment... We should update the Routing Table to handle DHTPeers instead of PeerID
    func testBucketGetPeersByCPL() throws {
        let local = RandomDHTPeer()

        print(local.id)
        print(local.dhtID.bytes)

        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        let routingTable = RoutingTable(
            eventloop: elg.next(),
            bucketSize: 2,
            localPeerID: local.id,
            latency: .hours(1),
            peerstoreMetrics: [:],
            usefulnessGracePeriod: .hours(1)
        )

        XCTAssertEqual(0, try routingTable.numberOfPeers(withCommonPrefixLength: 0).wait())
        XCTAssertEqual(0, try routingTable.numberOfPeers(withCommonPrefixLength: 1).wait())

        /// Generate and add a peer with a commonPrefixLength of 1 w.r.t our local peer id
        let peerCPL1 = RandomDHTPeer(withCPL: 1, wrt: local.dhtID)
        XCTAssertTrue(
            try routingTable.addPeer(peerCPL1, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait()
        )
        print("Added peer: \(peerCPL1.dhtID.bytes)")
        print(routingTable)
        XCTAssertEqual(0, try routingTable.numberOfPeers(withCommonPrefixLength: 0).wait())
        XCTAssertEqual(1, try routingTable.numberOfPeers(withCommonPrefixLength: 1).wait())
        XCTAssertEqual(0, try routingTable.numberOfPeers(withCommonPrefixLength: 2).wait())

        /// Generate and add a peer with a commonPrefixLength of 0 w.r.t our local peer id
        let peerCPL0 = RandomDHTPeer(withCPL: 0, wrt: local.dhtID)
        XCTAssertTrue(
            try routingTable.addPeer(peerCPL0, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait()
        )
        print("Added peer: \(peerCPL0.dhtID.bytes)")
        print(routingTable)
        XCTAssertEqual(1, try routingTable.numberOfPeers(withCommonPrefixLength: 0).wait())
        XCTAssertEqual(1, try routingTable.numberOfPeers(withCommonPrefixLength: 1).wait())
        XCTAssertEqual(0, try routingTable.numberOfPeers(withCommonPrefixLength: 2).wait())

        /// Generate and add a peer with a commonPrefixLength of 1 w.r.t our local peer id
        /// Adding a third peer will force a bucket split when our bucketSize param is set to 2
        let peerCPL1_2 = RandomDHTPeer(withCPL: 1, wrt: local.dhtID)
        XCTAssertTrue(
            try routingTable.addPeer(peerCPL1_2, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait()
        )
        print("Added peer: \(peerCPL1_2.dhtID.bytes)")
        print(routingTable)
        XCTAssertEqual(1, try routingTable.numberOfPeers(withCommonPrefixLength: 0).wait())
        XCTAssertEqual(2, try routingTable.numberOfPeers(withCommonPrefixLength: 1).wait())
        XCTAssertEqual(0, try routingTable.numberOfPeers(withCommonPrefixLength: 2).wait())

        /// Generate and add a peer with a commonPrefixLength of 0 w.r.t our local peer id
        let peerCPL0_2 = RandomDHTPeer(withCPL: 0, wrt: local.dhtID)
        XCTAssertTrue(
            try routingTable.addPeer(peerCPL0_2, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait()
        )
        print("Added peer: \(peerCPL0_2.dhtID.bytes)")
        print(routingTable)
        XCTAssertEqual(2, try routingTable.numberOfPeers(withCommonPrefixLength: 0).wait())
        XCTAssertEqual(2, try routingTable.numberOfPeers(withCommonPrefixLength: 1).wait())
        XCTAssertEqual(0, try routingTable.numberOfPeers(withCommonPrefixLength: 2).wait())

        /// Generate and add a peer with a commonPrefixLength of 1 w.r.t our local peer id
        /// Attempting to add a third peer with a CPL of 1 will force a bucket split but will fail to add the peer due to the bucket for CPL 1's being full
        /// And the fact that all of the peers added so far have their replaceable param set to false
        let peerCPL1_3 = RandomDHTPeer(withCPL: 1, wrt: local.dhtID)
        XCTAssertFalse(
            try routingTable.addPeer(peerCPL1_3, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait()
        )
        print(routingTable)

        /// Attempting Again Fails
        let peerCPL1_4 = RandomDHTPeer(withCPL: 1, wrt: local.dhtID)
        XCTAssertFalse(
            try routingTable.addPeer(peerCPL1_4, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait()
        )
        print(routingTable)

        /// Generate and add a peer with a commonPrefixLength of 2 w.r.t our local peer id
        /// Adding this peer should succeed due to having excess capacity in bucket[2]
        let peerCPL2_1 = RandomDHTPeer(withCPL: 2, wrt: local.dhtID)
        XCTAssertTrue(
            try routingTable.addPeer(peerCPL2_1, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait()
        )
        print("Added peer: \(peerCPL2_1.dhtID.bytes)")
        print(routingTable)
        XCTAssertEqual(2, try routingTable.numberOfPeers(withCommonPrefixLength: 0).wait())
        XCTAssertEqual(2, try routingTable.numberOfPeers(withCommonPrefixLength: 1).wait())
        XCTAssertEqual(1, try routingTable.numberOfPeers(withCommonPrefixLength: 2).wait())

        /// TODO: Mark a peer as replaceable and attempt to add a peer in it's place

        try elg.syncShutdownGracefully()
    }

    func testEmptyBucketCollapse() throws {

        let local = RandomDHTPeer()

        print(local.id)
        print(local.dhtID.bytes)

        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { try! elg.syncShutdownGracefully() }

        let routingTable = RoutingTable(
            eventloop: elg.next(),
            bucketSize: 1,
            localPeerID: local.id,
            latency: .hours(1),
            peerstoreMetrics: [:],
            usefulnessGracePeriod: .hours(1)
        )
        routingTable.logLevel = .debug

        /// Generate Peers with CPLs of 0, 1, 2 and 3
        let peer0 = RandomDHTPeer(withCPL: 0, wrt: local.dhtID)
        let peer1 = RandomDHTPeer(withCPL: 1, wrt: local.dhtID)
        let peer2 = RandomDHTPeer(withCPL: 2, wrt: local.dhtID)
        let peer3 = RandomDHTPeer(withCPL: 3, wrt: local.dhtID)

        /// Removing a peer on an empty bucket shouldn't throw an error
        XCTAssertNoThrow(try routingTable.removePeer(peer0).wait())

        /// Add and then remove peer0, this should create a bucket[0]
        XCTAssertTrue(try routingTable.addPeer(peer0, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        /// Removing the peer from the bucket shouldn't cause the bucket to disappear due to it being the only bucket we have
        XCTAssertTrue(try routingTable.removePeer(peer0).wait())
        XCTAssertEqual(try routingTable.bucketCount.wait(), 1)
        XCTAssertEqual(try routingTable.getPeerInfos().wait().count, 0)
        print(routingTable)

        /// Add peer with CPL 0 and 1 and verify that our RoutingTable has 2 buckets
        XCTAssertTrue(try routingTable.addPeer(peer0, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertTrue(try routingTable.addPeer(peer1, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertEqual(try routingTable.bucketCount.wait(), 2)
        XCTAssertEqual(try routingTable.getPeerInfos().wait().count, 2)
        print(routingTable)

        /// Removing a peer from the last bucket should cause it to collapse
        XCTAssertTrue(try routingTable.removePeer(peer1).wait())
        XCTAssertEqual(try routingTable.bucketCount.wait(), 1)
        XCTAssertEqual(try routingTable.getPeerInfos().wait().count, 1)
        XCTAssertTrue(try routingTable.getPeerInfos().wait().contains(where: { $0.id == peer0.id }))
        //print(try routingTable.getPeerInfos().wait())
        print(routingTable)

        /// Add peer with CPL 1 again and verify that our RoutingTable has 2 buckets
        XCTAssertTrue(try routingTable.addPeer(peer1, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertEqual(try routingTable.bucketCount.wait(), 2)
        XCTAssertEqual(try routingTable.getPeerInfos().wait().count, 2)
        print(routingTable)

        /// Remove a peer from the second to last bucket (the first bucket in this case) and ensure it collapses as expected
        XCTAssertTrue(try routingTable.removePeer(peer0).wait())
        XCTAssertEqual(try routingTable.bucketCount.wait(), 1)
        XCTAssertEqual(try routingTable.getPeerInfos().wait().count, 1)
        XCTAssertTrue(try routingTable.getPeerInfos().wait().contains(where: { $0.id == peer1.id }))
        print(routingTable)

        /// Add all four peers and expect 4 buckets (peer1 shouldn't be added twice)
        XCTAssertTrue(try routingTable.addPeer(peer0, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertFalse(try routingTable.addPeer(peer1, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertTrue(try routingTable.addPeer(peer2, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertTrue(try routingTable.addPeer(peer3, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertEqual(try routingTable.bucketCount.wait(), 4)
        XCTAssertEqual(try routingTable.getPeerInfos().wait().count, 4)
        print(routingTable)

        /// Make sure we can find all of the peers
        XCTAssertNotNil(try routingTable.find(id: peer0).wait())
        XCTAssertNotNil(try routingTable.find(id: peer1).wait())
        XCTAssertNotNil(try routingTable.find(id: peer2).wait())
        XCTAssertNotNil(try routingTable.find(id: peer3).wait())
        XCTAssertEqual(try routingTable.nearest(1, peersTo: peer0).wait().first?.id, peer0.id)
        XCTAssertEqual(try routingTable.nearest(1, peersTo: peer1).wait().first?.id, peer1.id)
        XCTAssertEqual(try routingTable.nearest(1, peersTo: peer2).wait().first?.id, peer2.id)
        XCTAssertEqual(try routingTable.nearest(1, peersTo: peer3).wait().first?.id, peer3.id)

        let nearbyPeers = try routingTable.nearest(4, peersTo: peer1).wait()
        for dhtPeer in nearbyPeers { print(dhtPeer.id) }
        XCTAssertEqual(nearbyPeers.count, 4)
        XCTAssertTrue(nearbyPeers.contains(where: { $0.id == peer0.id }))
        XCTAssertTrue(nearbyPeers.contains(where: { $0.id == peer1.id }))
        XCTAssertTrue(nearbyPeers.contains(where: { $0.id == peer2.id }))
        XCTAssertTrue(nearbyPeers.contains(where: { $0.id == peer3.id }))

        /// Removing peer 1, 2 and 3 should result in only a single bucket
        /// removing peer1 first results in an empty internal bucket without triggering a collapse
        XCTAssertTrue(try routingTable.removePeer(peer1).wait())
        /// 📒 --------------------------------- 📒
        /// Routing Table [<peer.ID dJwpME>]
        /// Bucket Count: 4 buckets of size: 1
        /// Total Peers: 3
        /// b[0] = [0]
        /// b[1] = []
        /// b[2] = [2]
        /// b[3] = [3]
        /// ---------------------------------------
        /// removing peer2 next causes a cascading collapse that ends up 'removing' bucket 1 and 2
        XCTAssertTrue(try routingTable.removePeer(peer2).wait())
        /// 📒 --------------------------------- 📒
        /// Routing Table [<peer.ID dJwpME>]
        /// Bucket Count: 2 buckets of size: 1
        /// Total Peers: 2
        /// b[0] = [0]
        /// b[1] = [3]
        /// ---------------------------------------
        /// removing peer3 from the last bucket also deletes it resulting in a single peer0 in bucket[0]
        XCTAssertTrue(try routingTable.removePeer(peer3).wait())
        /// 📒 --------------------------------- 📒
        /// Routing Table [<peer.ID dJwpME>]
        /// Bucket Count: 1 buckets of size: 1
        /// Total Peers: 1
        /// b[0] = [0]
        /// ---------------------------------------
        XCTAssertEqual(try routingTable.bucketCount.wait(), 1)
        XCTAssertEqual(try routingTable.getPeerInfos().wait().count, 1)
        XCTAssertTrue(try routingTable.getPeerInfos().wait().contains(where: { $0.id == peer0.id }))
        print(routingTable)

        /// Add back all four peers
        XCTAssertFalse(try routingTable.addPeer(peer0, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertTrue(try routingTable.addPeer(peer1, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertTrue(try routingTable.addPeer(peer2, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertTrue(try routingTable.addPeer(peer3, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertEqual(try routingTable.bucketCount.wait(), 4)
        XCTAssertEqual(try routingTable.getPeerInfos().wait().count, 4)
        print(routingTable)

        /// Removing peer 1, causing an empty interior bucket, doesn't trigger a collapse (ew should still have 4 buckets) (see above for more info)
        XCTAssertTrue(try routingTable.removePeer(peer1).wait())
        XCTAssertEqual(try routingTable.bucketCount.wait(), 4)
        XCTAssertEqual(try routingTable.getPeerInfos().wait().count, 3)
        XCTAssertFalse(try routingTable.getPeerInfos().wait().contains(where: { $0.id == peer1.id }))
        print(routingTable)
    }

    func testRemovePeer() throws {
        let local = RandomDHTPeer()

        print(local.id)
        print(local.dhtID.bytes)

        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            print("Shutting Down EventLoopGroup")
            try! elg.syncShutdownGracefully()
        }

        let routingTable = RoutingTable(
            eventloop: elg.next(),
            bucketSize: 2,
            localPeerID: local.id,
            latency: .hours(1),
            peerstoreMetrics: [:],
            usefulnessGracePeriod: .hours(1)
        )

        /// Generate random Peers
        let peer0 = RandomDHTPeer()
        let peer1 = RandomDHTPeer()

        /// Add the peers to the routing table
        XCTAssertTrue(try routingTable.addPeer(peer0, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertTrue(try routingTable.addPeer(peer1, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())

        /// Ensure the peers were added
        XCTAssertTrue(try routingTable.getPeerInfos().wait().contains(where: { $0.id == peer0.id }))
        XCTAssertTrue(try routingTable.getPeerInfos().wait().contains(where: { $0.id == peer1.id }))
        XCTAssertNotNil(try routingTable.find(id: peer0).wait())
        XCTAssertNotNil(try routingTable.find(id: peer1).wait())
        XCTAssertEqual(try routingTable.getPeerInfos().wait().count, 2)
        XCTAssertEqual(try routingTable.bucketCount.wait(), 1)

        /// Remove a peer from the routing table
        XCTAssertNotNil(try routingTable.find(id: peer1).wait())
        XCTAssertTrue(try routingTable.removePeer(peer1).wait())
        XCTAssertNil(try routingTable.find(id: peer1).wait())
        XCTAssertEqual(try routingTable.getPeerInfos().wait().count, 1)
        XCTAssertEqual(try routingTable.bucketCount.wait(), 1)
        XCTAssertFalse(try routingTable.getPeerInfos().wait().contains(where: { $0.id == peer1.id }))
        XCTAssertTrue(try routingTable.getPeerInfos().wait().contains(where: { $0.id == peer0.id }))
    }

    func testTableCallbackClosures() throws {
        let local = RandomDHTPeer()

        print(local.id)
        print(local.dhtID.bytes)

        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            print("Shutting Down EventLoopGroup")
            try! elg.syncShutdownGracefully()
        }

        let routingTable = RoutingTable(
            eventloop: elg.next(),
            bucketSize: 10,
            localPeerID: local.id,
            latency: .hours(1),
            peerstoreMetrics: [:],
            usefulnessGracePeriod: .hours(1)
        )
        routingTable.logLevel = .warning

        var peersSet: [PeerID] = []
        /// install our callback handlers
        routingTable.peerAddedHandler = { peer in
            print("Peer Added: \(peer)")
            peersSet.append(peer)
        }
        routingTable.peerRemovedHandler = { peer in
            print("Peer Removed: \(peer)")
            peersSet.removeAll(where: { $0.id == peer.id })
        }

        /// Generate random Peers
        let peerCount = 100
        let peers: [DHTPeerInfo] = (0..<peerCount).map { _ in RandomDHTPeer() }

        /// Make sure our callbacks work...
        XCTAssertTrue(try routingTable.addPeer(peers[0], isQueryPeer: true).wait())
        XCTAssertTrue(peersSet.contains(peers[0].id))
        if peersSet.count != 1 { XCTFail("Our peerAdded callback didn't work ") }

        XCTAssertTrue(try routingTable.removePeer(peers[0]).wait())
        XCTAssertFalse(peersSet.contains(peers[0].id))
        if peersSet.count != 0 { XCTFail("Our peerRemoved callback didn't work") }

        /// Add all the peers!
        let _ = try peers.map {
            try routingTable.addPeer($0, isQueryPeer: true)
        }.flatten(on: elg.next()).wait()

        print(routingTable)

        /// Check to make sure that each peer that was added to the routingTable was added to our peersSet array
        let out = try routingTable.getPeerInfos().wait()
        XCTAssertEqual(out.count, peersSet.count)
        for peer in out {
            XCTAssertTrue(peersSet.contains(peer.id))
            XCTAssertTrue(try routingTable.removePeer(peer).wait())
            /// removing the peer should trigger our peerRemovalHandler which will remove the peer from our peersSet array
        }

        /// Ensure both our peersSet and routingTable are empty after the peer removal
        XCTAssertEqual(peersSet.count, 0)
        XCTAssertEqual(try routingTable.getPeerInfos().wait().count, 0)
    }

    func testAddLargeAmountOfPeersAndLoad() throws {
        let local = RandomDHTPeer()

        print(local.id)
        print(local.dhtID.bytes)

        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            print("Shutting Down EventLoopGroup")
            try! elg.syncShutdownGracefully()
        }

        let routingTable = RoutingTable(
            eventloop: elg.next(),
            bucketSize: 20,
            localPeerID: local.id,
            latency: .hours(1),
            peerstoreMetrics: [:],
            usefulnessGracePeriod: .hours(1)
        )
        routingTable.logLevel = .warning

        /// Generate random Peers
        let peerCount = 500
        let peers: [DHTPeerInfo] = (0..<peerCount).map { _ in RandomDHTPeer() }

        /// Add random peers from the list
        for _ in (0..<50000) {
            guard let randPeer = peers.randomElement() else { continue }
            XCTAssertNoThrow(try routingTable.addPeer(randPeer, isQueryPeer: true).wait())
        }

        print(routingTable)

        for _ in (0..<100) {
            let rp = RandomPeerID()
            XCTAssertEqual(try routingTable.nearest(5, peersTo: rp).wait().count, 5)
        }
    }

    func testTableFind() throws {
        let local = RandomDHTPeer()

        print(local.id)
        print(local.dhtID.bytes)

        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            print("Shutting Down EventLoopGroup")
            try! elg.syncShutdownGracefully()
        }

        let routingTable = RoutingTable(
            eventloop: elg.next(),
            bucketSize: 10,
            localPeerID: local.id,
            latency: .hours(1),
            peerstoreMetrics: [:],
            usefulnessGracePeriod: .hours(1)
        )
        routingTable.logLevel = .warning

        /// Generate random Peers
        let peerCount = 5
        let peers: [DHTPeerInfo] = (0..<peerCount).map { _ in RandomDHTPeer() }

        /// Add random peers from the list
        for peer in peers {
            XCTAssertNoThrow(try routingTable.addPeer(peer, isQueryPeer: true).wait())
        }

        print(routingTable)

        for peer in peers {
            XCTAssertTrue(try routingTable.getPeerInfos().wait().contains(where: { $0.id == peer.id }))
            XCTAssertNotNil(try routingTable.find(id: peer).wait())
            //XCTAssertEqual(try routingTable.nearestPeer(to: peer).wait()!.dhtID, peer.dhtID)
        }
    }

    func testUpdateLastSuccessfulOutboundQueryAt() throws {
        let local = RandomDHTPeer()

        print(local.id)
        print(local.dhtID.bytes)

        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            print("Shutting Down EventLoopGroup")
            try! elg.syncShutdownGracefully()
        }

        let routingTable = RoutingTable(
            eventloop: elg.next(),
            bucketSize: 10,
            localPeerID: local.id,
            latency: .hours(1),
            peerstoreMetrics: [:],
            usefulnessGracePeriod: .hours(1)
        )
        routingTable.logLevel = .warning

        /// Generate a random peer and add them to the table
        let peer = RandomDHTPeer()
        let peer2 = RandomDHTPeer()
        XCTAssertTrue(try routingTable.addPeer(peer, isQueryPeer: true).wait())
        XCTAssertEqual(try routingTable.find(id: peer).wait()?.id, peer.id)

        /// Update the last successful outbound query for the peer
        let time = Date().timeIntervalSince1970 + 3600
        XCTAssertTrue(try routingTable.updateLastSuccessfulOutboundQuery(at: time, for: peer.id).wait())
        XCTAssertFalse(try routingTable.updateLastSuccessfulOutboundQuery(at: time, for: peer2).wait())

        /// Ensure the update stuck
        XCTAssertEqual(try routingTable.find(id: peer).wait()?.lastSuccessfulOutboundQueryAt, time)

        print(routingTable)

        /// Now do it a bunch of times
        for i in 0..<1000 {
            let newTime = Date().timeIntervalSince1970.advanced(by: Double(i))
            XCTAssertTrue(try routingTable.updateLastSuccessfulOutboundQuery(at: newTime, for: peer).wait())

            if i % 10 == 0 {
                XCTAssertEqual(try routingTable.find(id: peer).wait()?.lastSuccessfulOutboundQueryAt, newTime)
            }
        }
    }

    func testUpdateLastUsefulAt() throws {
        let local = RandomDHTPeer()

        print(local.id)
        print(local.dhtID.bytes)

        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            print("Shutting Down EventLoopGroup")
            try! elg.syncShutdownGracefully()
        }

        let routingTable = RoutingTable(
            eventloop: elg.next(),
            bucketSize: 10,
            localPeerID: local.id,
            latency: .hours(1),
            peerstoreMetrics: [:],
            usefulnessGracePeriod: .hours(1)
        )
        routingTable.logLevel = .warning

        /// Generate a random peer and add them to the table
        let peer = RandomDHTPeer()
        let peer2 = RandomDHTPeer()
        XCTAssertTrue(try routingTable.addPeer(peer, isQueryPeer: true).wait())
        XCTAssertEqual(try routingTable.find(id: peer).wait()?.id, peer.id)

        /// Update the last useful at time for the peer
        let time = Date().timeIntervalSince1970 + 3600
        XCTAssertTrue(try routingTable.updateLastUseful(at: time, for: peer).wait())
        XCTAssertFalse(try routingTable.updateLastUseful(at: time, for: peer2).wait())

        /// Ensure the update stuck
        XCTAssertEqual(try routingTable.find(id: peer).wait()?.lastUsefulAt, time)

        /// Now do it a bunch of times
        for i in 0..<1000 {
            let newTime = Date().timeIntervalSince1970.advanced(by: Double(i))
            XCTAssertTrue(try routingTable.updateLastUseful(at: newTime, for: peer).wait())

            if i % 10 == 0 {
                XCTAssertEqual(try routingTable.find(id: peer).wait()?.lastUsefulAt, newTime)
            }
        }
    }

    func testAddReplaceableAndNonQueryPeers() throws {
        let local = RandomDHTPeer()

        print(local.id)
        print(local.dhtID.bytes)

        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            print("Shutting Down EventLoopGroup")
            try! elg.syncShutdownGracefully()
        }

        let routingTable = RoutingTable(
            eventloop: elg.next(),
            bucketSize: 2,
            localPeerID: local.id,
            latency: .hours(1),
            peerstoreMetrics: [:],
            usefulnessGracePeriod: .hours(1)
        )
        routingTable.logLevel = .trace

        /// Generate and Add two peers to saturate the first bucket
        let peer1 = RandomDHTPeer(withCPL: 0, wrt: local.dhtID, isReplaceable: false)
        let peer2 = RandomDHTPeer(withCPL: 0, wrt: local.dhtID, isReplaceable: true)
        XCTAssertTrue(try routingTable.addPeer(peer1, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertTrue(try routingTable.addPeer(peer2, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertEqual(try routingTable.bucketCount.wait(), 1)
        XCTAssertEqual(try routingTable.getPeerInfos().wait().count, 2)
        XCTAssertEqual(try routingTable.find(id: peer1).wait()?.id, peer1.id)
        XCTAssertEqual(try routingTable.find(id: peer2).wait()?.id, peer2.id)
        print(routingTable)

        /// Generate a third peer and attempt to add to table, this should work because peer2 is replaceable
        let peer3 = RandomDHTPeer(withCPL: 0, wrt: local.dhtID, isReplaceable: false)
        XCTAssertTrue(try routingTable.addPeer(peer3, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertEqual(try routingTable.getPeerInfos().wait().count, 2)
        XCTAssertEqual(try routingTable.find(id: peer1).wait()?.id, peer1.id)
        XCTAssertEqual(try routingTable.find(id: peer3).wait()?.id, peer3.id)
        XCTAssertNil(try routingTable.find(id: peer2).wait())
        XCTAssertEqual(try routingTable.bucketCount.wait(), 1)
        print(routingTable)

        /// Generate a fourth peer with CPL = 0 and attempt to add to table, this should fail due to max number of CPL = 0 peers, and no replaceable peer
        let peer4 = RandomDHTPeer(withCPL: 0, wrt: local.dhtID, isReplaceable: false)
        XCTAssertFalse(try routingTable.addPeer(peer4, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertEqual(try routingTable.getPeerInfos().wait().count, 2)
        XCTAssertEqual(try routingTable.find(id: peer1).wait()?.id, peer1.id)
        XCTAssertEqual(try routingTable.find(id: peer3).wait()?.id, peer3.id)
        XCTAssertNil(try routingTable.find(id: peer4).wait())
        // The attempt to add another CPL = 0 Peer results in a nextBucket operation being invoked,
        // so we should have 2 buckets, the last one being empty...
        XCTAssertEqual(try routingTable.bucketCount.wait(), 2)
        print(routingTable)

        /// Generate a fifth peer with CPL = 1 and attempt to add to table, this should succeed because we have excess capcity for peers with CPL = 1 (2 slots available at the moment)
        let peer5 = RandomDHTPeer(withCPL: 1, wrt: local.dhtID, isReplaceable: false)
        XCTAssertTrue(try routingTable.addPeer(peer5, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertEqual(try routingTable.getPeerInfos().wait().count, 3)
        XCTAssertEqual(try routingTable.find(id: peer1).wait()?.id, peer1.id)
        XCTAssertEqual(try routingTable.find(id: peer3).wait()?.id, peer3.id)
        XCTAssertEqual(try routingTable.find(id: peer5).wait()?.id, peer5.id)
        XCTAssertEqual(try routingTable.bucketCount.wait(), 2)
        print(routingTable)

        /// Generate a sixth peer with CPL = 3 and isQueryPeer = false and attempt to add to table, this should work
        let peer6 = RandomDHTPeer(withCPL: 3, wrt: local.dhtID, isReplaceable: false)
        XCTAssertTrue(try routingTable.addPeer(peer6, isQueryPeer: false, replacementStrategy: .anyReplaceable).wait())
        XCTAssertEqual(try routingTable.getPeerInfos().wait().count, 4)
        XCTAssertEqual(try routingTable.find(id: peer1).wait()?.id, peer1.id)
        XCTAssertEqual(try routingTable.find(id: peer3).wait()?.id, peer3.id)
        XCTAssertEqual(try routingTable.find(id: peer5).wait()?.id, peer5.id)
        XCTAssertEqual(try routingTable.find(id: peer6).wait()?.id, peer6.id)
        XCTAssertEqual(try routingTable.find(id: peer6).wait()?.lastUsefulAt, nil)
        XCTAssertEqual(try routingTable.bucketCount.wait(), 2)
        print(routingTable)
    }

    func testMarkAllPeersIrreplaceable() throws {
        let local = RandomDHTPeer()

        print(local.id)
        print(local.dhtID.bytes)

        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            print("Shutting Down EventLoopGroup")
            try! elg.syncShutdownGracefully()
        }

        let bucketSize: Int = 20
        let routingTable = RoutingTable(
            eventloop: elg.next(),
            bucketSize: bucketSize,
            localPeerID: local.id,
            latency: .hours(1),
            peerstoreMetrics: [:],
            usefulnessGracePeriod: .hours(1)
        )
        routingTable.logLevel = .warning

        let peerCount = 50
        let peers = (0..<peerCount).map { _ in
            RandomDHTPeer(withCPL: Int.random(in: 0...8), wrt: local.dhtID, isReplaceable: true)
        }

        for peer in peers {
            XCTAssertNoThrow(try routingTable.addPeer(peer, isQueryPeer: true).wait())
        }

        print(routingTable)

        /// Get a list of all the peers that made it into our table
        let peerInfos = try routingTable.getPeerInfos().wait()
        XCTAssertGreaterThanOrEqual(peerInfos.count, min(bucketSize, peerCount))
        XCTAssertGreaterThanOrEqual(peerInfos.count, 1)

        /// Ensure all the peers in the table are still marked as replaceable
        for pi in peerInfos {
            XCTAssertEqual(try routingTable.find(id: pi).wait()?.replaceable, true)
        }

        /// Mark all peers irreplaceable
        try routingTable.markAllPeersIrreplaceable().wait()

        /// Ensure all the peers in our table are now marked as irreplaceable
        for pi in peerInfos {
            XCTAssertEqual(try routingTable.find(id: pi).wait()?.replaceable, false)
        }

        /// Mark all peers replaceable
        try routingTable.markAllPeersReplaceable().wait()

        /// Ensure all the peers in our table are now marked as replaceable
        for pi in peerInfos {
            XCTAssertEqual(try routingTable.find(id: pi).wait()?.replaceable, true)
        }
    }

    func testFindMultiplePeers() throws {
        let local = RandomDHTPeer()

        print(local.id)
        print(local.dhtID.bytes)

        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            print("Shutting Down EventLoopGroup")
            try! elg.syncShutdownGracefully()
        }

        let bucketSize: Int = 20
        let routingTable = RoutingTable(
            eventloop: elg.next(),
            bucketSize: bucketSize,
            localPeerID: local.id,
            latency: .hours(1),
            peerstoreMetrics: [:],
            usefulnessGracePeriod: .hours(1)
        )
        routingTable.logLevel = .warning

        let peerCount = 50
        let peers = (0..<peerCount).map { _ in
            RandomDHTPeer(withCPL: Int.random(in: 0...4), wrt: local.dhtID, isReplaceable: true)
        }

        for peer in peers {
            XCTAssertNoThrow(try routingTable.addPeer(peer, isQueryPeer: true).wait())
        }

        print(routingTable)

        /// The number of closest peers we're going to request
        let expectedNearbyPeers: Int = 25

        XCTAssertGreaterThanOrEqual(try routingTable.getPeerInfos().wait().count, expectedNearbyPeers)

        print("Searching for the \(expectedNearbyPeers) closest peers to \(peers.first!.id)")
        routingTable.logLevel = .debug
        let nearbyPeers = try routingTable.nearest(expectedNearbyPeers, peersTo: peers.first!).wait()
        XCTAssertEqual(nearbyPeers.count, expectedNearbyPeers)
    }

    func testPeerRemovedNotificationOnEviction() throws {
        let local = RandomDHTPeer()

        print(local.id)
        print(local.dhtID.bytes)

        let elg = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            print("Shutting Down EventLoopGroup")
            try! elg.syncShutdownGracefully()
        }

        let routingTable = RoutingTable(
            eventloop: elg.next(),
            bucketSize: 1,
            localPeerID: local.id,
            latency: .hours(1),
            peerstoreMetrics: [:],
            usefulnessGracePeriod: .hours(1)
        )
        routingTable.logLevel = .info

        var peersSet: [PeerID] = []
        /// install our callback handlers
        routingTable.peerAddedHandler = { peer in
            print("Peer Added: \(peer)")
            peersSet.append(peer)
        }
        routingTable.peerRemovedHandler = { peer in
            print("Peer Removed: \(peer)")
            peersSet.removeAll(where: { $0.id == peer.id })
        }

        /// Generate two random peers
        let peer1 = RandomDHTPeer(withCPL: 0, wrt: local.dhtID, isReplaceable: false)
        let peer2 = RandomDHTPeer(withCPL: 0, wrt: local.dhtID, isReplaceable: false)

        XCTAssertEqual(local.dhtID.commonPrefixLength(with: peer1.dhtID), 0)
        XCTAssertEqual(local.dhtID.commonPrefixLength(with: peer2.dhtID), 0)

        /// Add the first peer, should work fine due to excess capacity
        XCTAssertTrue(try routingTable.addPeer(peer1, isQueryPeer: true).wait())
        XCTAssertNotNil(try routingTable.find(id: peer1).wait())
        XCTAssertTrue(peersSet.contains(where: { $0.id == peer1.id }))

        /// Try and add the second peer, this should fail due to max capacity
        XCTAssertFalse(try routingTable.addPeer(peer2, isQueryPeer: true).wait())
        XCTAssertNil(try routingTable.find(id: peer2).wait())
        XCTAssertFalse(peersSet.contains(where: { $0.id == peer2.id }))

        /// Mark all peers (peer1) as replaceable
        try routingTable.markAllPeersReplaceable().wait()

        /// Try and add the second peer again, this should now succeed due to peer1 being marked as replaceable
        XCTAssertTrue(try routingTable.addPeer(peer2, isQueryPeer: true, replacementStrategy: .anyReplaceable).wait())
        XCTAssertNotNil(try routingTable.find(id: peer2).wait())
        XCTAssertTrue(peersSet.contains(where: { $0.id == peer2.id }))

        /// Make sure peer1 is no longer in the routing table and has also been removed from our peersSet array
        XCTAssertNil(try routingTable.find(id: peer1).wait())
        XCTAssertFalse(peersSet.contains(where: { $0.id == peer1.id }))
    }

}
