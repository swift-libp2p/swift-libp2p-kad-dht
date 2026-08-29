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

import LibP2P

extension KadDHT {

    struct NodeMetrics {
        var history: [(date: TimeInterval, event: KadDHT.Event)] = []
        private let record: Bool

        init(record: Bool = true) {
            self.record = record
        }

        mutating func add(event: KadDHT.Event) {
            if self.record { self.history.append((Date().timeIntervalSince1970, event)) }
        }
    }

    enum Event {
        case initialized
        case peerDiscovered(PeerInfo)
        case dialedPeer(Multiaddr, Bool)
        case addedPeer(PeerInfo)
        case droppedPeer(PeerInfo, DropPeerReason)
        case queriedPeer(PeerInfo, Query)
        case queryResponse(PeerInfo, Response)
        case deinitialized
    }

    enum DropPeerReason {
        case closerPeerFound
        case maxLatencyExceeded
        case brokenConnection
        case failedToAdd
    }

    enum Errors: Error {
        case AttemptedToStoreNonCodableValue
        case DecodingErrorInvalidLength
        case DecodingErrorInvalidType
        case connectionDropped
        case connectionTimedOut
        case unknownPeer
        case noCloserPeers
        case encodingError
        case noNetwork
        case invalidCID
        case maxLookupDepthExceeded
        case lookupPeersExhausted
        case alreadyPerformingLookup
        case cannotCallHeartbeatWhileNodeIsInAutoUpdateMode
        case noDialableAddressesForPeer
        case clientModeDoesNotAcceptInboundTraffic
        case cantPutValueWithoutExternallyDialableAddress
        case peerIDMultiaddrEncapsulationFailed
        case notSupported
    }

    public struct NodeOptions {
        public let connectionTimeout: TimeAmount
        public let maxConcurrentConnections: Int
        public let bucketSize: Int
        public let maxPeers: Int
        public let maxKeyValueStoreSize: Int
        public let maxProviderStoreSize: Int
        public let supportLocalNetwork: Bool

        /// The longest we'll hold a value record, measured from the `timeReceived` we stamped on it.
        ///
        /// A record that outlives this is dropped on read and swept by the value GC pass, so a
        /// publisher has to re-put more often than this for the value to stay resolvable.
        ///
        /// 48 hours, matching go's `DefaultMaxRecordAge = 48 * time.Hour`.
        public let maxRecordAge: TimeAmount

        /// How often the value store is swept for aged-out records.
        ///
        /// 24 hours, matching go's `DefaultValueGCInterval = 24 * time.Hour`.
        public let valueGCInterval: TimeAmount

        public init(
            connectionTimeout: TimeAmount = .seconds(4),
            maxConcurrentConnections: Int = 4,
            bucketSize: Int = 20,
            maxPeers: Int = 100,
            maxKeyValueStoreEntries: Int = 100,
            maxProviderStoreSize: Int = 10_000,
            supportLocalNetwork: Bool = false,
            maxRecordAge: TimeAmount = .hours(48),
            valueGCInterval: TimeAmount = .hours(24)
        ) {
            self.connectionTimeout = connectionTimeout
            self.maxConcurrentConnections = maxConcurrentConnections
            self.bucketSize = bucketSize
            self.maxPeers = maxPeers
            self.maxKeyValueStoreSize = maxKeyValueStoreEntries
            self.maxProviderStoreSize = maxProviderStoreSize
            self.supportLocalNetwork = supportLocalNetwork
            self.maxRecordAge = maxRecordAge
            self.valueGCInterval = valueGCInterval
        }

        public static var `default`: NodeOptions {
            .init()
        }
    }
}

/// If we abstract the Application into a Network protocol then we can create a FauxNetwork for testing purposes...
protocol Network {
    var logger: Logger { get }
    var eventLoopGroup: EventLoopGroup { get }
    var peerID: PeerID { get }
    var listenAddresses: [Multiaddr] { get }
    var peers: PeerStore { get }
    func registerProtocol(_ proto: SemVerProtocol) throws
    func dialableAddress(_ mas: [Multiaddr], externalAddressesOnly: Bool, on: EventLoop) -> EventLoopFuture<[Multiaddr]>
    func newRequest(
        to ma: Multiaddr,
        forProtocol proto: String,
        withRequest request: Data,
        style: Application.SingleRequest.Style,
        withHandlers handlers: HandlerConfig,
        andMiddleware middleware: MiddlewareConfig,
        withTimeout timeout: TimeAmount
    ) -> EventLoopFuture<Data>
}
