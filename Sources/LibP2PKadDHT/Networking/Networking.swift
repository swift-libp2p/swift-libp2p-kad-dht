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
        case alreadyPerformingLookup
        case cannotCallHeartbeatWhileNodeIsInAutoUpdateMode
        case noDialableAddressesForPeer
        case clientModeDoesNotAcceptInboundTraffic
        case cantPutValueWithoutExternallyDialableAddress
        case peerIDMultiaddrEncapsulationFailed
        case notSupported
        case messageTooLarge(bytes: Int, limit: Int)
        case recordTooLarge(bytes: Int, limit: Int)
    }

    public struct NodeOptions: Sendable {
        public let connectionTimeout: TimeAmount

        /// Lookup concurrency (`α`), how many requests a query path keeps in flight.
        public let concurrency: Int

        @available(*, deprecated, renamed: "concurrency")
        public var maxConcurrentConnections: Int { self.concurrency }

        /// Resiliency (`β`), how many of the closest peers must respond before a lookup is done.
        public let resiliency: Int

        /// How many records a value lookup collects before it stops early.
        ///
        /// `0` searches to convergence, which is the spec's default behaviour: every peer among the
        /// closest is asked, and the best of all the answers wins.
        public let quorum: Int

        public let bucketSize: Int
        public let maxPeers: Int
        public let maxKeyValueStoreSize: Int
        public let maxProviderStoreSize: Int
        public let supportLocalNetwork: Bool

        /// Whether an `ADD_PROVIDER` whose `providerPeers` don't carry the sender's addresses may
        /// fall back to the address we observed the stream on.
        ///
        /// Off by default, matching go: a provider record is only as good as its addresses, and the
        /// address an inbound stream arrives on is usually an ephemeral source port nobody can dial
        /// back. A provider that doesn't advertise itself is dropped instead.
        public let acceptObservedProviderAddress: Bool

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
            concurrency: Int = KadDHT.Defaults.concurrency,
            resiliency: Int = KadDHT.Defaults.resiliency,
            quorum: Int = 0,
            bucketSize: Int = KadDHT.Defaults.bucketSize,
            maxPeers: Int = 100,
            maxKeyValueStoreEntries: Int = 100,
            maxProviderStoreSize: Int = 10_000,
            supportLocalNetwork: Bool = false,
            maxRecordAge: TimeAmount = KadDHT.Defaults.maxRecordAge,
            valueGCInterval: TimeAmount = KadDHT.Defaults.valueGCInterval,
            acceptObservedProviderAddress: Bool = false
        ) {
            self.connectionTimeout = connectionTimeout
            self.concurrency = concurrency
            self.resiliency = resiliency
            self.quorum = quorum
            self.bucketSize = bucketSize
            self.maxPeers = maxPeers
            self.maxKeyValueStoreSize = maxKeyValueStoreEntries
            self.maxProviderStoreSize = maxProviderStoreSize
            self.supportLocalNetwork = supportLocalNetwork
            self.maxRecordAge = maxRecordAge
            self.valueGCInterval = valueGCInterval
            self.acceptObservedProviderAddress = acceptObservedProviderAddress
        }

        @available(
            *,
            deprecated,
            message: "`maxConcurrentConnections` is the Kademlia α parameter — use `concurrency:`"
        )
        public init(
            connectionTimeout: TimeAmount = .seconds(4),
            maxConcurrentConnections: Int,
            bucketSize: Int = KadDHT.Defaults.bucketSize,
            maxPeers: Int = 100,
            maxKeyValueStoreEntries: Int = 100,
            maxProviderStoreSize: Int = 10_000,
            supportLocalNetwork: Bool = false,
            maxRecordAge: TimeAmount = KadDHT.Defaults.maxRecordAge,
            valueGCInterval: TimeAmount = KadDHT.Defaults.valueGCInterval
        ) {
            self.init(
                connectionTimeout: connectionTimeout,
                concurrency: maxConcurrentConnections,
                bucketSize: bucketSize,
                maxPeers: maxPeers,
                maxKeyValueStoreEntries: maxKeyValueStoreEntries,
                maxProviderStoreSize: maxProviderStoreSize,
                supportLocalNetwork: supportLocalNetwork,
                maxRecordAge: maxRecordAge,
                valueGCInterval: valueGCInterval
            )
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
