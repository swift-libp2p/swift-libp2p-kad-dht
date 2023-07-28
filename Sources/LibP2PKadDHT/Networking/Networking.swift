//
//  Networking.swift
//
//
//  Created by Brandon Toms on 4/29/22.
//

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
        let connectionTimeout: TimeAmount
        let maxConcurrentConnections: Int
        let bucketSize: Int
        let maxPeers: Int
        let maxKeyValueStoreSize: Int
        let maxProviderStoreSize: Int
        let supportLocalNetwork: Bool

        init(connectionTimeout: TimeAmount = .seconds(4), maxConcurrentConnections: Int = 4, bucketSize: Int = 20, maxPeers: Int = 100, maxKeyValueStoreEntries: Int = 100, maxProviderStoreSize: Int = 10_000, supportLocalNetwork: Bool = false) {
            self.connectionTimeout = connectionTimeout
            self.maxConcurrentConnections = maxConcurrentConnections
            self.bucketSize = bucketSize
            self.maxPeers = maxPeers
            self.maxKeyValueStoreSize = maxKeyValueStoreEntries
            self.maxProviderStoreSize = maxProviderStoreSize
            self.supportLocalNetwork = supportLocalNetwork
        }

        static var `default`: NodeOptions {
            return .init()
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
    func newRequest(to ma: Multiaddr, forProtocol proto: String, withRequest request: Data, style: Application.SingleRequest.Style, withHandlers handlers: HandlerConfig, andMiddleware middleware: MiddlewareConfig, withTimeout timeout: TimeAmount) -> EventLoopFuture<Data>
}
