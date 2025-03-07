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

class Lookup {
    let target: PeerID
    let maxConcurrentRequests: Int
    let list: LookupList

    private let group: EventLoopGroup
    private let eventLoop: EventLoop
    private let host: KadDHT.Node

    private var began: Bool = false
    private var logger: Logger
    private var requestsInProgress: UInt8 = 0
    private var canceled: Bool = false

    private let completionPromise: EventLoopPromise<Void>
    private let sharedELG: Bool

    enum Errors: Error {
        case canceled
    }

    init(
        host: KadDHT.Node,
        target: PeerID,
        concurrentRequests: Int = 1,
        seeds: [PeerInfo] = [],
        groupProvider: Application.EventLoopGroupProvider = .createNew
    ) {
        self.host = host
        self.target = target
        self.maxConcurrentRequests = concurrentRequests
        switch groupProvider {
        case .shared(let group):
            self.group = group
            self.sharedELG = true
        case .createNew:
            self.group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
            self.sharedELG = false
        }
        self.eventLoop = self.group.next()
        self.list = LookupList(id: target, capacity: host.routingTable.bucketSize, seeds: seeds)
        self.began = false
        self.logger = Logger(label: "Lookup[\(UUID().uuidString.prefix(5))]")
        self.logger.logLevel = self.host.logger.logLevel
        self.completionPromise = self.eventLoop.makePromise(of: Void.self)
    }

    deinit { print("Lookup::deinit") }

    private func tearDownSelf() {
        print("Lookup::tearDownSelf")
        if !self.sharedELG {
            try? self.eventLoop.close()
            self.group.shutdownGracefully(queue: .global()) { _ in print("Lookup::ELG shutdown") }
        }
    }

    /// Spawns ⍺ eventLoops and begins a recursive node query on each one until all contacts in our LookupList have been processed...
    func proceed() -> EventLoopFuture<[PeerInfo]> {
        guard self.began == false else { return self.eventLoop.makeFailedFuture(KadDHT.Errors.alreadyPerformingLookup) }
        self.began = true
        self.completionPromise.completeWith(
            (0..<self.maxConcurrentRequests).compactMap { _ -> EventLoopFuture<Void> in
                self.logger.info("Deploying Worker")
                return _recursivelyQueryForTarget(on: self.group.next())
            }.flatten(on: self.eventLoop)
        )
        return self.completionPromise.futureResult.map {
            self.logger.info("Completed!")
            if self.logger.logLevel <= .debug {
                self.logger.debug("Distances to \(self.target.b58String)")
                for peerInfo in self.list.all() {
                    let dist = KadDHT.Key(peerInfo.peer).distanceTo(key: KadDHT.Key(self.target))
                    let clp = dist.commonPrefixLengthBits(with: KadDHT.Key.Zero.bytes)
                    self.logger.debug("[\(clp)] \(dist.asString(base: .base16))")
                }
                self.logger.debug("-------------")
            }
            return self.list.all()
        }.always { _ in
            self.tearDownSelf()
        }
    }

    /// This method will prevent any additional requests/queries from being performed and will return the results of the lookup list.
    func terminateEarly() -> EventLoopFuture<[PeerInfo]> {
        self.eventLoop.submit {
            self.logger.warning("Attempting to terminate lookup early and return current results")
            self.canceled = true
            self.tearDownSelf()
            self.completionPromise.fail(Errors.canceled)
            return self.list.all()
        }
    }

    /// This method will prevent any additional requests/queries from being performed and will deinit the lookup list.
    func cancel() -> EventLoopFuture<Void> {
        self.eventLoop.submit {
            self.logger.warning("Attempting to cancel lookup")
            self.canceled = true
            self.tearDownSelf()
            self.completionPromise.fail(Errors.canceled)
        }
    }

    /// TODO: Multiple async workers are calling next() on our list and getting redundant values. LookupList needs to be constrained to a single eventloop.
    /// I think this was actually due to the same peer being removed from the list and then later added again. We're now keeping track of removed peers and checking
    /// against that list before adding peers to the list.
    ///
    /// TODO: We should add a trace to this so we can debug these lookups (a history of each query, and the returned value, the state of the list at various points etc...)
    private func _recursivelyQueryForTarget(on: EventLoop, decrementingRequests: Bool = false) -> EventLoopFuture<Void>
    {
        self.eventLoop.flatSubmit {
            if decrementingRequests { self.requestsInProgress -= 1 }
            guard !self.canceled else {
                self.logger.warning("Lookup Cancelled, Worker Terminating")
                return self.eventLoop.makeSucceededVoidFuture()
            }
            guard let next = self.list.next() else {
                if self.requestsInProgress > 0 {
                    /// We have an outstanding query that might return more work, lets check back in a few ms...
                    return self.eventLoop.flatScheduleTask(in: .milliseconds(50)) {
                        self._recursivelyQueryForTarget(on: on)
                    }.futureResult
                }
                self.logger.warning("Worker Terminating - No More Work")
                return self.eventLoop.makeSucceededVoidFuture()
            }
            self.logger.info("Querying \(next.peer.b58String) for id: \(self.target.b58String.prefix(6))")
            self.requestsInProgress += 1
            return on.flatSubmit {
                self.host._sendQuery(.findNode(id: self.target), to: next, on: on).flatMapAlways { result in
                    switch result {
                    case .failure(let error):
                        return self.eventLoop.flatSubmit {
                            self.logger.warning(
                                "Query to peer \(next.peer) failed due to \(error), removing them from our list"
                            )
                            self.list.remove(next.peer)
                            return self._recursivelyQueryForTarget(on: on, decrementingRequests: true)
                        }
                    case .success(let response):
                        if case .findNode(let peers) = response {
                            return self.eventLoop.flatSubmit {
                                //if id != self.target.id { self.logger.warning("Resposne target `\(id.asString(base: .base16))` doesn't match Query target \(self.target.id.asString(base: .base16))") }
                                self.list.insertMany(peers.compactMap { try? $0.toPeerInfo() })
                                self.logger.info(
                                    "Query to peer \(next.peer) succeeded, got \(peers.count) additional peers"
                                )
                                //self.logger.info("\(peers.compactMap { try? $0.toPeerInfo() })")
                                return self._recursivelyQueryForTarget(on: on, decrementingRequests: true)
                            }
                        } else {
                            return self.eventLoop.flatSubmit {
                                self.logger.warning("Query to peer \(next.peer) failed, removing them from our list")
                                self.list.remove(next.peer)
                                return self._recursivelyQueryForTarget(on: on, decrementingRequests: true)
                            }
                        }
                    }
                }
            }
        }
    }
}

class KeyLookup {
    let target: KadDHT.Key
    let maxConcurrentRequests: Int
    let list: LookupList

    //private var requests:Int = 0
    private let group: EventLoopGroup
    private let eventLoop: EventLoop
    private let host: KadDHT.Node

    private var began: Bool = false
    private var logger: Logger
    private var value: [DHT.Record] = []
    private var providers: [PeerInfo] = []

    private var queriesInProgress: UInt8 = 0
    private var canceled: Bool = false
    private let sharedELG: Bool

    init(
        host: KadDHT.Node,
        target: KadDHT.Key,
        concurrentRequests: Int = 1,
        seeds: [PeerInfo] = [],
        groupProvider: Application.EventLoopGroupProvider = .createNew
    ) {
        self.host = host
        self.target = target
        self.maxConcurrentRequests = concurrentRequests
        self.list = LookupList(id: target, capacity: host.routingTable.bucketSize, seeds: seeds)
        switch groupProvider {
        case .shared(let group):
            self.group = group
            self.sharedELG = true
        case .createNew:
            self.group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
            self.sharedELG = false
        }
        self.eventLoop = self.group.next()
        self.began = false
        self.logger = Logger(label: "KeyLookup[\(UUID().uuidString.prefix(5))]")
        self.logger.logLevel = self.host.logger.logLevel

        self.logger.warning(
            "KeyLookup Instantiated with \(seeds.count) Seeds, searching for key: \(String(data: Data(target.original), encoding: .utf8) ?? "NIL")"
        )
    }

    deinit { print("KeyLookup::deinit") }

    func tearDownSelf() {
        print("KeyLookup::tearDownSelf")
        if !self.sharedELG {
            try? self.eventLoop.close()
            self.group.shutdownGracefully(queue: .global()) { _ in print("KeyLookup::ELG shutdown") }
        }
    }

    /// Spawns ⍺ eventLoops and begins a recursive node query on each one until all contacts in our LookupList have been processed...
    func proceedForPeers() -> EventLoopFuture<[PeerInfo]> {
        guard self.began == false else { return self.eventLoop.makeFailedFuture(KadDHT.Errors.alreadyPerformingLookup) }
        self.logger.warning("Proceeding with KeyLookup using \(self.maxConcurrentRequests) workers")
        self.began = true
        return (0..<self.maxConcurrentRequests).compactMap { _ -> EventLoopFuture<Void> in
            self.logger.info("Deploying Worker")
            return _recursivelyQueryForTarget(on: self.group.next())
        }.flatten(on: self.eventLoop).map {
            self.logger.info("Completed!")
            if self.logger.logLevel <= .debug {
                self.list.dumpMetrics()
            }
            return self.list.all()
        }.always { _ in
            self.tearDownSelf()
        }
    }

    func proceedForValue() -> EventLoopFuture<DHT.Record?> {
        guard self.began == false else { return self.eventLoop.makeFailedFuture(KadDHT.Errors.alreadyPerformingLookup) }
        self.logger.warning("Proceeding with KeyLookup using \(self.maxConcurrentRequests) workers")
        self.began = true
        return (0..<self.maxConcurrentRequests).compactMap { _ -> EventLoopFuture<Void> in
            self.logger.info("Deploying Worker")
            return _recursivelyQueryForValue(on: self.group.next())
        }.flatten(on: self.eventLoop).map {
            self.logger.info("Completed!")
            if self.logger.logLevel <= .debug {
                self.list.dumpMetrics()
            }
            /// We should validate any records we found. Maybe return the one with the most redundancy if we found multiple copies...
            return self.value.first
        }.always { _ in
            self.tearDownSelf()
        }
    }

    func proceedForProvider() -> EventLoopFuture<[PeerInfo]> {
        guard self.began == false else { return self.eventLoop.makeFailedFuture(KadDHT.Errors.alreadyPerformingLookup) }
        self.logger.warning("Proceeding with Provider Lookup using \(self.maxConcurrentRequests) workers")
        self.began = true
        return (0..<self.maxConcurrentRequests).compactMap { _ -> EventLoopFuture<Void> in
            self.logger.info("Deploying Worker")
            return _recursivelyQueryForProvider(on: self.group.next())
        }.flatten(on: self.eventLoop).map {
            self.logger.info("Completed!")
            if self.logger.logLevel <= .debug {
                self.list.dumpMetrics()
            }
            /// We should validate any records we found. Maybe return the one with the most redundancy if we found multiple copies...
            return self.providers
        }.always { _ in
            self.tearDownSelf()
        }
    }

    private func _recursivelyQueryForTarget(on: EventLoop, decrementingQueries: Bool = false) -> EventLoopFuture<Void> {
        self.eventLoop.flatSubmit {
            if decrementingQueries { self.queriesInProgress -= 1 }
            guard !self.canceled else {
                self.logger.warning("Lookup Canceled")
                return self.eventLoop.makeSucceededVoidFuture()
            }
            guard let next = self.list.next() else {
                if self.queriesInProgress > 0 {
                    /// We have an outstanding query that might return more work, lets check back in a few ms...
                    return self.eventLoop.flatScheduleTask(in: .milliseconds(50)) {
                        self._recursivelyQueryForTarget(on: on)
                    }.futureResult
                }
                self.logger.warning("Done Processing Peers")
                return self.eventLoop.makeSucceededVoidFuture()
            }
            self.logger.warning(
                "Querying \(next.peer.b58String) for id: \(KadDHT.keyToHumanReadableString(self.target.original))"
            )
            self.queriesInProgress += 1
            return on.flatSubmit {
                guard let p = try? PeerID(fromBytesID: self.target.original) else {
                    self.logger.warning(
                        "Query to peer \(next.peer) failed due to having an invalid PeerID, removing them from our list"
                    )
                    self.list.remove(next.peer)
                    return self._recursivelyQueryForTarget(on: on, decrementingQueries: true)
                }
                return self.host._sendQuery(.findNode(id: p), to: next, on: on).flatMapAlways { result in
                    switch result {
                    case .failure(let error):
                        return self.eventLoop.flatSubmit {
                            self.logger.warning(
                                "Query to peer \(next.peer) failed due to \(error), removing them from our list"
                            )
                            self.list.remove(next.peer)
                            return self._recursivelyQueryForTarget(on: on, decrementingQueries: true)
                        }
                    case .success(let response):
                        if case let .findNode(closerPeers) = response {
                            return self.eventLoop.flatSubmit {
                                //if id != self.target.original { self.logger.warning("Resposne target `\(id.asString(base: .base16))` doesn't match Query target \(self.target.original.asString(base: .base16))") }
                                self.list.insertMany(closerPeers.compactMap { try? $0.toPeerInfo() })
                                self.logger.warning(
                                    "Query to peer \(next.peer) succeeded, got \(closerPeers.count) additional peers"
                                )
                                //self.logger.info("\(closerPeers)")
                                return self._recursivelyQueryForTarget(on: on, decrementingQueries: true)
                            }
                        } else {
                            return self.eventLoop.flatSubmit {
                                self.logger.warning("Query to peer \(next.peer) failed, removing them from our list")
                                self.list.remove(next.peer)
                                return self._recursivelyQueryForTarget(on: on, decrementingQueries: true)
                            }
                        }
                    }
                }
            }
        }
    }

    /// Should we be issueing getProviders queries instead of findNode queries?
    /// Once we find a provider we issue a getValue to them?
    /// Compile all responses and only return if a certain threshold of equal results have been found?
    private func _recursivelyQueryForValue(on: EventLoop, decrementingQueries: Bool = false) -> EventLoopFuture<Void> {
        self.eventLoop.flatSubmit {
            if decrementingQueries { self.queriesInProgress -= 1 }
            guard !self.canceled else {
                self.logger.warning("Lookup Canceled")
                return self.eventLoop.makeSucceededVoidFuture()
            }
            guard let next = self.list.next() else {
                if self.queriesInProgress > 0 {
                    /// We have an outstanding query that might return more work, lets check back in a few ms...
                    return self.eventLoop.flatScheduleTask(in: .milliseconds(50)) {
                        self._recursivelyQueryForValue(on: on)
                    }.futureResult
                }
                self.logger.warning("Done Processing Peers")
                return self.eventLoop.makeSucceededVoidFuture()
            }
            self.logger.warning(
                "Querying \(next.peer.b58String) for id: \(KadDHT.keyToHumanReadableString(self.target.original))"
            )
            self.queriesInProgress += 1
            return on.flatSubmit {
                self.host._sendQuery(.getValue(key: self.target.original), to: next, on: on).flatMapAlways { result in
                    switch result {
                    case .failure(let error):
                        return self.eventLoop.flatSubmit {
                            self.logger.warning(
                                "Query to peer \(next.peer) failed due to \(error), removing them from our list"
                            )
                            self.list.remove(next.peer)
                            return self._recursivelyQueryForValue(on: on, decrementingQueries: true)
                        }
                    case .success(let response):
                        if case let .getValue(key, record, closerPeers) = response {
                            /// If we found a record, store it...
                            if let record = record {
                                if record.key.bytes == self.target.original, key == self.target.original {
                                    self.value.append(record)
                                    self.logger.warning("Query to peer \(next.peer) succeeded, got value \(record)")
                                    /// Terminate Lookup now that we have a value...
                                    self.list.cancel()
                                    self.canceled = true
                                    return self.eventLoop.makeSucceededVoidFuture()
                                } else {
                                    self.logger.warning("Got record but it didn't match the key target key")
                                    self.logger.warning("\(record)")
                                }
                            }

                            return self.eventLoop.flatSubmit {
                                self.list.insertMany(closerPeers.compactMap { try? $0.toPeerInfo() })
                                self.logger.warning(
                                    "Query to peer \(next.peer) succeeded, got \(closerPeers.count) additional peers"
                                )
                                return self._recursivelyQueryForValue(on: on, decrementingQueries: true)
                            }
                        } else {
                            return self.eventLoop.flatSubmit {
                                self.logger.warning("Query to peer \(next.peer) failed, removing them from our list")
                                self.list.remove(next.peer)
                                return self._recursivelyQueryForValue(on: on, decrementingQueries: true)
                            }
                        }
                    }
                }
            }
        }
    }

    // TODO: Support minimum providers before terminating
    private func _recursivelyQueryForProvider(on: EventLoop, decrementingQueries: Bool = false) -> EventLoopFuture<Void>
    {
        self.eventLoop.flatSubmit {
            if decrementingQueries { self.queriesInProgress -= 1 }
            guard !self.canceled else {
                self.logger.warning("Lookup Canceled")
                return self.eventLoop.makeSucceededVoidFuture()
            }
            guard let next = self.list.next() else {
                if self.queriesInProgress > 0 {
                    /// We have an outstanding query that might return more work, lets check back in a few ms...
                    return self.eventLoop.flatScheduleTask(in: .milliseconds(50)) {
                        self._recursivelyQueryForProvider(on: on)
                    }.futureResult
                }
                self.logger.warning("Done Processing Peers")
                return self.eventLoop.makeSucceededVoidFuture()
            }
            self.logger.warning(
                "Querying \(next.peer.b58String) for cid: \(KadDHT.keyToHumanReadableString(self.target.original))"
            )
            self.queriesInProgress += 1
            return on.flatSubmit {
                self.host._sendQuery(.getProviders(cid: self.target.original), to: next, on: on).flatMapAlways {
                    result in
                    switch result {
                    case .failure(let error):
                        return self.eventLoop.flatSubmit {
                            self.logger.warning(
                                "Query to peer \(next.peer) failed due to \(error), removing them from our list"
                            )
                            self.list.remove(next.peer)
                            return self._recursivelyQueryForProvider(on: on, decrementingQueries: true)
                        }
                    case .success(let response):
                        if case let .getProviders(cid, providers, closerPeers) = response, cid == self.target.original {
                            /// If we found a provider, store it...
                            if !providers.isEmpty {
                                let providerPeers = providers.compactMap { try? $0.toPeerInfo() }.filter { pInfo in
                                    !pInfo.addresses.isEmpty
                                }
                                for prov in providerPeers {
                                    if !self.providers.contains(where: { $0.peer == prov.peer }) {
                                        self.providers.append(prov)
                                    }
                                }
                                //self.providers.append(contentsOf: providerPeers)

                                /// Terminate Lookup now that we have a value...

                                //self.list.cancel()
                                //self.canceled = true
                                //return self.eventLoop.makeSucceededVoidFuture()
                            }

                            return self.eventLoop.flatSubmit {
                                self.list.insertMany(closerPeers.compactMap { try? $0.toPeerInfo() })
                                self.logger.warning(
                                    "Query to peer \(next.peer) succeeded, got \(closerPeers.count) additional peers"
                                )
                                return self._recursivelyQueryForProvider(on: on, decrementingQueries: true)
                            }
                        } else {
                            return self.eventLoop.flatSubmit {
                                self.logger.warning("Query to peer \(next.peer) failed, removing them from our list")
                                self.list.remove(next.peer)
                                return self._recursivelyQueryForProvider(on: on, decrementingQueries: true)
                            }
                        }
                    }
                }
            }
        }
    }
}
