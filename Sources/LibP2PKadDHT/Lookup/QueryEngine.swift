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

    /// An iterative Kademlia lookup.
    ///
    /// Keeps `concurrency` (α) requests in flight against the peers it has heard of, closest to the
    /// target first, and terminates when any of the following happen:
    ///
    /// - the `resiliency` (β) closest peers we know of have all responded,
    /// - nothing is left to query and nothing is in flight,
    /// - the step asks to stop (a quorum of records, enough providers, the peer we wanted),
    /// - the optional timeout fires.
    ///
    /// - Note: Confined to the host node's event loop.
    final class QueryEngine: @unchecked Sendable {

        /// What one RPC produced.
        struct StepResult: Sendable {
            /// Peers the responder told us about. They become candidates.
            let closerPeers: [PeerInfo]
            /// Ends the lookup once this response is folded in.
            let stop: Bool

            init(closerPeers: [PeerInfo] = [], stop: Bool = false) {
                self.closerPeers = closerPeers
                self.stop = stop
            }
        }

        /// Queries one peer. A failed future marks the peer unreachable.
        typealias Step = @Sendable (PeerInfo) -> EventLoopFuture<StepResult>

        let target: KadDHT.Key

        private let host: KadDHT.Node
        private let eventLoop: EventLoop
        private let concurrency: Int
        private let resiliency: Int
        private let bucketSize: Int
        private let timeout: TimeAmount?
        private let step: Step
        private var logger: Logger

        private var peers: PeerSet
        private var started: Bool = false
        private var finished: Bool = false
        private var stopped: Bool = false
        private var querying: Bool = false
        private var promise: EventLoopPromise<[PeerInfo]>?
        private var timeoutTask: Scheduled<Void>?

        init(
            host: KadDHT.Node,
            target: KadDHT.Key,
            seeds: [PeerInfo],
            timeout: TimeAmount? = nil,
            step: @escaping Step
        ) {
            self.host = host
            self.target = target
            self.eventLoop = host.eventLoop
            self.concurrency = host.concurrency
            self.resiliency = host.resiliency
            self.bucketSize = host.routingTable.bucketSize
            self.timeout = timeout
            self.step = step
            self.peers = PeerSet(target: target, seeds: seeds)
            self.logger = host.logger
            self.logger[metadataKey: "lookup"] = .string(KadDHT.keyToHumanReadableString(target.original))
        }

        /// Runs the lookup, returning the k closest peers that responded.
        func run() -> EventLoopFuture<[PeerInfo]> {
            self.eventLoop.flatSubmit {
                guard !self.started else {
                    return self.eventLoop.makeFailedFuture(KadDHT.Errors.alreadyPerformingLookup)
                }
                self.started = true

                let promise = self.eventLoop.makePromise(of: [PeerInfo].self)
                self.promise = promise

                if let timeout = self.timeout {
                    self.timeoutTask = self.eventLoop.scheduleTask(in: timeout) {
                        self.logger.debug("Lookup timed out, returning what we have")
                        self.finish()
                    }
                }

                self.query()
                return promise.futureResult
            }
        }

        /// Dispatches queries until α are in flight, or finishes the lookup.
        ///
        /// - Note: A query can complete before `dispatch` returns (a cached answer, a synchronous
        ///   failure), which re-enters this method. The `querying` guard keeps that from stacking:
        ///   the loop below re-reads the state each pass, so it sees whatever the inner completion
        ///   recorded — including termination, which is why a `stop` can't be dispatched past.
        private func query() {
            self.eventLoop.assertInEventLoop()
            guard !self.querying else { return }
            self.querying = true
            defer { self.querying = false }

            while !self.finished {
                if self.stopped || self.peers.isStarved || self.peers.isComplete(resiliency: self.resiliency) {
                    self.finish()
                    return
                }
                guard self.peers.inFlight < self.concurrency, let next = self.peers.nextToQuery() else { return }
                self.dispatch(to: next)
            }
        }

        private func dispatch(to peer: PeerInfo) {
            self.step(peer).hop(to: self.eventLoop).whenComplete { result in
                switch result {
                case .success(let outcome):
                    self.peers.mark(peer.peer, as: .queried)
                    /// A peer that was helpful should be marked useful
                    if !outcome.closerPeers.isEmpty || outcome.stop {
                        _ = self.host.routingTable.updateLastUseful(
                            at: Date().timeIntervalSince1970,
                            for: peer.peer
                        )
                    }
                    self.peers.insert(outcome.closerPeers.filter { $0.peer != self.host.peerID })
                    if outcome.stop { self.stopped = true }
                case .failure(let error):
                    self.logger.debug("Query to \(peer.peer) failed: \(error)")
                    self.peers.mark(peer.peer, as: .unreachable)
                }
                self.query()
            }
        }

        private func finish() {
            self.eventLoop.assertInEventLoop()
            guard !self.finished else { return }
            self.finished = true
            self.timeoutTask?.cancel()
            self.timeoutTask = nil

            let results = self.peers.responded(self.bucketSize)
            self.logger.debug("Lookup finished with \(results.count) responders of \(self.peers.count) peers heard")
            self.promise?.succeed(results)
            self.promise = nil
        }
    }
}
