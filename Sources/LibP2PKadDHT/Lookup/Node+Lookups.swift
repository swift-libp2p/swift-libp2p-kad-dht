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

/// The four lookups every public operation is built on.
extension KadDHT.Node {

    // MARK: - Peers

    /// FIND_NODE: the k closest peers the network knows of to `target`.
    ///
    /// The peers it finds are folded into our routing table on the way out, a lookup is also how we
    /// learn about the part of the keyspace we just walked.
    func lookupClosestPeers(to target: KadDHT.Key, timeout: TimeAmount? = nil) -> EventLoopFuture<[PeerInfo]> {
        self._nearest(self.routingTable.bucketSize, peersToKey: target).flatMap { seeds in
            KadDHT.QueryEngine(host: self, target: target, seeds: seeds, timeout: timeout) { peer in
                self._sendQuery(.findNode(key: target.original), to: peer, on: self.eventLoop).map { response in
                    guard case .findNode(let closerPeers) = response else { return KadDHT.QueryEngine.StepResult() }
                    return KadDHT.QueryEngine.StepResult(closerPeers: closerPeers.compactMap { try? $0.toPeerInfo() })
                }
            }.run()
        }.flatMap { closestPeers in
            self.addPeersIfSpaceOrCloser(closestPeers).map { closestPeers }
        }
    }

    /// FIND_NODE for one particular peer, stopping as soon as that peer turns up.
    ///
    /// - Note: Any peer may report a third peer's addresses, whether or not that peer is a DHT
    ///   server, which is how a client-only peer's addresses are discoverable at all.
    func lookupPeer(_ peer: PeerID) -> EventLoopFuture<PeerInfo?> {
        let target = KadDHT.Key(peer, keySpace: .xor)
        let found = NIOLockedValueBox<PeerInfo?>(nil)

        return self._nearest(self.routingTable.bucketSize, peersToKey: target).flatMap { seeds in
            /// A seed might be the peer itself.
            if let match = seeds.first(where: { $0.peer == peer }), !match.addresses.isEmpty {
                found.withLockedValue { $0 = match }
            }

            return KadDHT.QueryEngine(host: self, target: target, seeds: seeds) { queried in
                self._sendQuery(.findNode(key: peer.id), to: queried, on: self.eventLoop).map { response in
                    guard case .findNode(let closerPeers) = response else { return KadDHT.QueryEngine.StepResult() }
                    let peers = closerPeers.compactMap { try? $0.toPeerInfo() }
                    if let match = peers.first(where: { $0.peer == peer }), !match.addresses.isEmpty {
                        found.withLockedValue { $0 = match }
                        return KadDHT.QueryEngine.StepResult(closerPeers: peers, stop: true)
                    }
                    return KadDHT.QueryEngine.StepResult(closerPeers: peers)
                }
            }.run()
        }.flatMap { closestPeers in
            self.addPeersIfSpaceOrCloser(closestPeers).map {
                found.withLockedValue { $0 } ?? closestPeers.first(where: { $0.peer == peer })
            }
        }
    }

    // MARK: - Values

    /// GET_VALUE: the best record the network holds for `key`, or `nil`.
    ///
    /// Divergent answers are resolved with the namespace's validator rather than by arrival order,
    /// and once the lookup settles the peers that answered with a worse record (or none) are sent
    /// the winner, the spec's entry-correction step.
    ///
    /// - Parameters:
    ///   - quorum: Stop once this many records have been collected. `0` searches to convergence.
    ///   - trace: Optional record of every response, for ``KadDHT/Node/getWithTrace(_:)``.
    func lookupValue(
        _ key: [UInt8],
        quorum: Int,
        trace: LookupTrace? = nil
    ) -> EventLoopFuture<DHT.Record?> {
        let target = KadDHT.Key(key, keySpace: .xor)
        let best = NIOLockedValueBox<DHT.Record?>(nil)
        /// Peers that answered without the best record. They get a correcting PUT once we're done.
        let outdated = NIOLockedValueBox<[PeerInfo]>([])
        let recordsSeen = NIOLockedValueBox<Int>(0)

        return self._nearest(self.routingTable.bucketSize, peersToKey: target).flatMap { seeds in
            KadDHT.QueryEngine(host: self, target: target, seeds: seeds) { peer in
                self._sendQuery(.getValue(key: key), to: peer, on: self.eventLoop).map { response in
                    guard case .getValue(let responseKey, let record, let closerPeers) = response else {
                        return KadDHT.QueryEngine.StepResult()
                    }
                    trace?.add(response, from: peer)
                    let peers = closerPeers.compactMap { try? $0.toPeerInfo() }

                    guard responseKey == key,
                        let record,
                        record.key.byteArray == key,
                        self.isValidRecord(record, for: key)
                    else {
                        /// No usable record from this peer, so it's behind whatever we do find.
                        outdated.withLockedValue { $0.append(peer) }
                        return KadDHT.QueryEngine.StepResult(closerPeers: peers)
                    }

                    let total = recordsSeen.withLockedValue { count -> Int in
                        count += 1
                        return count
                    }
                    let replaced = best.withLockedValue { current -> Bool in
                        guard let existing = current else {
                            current = record
                            return true
                        }
                        guard self.prefers(record, over: existing, for: key) else { return false }
                        current = record
                        return true
                    }
                    if !replaced { outdated.withLockedValue { $0.append(peer) } }

                    return KadDHT.QueryEngine.StepResult(closerPeers: peers, stop: quorum > 0 && total >= quorum)
                }
            }.run()
        }.flatMap { responders -> EventLoopFuture<DHT.Record?> in
            self.addPeersIfSpaceOrCloser(responders).flatMap { _ -> EventLoopFuture<DHT.Record?> in
                guard let winner = best.withLockedValue({ $0 }) else {
                    return self.eventLoop.makeSucceededFuture(nil)
                }
                let stale = outdated.withLockedValue { $0 }
                return self.correctEntries(key: key, to: winner, at: stale).map { winner }
            }
        }
    }

    /// Best-effort PUT_VALUE of the winning record at the peers that were behind.
    ///
    /// Spec: "the client may send the best value to the peers that returned an outdated value".
    private func correctEntries(
        key: [UInt8],
        to record: DHT.Record,
        at peers: [PeerInfo]
    ) -> EventLoopFuture<Void> {
        let stale = peers.prefix(self.routingTable.bucketSize)
        guard !stale.isEmpty else { return self.eventLoop.makeSucceededVoidFuture() }
        self.logger.debug("Correcting \(stale.count) peer(s) holding an outdated record")

        /// `timeReceived` is the holder's own stamp, so it's left off outbound records.
        var outbound = DHT.Record()
        outbound.key = record.key
        outbound.value = record.value

        return stale.map { peer in
            self._sendQuery(.putValue(key: key, record: outbound), to: peer, on: self.eventLoop)
                .flatMapAlways { _ in self.eventLoop.makeSucceededVoidFuture() }
        }.flatten(on: self.eventLoop)
    }

    /// Whether the namespace's validator accepts this record. Namespaces we hold no validator for
    /// are taken as-is, a client that registered none can still read the DHT.
    private func isValidRecord(_ record: DHT.Record, for key: [UInt8]) -> Bool {
        guard let namespace = KadDHT.extractNamespace(key), let validator = self.validators[namespace] else {
            return true
        }
        do {
            try validator.validate(key: key, value: record.value.byteArray)
            return true
        } catch {
            self.logger.debug("Dropping invalid record for \(KadDHT.keyToHumanReadableString(key)): \(error)")
            return false
        }
    }

    /// Whether `candidate` should replace `current`, per the namespace's validator.
    ///
    /// With no validator registered we keep what we have, so a lookup is never at the mercy of the
    /// order responses happen to arrive in.
    private func prefers(_ candidate: DHT.Record, over current: DHT.Record, for key: [UInt8]) -> Bool {
        guard let namespace = KadDHT.extractNamespace(key), let validator = self.validators[namespace] else {
            return false
        }
        let values = [current.value.byteArray, candidate.value.byteArray]
        guard let selected = try? validator.select(key: key, values: values) else { return false }
        return selected == 1
    }

    // MARK: - Providers

    /// GET_PROVIDERS: peers providing `key`, seeded with any we hold locally.
    ///
    /// - Parameter count: Stop once this many providers are known. `0` searches to convergence.
    func lookupProviders(_ key: [UInt8], count: Int) -> EventLoopFuture<[PeerInfo]> {
        let target = KadDHT.Key(key, keySpace: .xor)
        let providers = NIOLockedValueBox<[PeerInfo]>([])

        return self.providerStore.getValue(forKey: target, default: []).flatMap { local -> EventLoopFuture<Void> in
            let dialable = local.compactMap { try? $0.toPeerInfo() }.filter { !$0.addresses.isEmpty }
            providers.withLockedValue { $0 = dialable }
            return self.eventLoop.makeSucceededVoidFuture()
        }.flatMap {
            self._nearest(self.routingTable.bucketSize, peersToKey: target)
        }.flatMap { seeds -> EventLoopFuture<[PeerInfo]> in
            /// Already satisfied locally — no need to ask anyone.
            if count > 0, providers.withLockedValue({ $0.count }) >= count {
                return self.eventLoop.makeSucceededFuture(providers.withLockedValue { $0 })
            }

            return KadDHT.QueryEngine(host: self, target: target, seeds: seeds) { peer in
                self._sendQuery(.getProviders(key: key), to: peer, on: self.eventLoop).map { response in
                    guard case .getProviders(let responseKey, let providerPeers, let closerPeers) = response,
                        responseKey == key
                    else { return KadDHT.QueryEngine.StepResult() }

                    let found = providerPeers.compactMap { try? $0.toPeerInfo() }.filter { !$0.addresses.isEmpty }
                    let total = providers.withLockedValue { known -> Int in
                        for provider in found where !known.contains(where: { $0.peer == provider.peer }) {
                            known.append(provider)
                        }
                        return known.count
                    }

                    return KadDHT.QueryEngine.StepResult(
                        closerPeers: closerPeers.compactMap { try? $0.toPeerInfo() },
                        stop: count > 0 && total >= count
                    )
                }
            }.run().flatMap { responders in
                self.addPeersIfSpaceOrCloser(responders).map { providers.withLockedValue { $0 } }
            }
        }
    }
}
