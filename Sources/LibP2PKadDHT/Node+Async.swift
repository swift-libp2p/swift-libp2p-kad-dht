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

/// `async` counterparts to the node's public API.
extension KadDHT.Node {

    /// Finds `peer`'s addresses, walking the DHT and falling back to our peerstore.
    public func findPeer(peer: PeerID) async throws -> PeerInfo {
        try await self.findPeer(peer: peer).get()
    }

    /// Announces this node as a provider for `cid`.
    ///
    /// See the future-based ``provide(cid:announce:)`` for what `announce` does and what the
    /// returned value does *not* promise, a resolved call means the RPCs are on the wire, not that
    /// anybody stored the record.
    public func provide(cid: [UInt8], announce: Bool = true) async throws {
        try await self.provide(cid: cid, announce: announce).get()
    }

    /// The addresses of peers providing `cid`, stopping once `count` have been found.
    ///
    /// - Parameter count: Pass `0` to search to convergence.
    public func findProviders(cid: [UInt8], count: Int) async throws -> [Multiaddr] {
        try await self.findProviders(cid: cid, count: count).get()
    }

    /// The value stored under `key`, ours if we hold it, otherwise the best the network has.
    public func get(_ key: [UInt8]) async throws -> DHTRecord? {
        try await self.get(key).get()
    }

    /// Stores `value` locally and asks the k closest peers to store it too.
    ///
    /// - Returns: Whether the local write succeeded. Remote acceptance is best-effort, matching
    ///   the future-based ``storeNew(_:value:)``.
    @discardableResult
    public func storeNew(_ key: [UInt8], value: DHTRecord) async throws -> Bool {
        try await self.storeNew(key, value: value).get()
    }

    /// Runs one maintenance beat: provider expiry, value GC, re-publish, and KV sharing.
    ///
    /// - Throws: ``KadDHT/Errors/cannotCallHeartbeatWhileNodeIsInAutoUpdateMode`` when the node is
    ///   driving its own beat.
    public func heartbeat() async throws {
        try await self.heartbeat().get()
    }

    /// Cancels the node's periodic work, returning once an in-flight heartbeat has finished.
    ///
    /// Unlike the synchronous ``stop()`` this never blocks a thread, and unlike ``shutdown()`` it
    /// reads as a single statement at the call site.
    public func stop() async {
        /// `shutdown()`'s future is documented to always succeed.
        try? await self.shutdown().get()
    }
}
