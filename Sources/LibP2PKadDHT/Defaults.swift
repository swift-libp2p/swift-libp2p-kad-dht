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

    /// The Amino (public IPFS) DHT parameters.
    ///
    /// Values match go-libp2p-kad-dht's `amino/defaults.go` so a Swift node behaves like every other
    /// peer on `/ipfs/kad/1.0.0`.
    public enum Defaults {

        /// Bucket size and replication parameter (`k`).
        /// go: `amino.DefaultBucketSize`.
        public static let bucketSize: Int = 20

        /// Lookup concurrency (`α`), requests a query path keeps in flight.
        /// go: `amino.DefaultConcurrency`.
        public static let concurrency: Int = { Self.alpha }()

        /// Lookup concurrency (`α`), requests a query path keeps in flight.
        public static let alpha: Int = 10

        /// Resiliency (`β`), how many of the closest peers must respond before a query path completes.
        /// go: `amino.DefaultResiliency`.
        public static let resiliency: Int = { Self.beta }()

        /// Resiliency (`β`), how many of the closest peers must respond before a query path completes.
        public static let beta: Int = 3

        /// How long a provider record stays valid on the peers holding it.
        /// go: `amino.DefaultProvideValidity`.
        public static let provideValidity: TimeAmount = .hours(48)

        /// How often we re-announce our own provider records. Sits inside ``provideValidity`` so one
        /// missed republish doesn't drop us from remote stores.
        /// go: `amino.DefaultReprovideInterval`.
        public static let reprovideInterval: TimeAmount = .hours(22)

        /// The longest we hold a value record, measured from its `timeReceived` stamp.
        /// go: `DefaultMaxRecordAge`.
        public static let maxRecordAge: TimeAmount = .hours(48)

        /// Cadence of the value-store GC sweep.
        /// go: `DefaultValueGCInterval`.
        public static let valueGCInterval: TimeAmount = .hours(24)

        /// How long we serve a provider's multiaddrs; afterwards the entry is peer-ID only.
        /// go: `ProviderAddrTTL`.
        public static let providerAddrTTL: TimeAmount = .hours(24)

        /// Routing-table refresh cadence.
        /// go: `DefaultRoutingTableRefreshPeriod`.
        public static let refreshInterval: TimeAmount = .minutes(10)

        /// Per-query timeout for refresh lookups.
        /// go: `DefaultRoutingTableRefreshQueryTimeout`.
        public static let refreshQueryTimeout: TimeAmount = .seconds(10)

        /// The largest inbound message we'll reassemble.
        ///
        /// A length prefix is remote input: without a ceiling, a peer can announce a multi-gigabyte
        /// frame and we'll buffer toward it.
        public static let maxMessageSize: Int = 1 << 20

        /// The largest `DHT.Record` we'll accept or emit.
        ///
        /// IPNS caps entries at 10 KiB and `/pk/` records are a few hundred bytes
        public static let maxRecordSize: Int = 10 * 1024

        /// The most `closerPeers` / `providerPeers` we'll put in one response.
        ///
        /// The spec says "the k closest peers"
        public static let maxPeersPerMessage: Int = KadDHT.Defaults.bucketSize
    }
}
