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

    /// Everything tunable about a ``KadDHT/Node``.
    ///
    /// Every default is a ``KadDHT/Defaults`` value, so ``default`` is the Amino profile.
    public struct Configuration: Sendable {

        // MARK: Kademlia parameters

        /// Bucket size and replication parameter (`k`), how many peers a bucket holds and how many
        /// peers a lookup converges on.
        public let bucketSize: Int

        /// Lookup concurrency (`α`), how many requests a query path keeps in flight.
        public let concurrency: Int

        /// Resiliency (`β`), how many of the closest peers must respond before a lookup is done.
        public let resiliency: Int

        /// How many records a value lookup collects before it stops early.
        ///
        /// `0` searches to convergence, which is the spec's default behaviour: every peer among the
        /// closest is asked, and the best of all the answers wins.
        public let quorum: Int

        // MARK: Networking

        /// How long a single outbound request may take before we treat the peer as unreachable.
        public let connectionTimeout: TimeAmount

        /// Whether the node operates over private/local addresses.
        ///
        /// Off by default, which restricts dialing to externally routable addresses and advertises
        /// only `/ipfs/kad/1.0.0`. When set to `true`, we also accept `/ipfs/lan/kad/1.0.0`.
        public let supportLocalNetwork: Bool

        /// Whether an `ADD_PROVIDER` whose `providerPeers` don't carry the sender's addresses may
        /// fall back to the address we observed the stream on.
        ///
        /// Off by default, matching go: a provider record is only as good as its addresses, and the
        /// address an inbound stream arrives on is usually an ephemeral source port nobody can dial
        /// back. A provider that doesn't advertise itself is dropped instead.
        public let acceptObservedProviderAddress: Bool

        // MARK: Value records

        /// How many value records we hold before capacity pruning kicks in.
        public let maxValueStoreEntries: Int

        /// The longest we'll hold a value record, measured from the `timeReceived` we stamped on it.
        ///
        /// A record that outlives this is dropped on read and swept by the value GC pass, so a
        /// publisher has to re-put more often than this for the value to stay resolvable.
        ///
        /// 48 hours, matching go's `DefaultMaxRecordAge`.
        public let maxRecordAge: TimeAmount

        /// How often the value store is swept for aged-out records.
        ///
        /// 24 hours, matching go's `DefaultValueGCInterval`.
        public let valueGCInterval: TimeAmount

        // MARK: Provider records

        /// How many provider-store keys we hold before capacity pruning kicks in.
        public let maxProviderStoreEntries: Int

        /// How long a provider record we're holding stays valid.
        ///
        /// 48 hours, matching go's `amino.DefaultProvideValidity`.
        public let provideValidity: TimeAmount

        /// How often we re-announce our own provider records. Sits inside ``provideValidity`` so one
        /// missed republish doesn't drop us from remote stores.
        ///
        /// 22 hours, matching go's `amino.DefaultReprovideInterval`.
        public let reprovideInterval: TimeAmount

        /// How long we serve a provider's multiaddrs; afterwards the entry is peer-ID only.
        ///
        /// - Warning: Stored but not yet enforced
        public let providerAddrTTL: TimeAmount

        // MARK: Maintenance cadence

        /// Cadence of the maintenance beat: provider expiry, value GC, provider re-publish.
        public let heartbeatInterval: TimeAmount

        /// Routing-table refresh cadence. Slower than ``heartbeatInterval`` because a refresh is
        /// `1 + non-empty buckets` lookups.
        ///
        /// 10 minutes, matching go's `DefaultRoutingTableRefreshPeriod`.
        public let refreshInterval: TimeAmount

        /// Per-query timeout for refresh lookups.
        ///
        /// 10 seconds, matching go's `DefaultRoutingTableRefreshQueryTimeout`.
        public let refreshQueryTimeout: TimeAmount

        /// The deepest bucket a refresh will aim a targeted lookup at. Deeper buckets are covered by
        /// the self-lookup every refresh cycle runs.
        /// - Note: See ``KadDHT/Defaults/maxRefreshPrefixLength`` for why there's a ceiling.
        public let maxRefreshPrefixLength: Int

        // MARK: Routing table

        /// Maximum acceptable latency for peers in the routing table's cluster.
        ///
        /// 10 seconds, matching go's `RoutingTable.LatencyTolerance`.
        ///
        /// - Warning: Stored but not yet enforced
        public let routingTableLatencyTolerance: TimeAmount

        /// How long a peer stays "useful" after it last helped us, before eviction may prefer it.
        public let usefulnessGracePeriod: TimeAmount

        /// The strategy our ``RoutingTable`` uses to determine which peer to evict from a full ``Bucket``.
        public let replacementStrategy: KadDHT.ReplacementStrategy

        public init(
            bucketSize: Int = KadDHT.Defaults.bucketSize,
            concurrency: Int = KadDHT.Defaults.concurrency,
            resiliency: Int = KadDHT.Defaults.resiliency,
            quorum: Int = KadDHT.Defaults.quorum,
            connectionTimeout: TimeAmount = KadDHT.Defaults.connectionTimeout,
            supportLocalNetwork: Bool = false,
            acceptObservedProviderAddress: Bool = false,
            maxValueStoreEntries: Int = KadDHT.Defaults.maxValueStoreEntries,
            maxRecordAge: TimeAmount = KadDHT.Defaults.maxRecordAge,
            valueGCInterval: TimeAmount = KadDHT.Defaults.valueGCInterval,
            maxProviderStoreEntries: Int = KadDHT.Defaults.maxProviderStoreEntries,
            provideValidity: TimeAmount = KadDHT.Defaults.provideValidity,
            reprovideInterval: TimeAmount = KadDHT.Defaults.reprovideInterval,
            providerAddrTTL: TimeAmount = KadDHT.Defaults.providerAddrTTL,
            heartbeatInterval: TimeAmount = KadDHT.Defaults.heartbeatInterval,
            refreshInterval: TimeAmount = KadDHT.Defaults.refreshInterval,
            refreshQueryTimeout: TimeAmount = KadDHT.Defaults.refreshQueryTimeout,
            maxRefreshPrefixLength: Int = KadDHT.Defaults.maxRefreshPrefixLength,
            routingTableLatencyTolerance: TimeAmount = KadDHT.Defaults.routingTableLatencyTolerance,
            usefulnessGracePeriod: TimeAmount = KadDHT.Defaults.usefulnessGracePeriod,
            replacementStrategy: KadDHT.ReplacementStrategy = KadDHT.Defaults.replacementStrategy
        ) {
            self.bucketSize = bucketSize
            self.concurrency = concurrency
            self.resiliency = resiliency
            self.quorum = quorum
            self.connectionTimeout = connectionTimeout
            self.supportLocalNetwork = supportLocalNetwork
            self.acceptObservedProviderAddress = acceptObservedProviderAddress
            self.maxValueStoreEntries = maxValueStoreEntries
            self.maxRecordAge = maxRecordAge
            self.valueGCInterval = valueGCInterval
            self.maxProviderStoreEntries = maxProviderStoreEntries
            self.provideValidity = provideValidity
            self.reprovideInterval = reprovideInterval
            self.providerAddrTTL = providerAddrTTL
            self.heartbeatInterval = heartbeatInterval
            self.refreshInterval = refreshInterval
            self.refreshQueryTimeout = refreshQueryTimeout
            self.maxRefreshPrefixLength = maxRefreshPrefixLength
            self.routingTableLatencyTolerance = routingTableLatencyTolerance
            self.usefulnessGracePeriod = usefulnessGracePeriod
            self.replacementStrategy = replacementStrategy
        }

        /// The Amino (public IPFS) profile, every value straight from ``KadDHT/Defaults``.
        public static var `default`: Configuration {
            .init()
        }
    }

    @available(*, deprecated, renamed: "Configuration")
    public typealias NodeOptions = Configuration
}
