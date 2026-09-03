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
import LibP2PNoise
import LibP2PTesting
import LibP2PYAMUX
import Testing

@testable import LibP2PKadDHT

extension LibP2PKadDHTTests {

    @Suite("Defaults Tests")
    struct DefaultsTests {

        /// go `amino/defaults.go`: bucket size 20, concurrency 10, resiliency 3.
        @Test func kademliaParametersMatchAmino() {
            #expect(KadDHT.Defaults.bucketSize == 20)
            #expect(KadDHT.Defaults.alpha == 10)
            #expect(KadDHT.Defaults.concurrency == 10)
            #expect(KadDHT.Defaults.beta == 3)
            #expect(KadDHT.Defaults.resiliency == 3)
        }

        @Test func recordLifetimesMatchAmino() {
            #expect(KadDHT.Defaults.provideValidity == .hours(48))
            #expect(KadDHT.Defaults.reprovideInterval == .hours(22))
            #expect(KadDHT.Defaults.maxRecordAge == .hours(48))
            #expect(KadDHT.Defaults.valueGCInterval == .hours(24))
            #expect(KadDHT.Defaults.providerAddrTTL == .hours(24))

            /// Republishing has to happen inside the validity window.
            #expect(KadDHT.Defaults.reprovideInterval < KadDHT.Defaults.provideValidity)
        }

        @Test func refreshCadenceMatchesAmino() {
            #expect(KadDHT.Defaults.refreshInterval == .minutes(10))
            #expect(KadDHT.Defaults.refreshQueryTimeout == .seconds(10))
        }

        /// ``KadDHT/Defaults`` is meant to be the single source for every param
        @Test func defaultConfigurationMirrorsDefaults() {
            let configuration = KadDHT.Configuration.default

            #expect(configuration.bucketSize == KadDHT.Defaults.bucketSize)
            #expect(configuration.concurrency == KadDHT.Defaults.concurrency)
            #expect(configuration.resiliency == KadDHT.Defaults.resiliency)
            #expect(configuration.quorum == KadDHT.Defaults.quorum)

            #expect(configuration.connectionTimeout == KadDHT.Defaults.connectionTimeout)
            #expect(configuration.supportLocalNetwork == false)
            #expect(configuration.acceptObservedProviderAddress == false)

            #expect(configuration.maxValueStoreEntries == KadDHT.Defaults.maxValueStoreEntries)
            #expect(configuration.maxRecordAge == KadDHT.Defaults.maxRecordAge)
            #expect(configuration.valueGCInterval == KadDHT.Defaults.valueGCInterval)

            #expect(configuration.maxProviderStoreEntries == KadDHT.Defaults.maxProviderStoreEntries)
            #expect(configuration.provideValidity == KadDHT.Defaults.provideValidity)
            #expect(configuration.reprovideInterval == KadDHT.Defaults.reprovideInterval)
            #expect(configuration.providerAddrTTL == KadDHT.Defaults.providerAddrTTL)

            #expect(configuration.heartbeatInterval == KadDHT.Defaults.heartbeatInterval)
            #expect(configuration.refreshInterval == KadDHT.Defaults.refreshInterval)
            #expect(configuration.refreshQueryTimeout == KadDHT.Defaults.refreshQueryTimeout)
            #expect(configuration.maxRefreshPrefixLength == KadDHT.Defaults.maxRefreshPrefixLength)

            #expect(configuration.routingTableLatencyTolerance == KadDHT.Defaults.routingTableLatencyTolerance)
            #expect(configuration.usefulnessGracePeriod == KadDHT.Defaults.usefulnessGracePeriod)
            #expect(configuration.replacementStrategy == KadDHT.Defaults.replacementStrategy)
        }

        @Test func overridingOneParamKeepsTheRestDefaulted() {
            let configuration = KadDHT.Configuration(concurrency: 3)
            #expect(configuration.concurrency == 3)
            #expect(configuration.bucketSize == KadDHT.Defaults.bucketSize)
            #expect(configuration.resiliency == KadDHT.Defaults.resiliency)
        }

        /// The node has to actually carry α through to its lookups.
        @Test func nodeAdoptsConfiguredAlpha() async throws {
            try await withApp(configure: dhtHost(mode: .client, configuration: .default)) { app in
                #expect(app.dht.kadDHT.concurrency == KadDHT.Defaults.concurrency)
            }

            try await withApp(configure: dhtHost(mode: .client, configuration: .init(concurrency: 7))) { app in
                #expect(app.dht.kadDHT.concurrency == 7)
            }
        }

        /// Everything else the node derives from its configuration, including the two lifetimes it
        /// converts to `TimeInterval` once at init.
        @Test func nodeAdoptsTheRestOfTheConfiguration() async throws {
            let configuration = KadDHT.Configuration(
                bucketSize: 4,
                resiliency: 2,
                quorum: 5,
                connectionTimeout: .milliseconds(250),
                supportLocalNetwork: true,
                acceptObservedProviderAddress: true,
                maxValueStoreEntries: 7,
                maxProviderStoreEntries: 9,
                provideValidity: .hours(1),
                reprovideInterval: .minutes(30),
                usefulnessGracePeriod: .seconds(1),
                replacementStrategy: .oldestReplaceable
            )

            try await withApp(configure: dhtHost(mode: .client, configuration: configuration)) { app in
                let node = app.dht.kadDHT

                #expect(node.resiliency == 2)
                #expect(node.quorum == 5)
                #expect(node.connectionTimeout == .milliseconds(250))
                #expect(node.isRunningLocally == true)
                #expect(node.acceptObservedProviderAddress == true)
                #expect(node.maxValueStoreEntries == 7)
                #expect(node.maxProviderStoreEntries == 9)
                #expect(node.replacementStrategy == .oldestReplaceable)

                /// `k` reaches the routing table, not just the node.
                #expect(node.routingTable.bucketSize == 4)

                /// Converted to seconds once, at init.
                #expect(node.providerRecordTTL == 3600)
                #expect(node.providerRecordRepublishInterval == 1800)
            }
        }

        /// The LAN identifier was missing its leading slash, so it could never have matched a
        /// protocol string on the wire.
        @Test func protocolIdentifiersAreWellFormed() {
            #expect(KadDHT.multicodec == "/ipfs/kad/1.0.0")
            #expect(KadDHT.multicodecLAN == "/ipfs/lan/kad/1.0.0")
            #expect(SemVerProtocol(KadDHT.multicodecLAN)?.stringValue == KadDHT.multicodecLAN)
        }
    }
}
