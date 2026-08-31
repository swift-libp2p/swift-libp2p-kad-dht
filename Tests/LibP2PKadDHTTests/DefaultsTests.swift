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

        /// α was 4, which under-queries every lookup path relative to the rest of the network.
        @Test func nodeOptionsDefaultToAminoAlphaAndK() {
            let options = KadDHT.NodeOptions.default
            #expect(options.concurrency == KadDHT.Defaults.concurrency)
            #expect(options.bucketSize == KadDHT.Defaults.bucketSize)
            #expect(options.maxRecordAge == KadDHT.Defaults.maxRecordAge)
            #expect(options.valueGCInterval == KadDHT.Defaults.valueGCInterval)
        }

        /// The node has to actually carry α through to its lookups.
        @Test func nodeAdoptsConfiguredAlpha() async throws {
            try await withApp(configure: dhtHost(mode: .client, options: .default)) { app in
                #expect(app.dht.kadDHT.concurrency == KadDHT.Defaults.concurrency)
            }

            try await withApp(configure: dhtHost(mode: .client, options: .init(concurrency: 7))) { app in
                #expect(app.dht.kadDHT.concurrency == 7)
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
