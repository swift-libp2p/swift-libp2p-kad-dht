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

    /// Node start/stop behaviour.
    @Suite("Lifecycle Tests", .serialized)
    final class LifecycleTests {

        private func host(
            mode: KadDHT.Mode,
            autoUpdate: Bool = false
        ) -> ((Application) async throws -> Void) {
            { app in
                app.logger.logLevel = .warning
                app.security.use(.noise)
                app.muxers.use(.yamux)
                app.dht.use(
                    .kadDHT(
                        mode: mode,
                        configuration: KadDHT.Configuration(supportLocalNetwork: true),
                        bootstrapPeers: [],
                        autoUpdate: autoUpdate
                    )
                )
                app.servers.use(.tcp(host: "127.0.0.1", port: 0))
            }
        }

        // - MARK: Startup

        /// The route belongs to the `Application`'s router, which outlives a stop/start cycle of the
        /// node, so a restart must not install it a second time.
        @Test func aServerCanBeRestartedWithoutRegisteringTwice() async throws {
            try await withApp(configure: self.host(mode: .server)) { app in
                let node = app.dht.kadDHT
                #expect(node.state == .started)

                let registrations = {
                    app.routes.registeredProtocols.filter { $0.stringValue == KadDHT.multicodec }.count
                }
                #expect(registrations() == 1, "start() should have installed the route exactly once")

                node.stop()
                #expect(node.state == .stopped)

                try node.start()
                #expect(node.state == .started)
                #expect(registrations() == 1, "a restart must not add a second copy of the route")
            }
        }

        /// A client never registers the route at all, so restarting it exercises the same path with
        /// nothing installed.
        @Test func aClientCanBeRestartedAndDoesntRegisterTheRoute() async throws {
            try await withApp(configure: self.host(mode: .client)) { app in
                let node = app.dht.kadDHT

                let registrations = {
                    app.routes.registeredProtocols.filter { $0.stringValue == KadDHT.multicodec }.count
                }

                #expect(registrations() == 0, "a client must not register the route")
                node.stop()

                try node.start()
                #expect(node.state == .started)
                #expect(registrations() == 0, "a client must not register the route, even upon restart")
            }
        }

        /// Starting an already-started node is a warning, not an error, and doesn't re-register.
        @Test func startingTwiceIsAnUnstartedNoOp() async throws {
            try await withApp(configure: self.host(mode: .server)) { app in
                let node = app.dht.kadDHT
                #expect(node.state == .started)
                try node.start()
                #expect(node.state == .started)
            }
        }

        // MARK: - Shutdown

        /// The heartbeat's cancellation completes on the node's own event loop, so the old `stop()`
        /// could never make progress when called *from* that loop. It has to cancel without waiting instead.
        ///
        /// - Note: `autoUpdate` is on so there's a real `RepeatedTask` to cancel.
        @Test func stoppingFromTheEventLoopDoesNotDeadlock() async throws {
            try await withApp(configure: self.host(mode: .client, autoUpdate: true)) { app in
                let node = app.dht.kadDHT

                /// This shouldn't stall unless we regress
                try await node.eventLoop.submit { node.stop() }.get()

                /// Cancellation was scheduled rather than awaited, so settle it off-loop.
                await node.stop()
                #expect(node.state == .stopped)
            }
        }

        @Test func shutdownIsIdempotent() async throws {
            try await withApp(configure: self.host(mode: .client, autoUpdate: true)) { app in
                let node = app.dht.kadDHT

                try await node.shutdown().get()
                #expect(node.state == .stopped)

                try await node.shutdown().get()
                #expect(node.state == .stopped)
            }
        }

        @Test func aStoppedNodeRestartsItsHeartbeat() async throws {
            try await withApp(configure: self.host(mode: .client, autoUpdate: true)) { app in
                let node = app.dht.kadDHT

                await node.stop()
                try node.start()
                #expect(node.state == .started)

                await node.stop()
                #expect(node.state == .stopped)
            }
        }
    }
}
