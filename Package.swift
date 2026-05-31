// swift-tools-version: 6.0
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

import PackageDescription

let package = Package(
    name: "swift-libp2p-kad-dht",
    platforms: [
        .macOS(.v10_15),
        .iOS(.v13),
    ],
    products: [
        // Products define the executables and libraries a package produces, and make them visible to other packages.
        .library(
            name: "LibP2PKadDHT",
            targets: ["LibP2PKadDHT"]
        )
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        // jerimiah797/swift-libp2p (burrows-impl): tag 0.3.5 +
        // defensive guards for post-shutdown property access
        // (isShuttingDown flag + per-accessor early returns +
        // TCPServer.listeningAddress race fix). Pinned by
        // revision so SPM resolves deterministically — and so
        // downstream Burrows can pin the same fork without a
        // chain-of-identity conflict (two different repos
        // claiming the `swift-libp2p` identity).
        .package(
            url: "https://github.com/jerimiah797/swift-libp2p.git",
            revision: "c5e1533a6914fba74bae23b4735dc7890810b981"
        ),

        // Testing dependencies
        .package(url: "https://github.com/swift-libp2p/swift-libp2p-noise.git", .upToNextMinor(from: "0.2.0")),
        .package(url: "https://github.com/swift-libp2p/swift-libp2p-yamux.git", .upToNextMinor(from: "0.2.0")),
        .package(url: "https://github.com/swift-libp2p/swift-libp2p-crypto.git", .upToNextMinor(from: "0.2.0")),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages this package depends on.
        .target(
            name: "LibP2PKadDHT",
            dependencies: [
                .product(name: "LibP2P", package: "swift-libp2p")
            ],
            resources: [
                .copy("Protobufs/DHT.proto"),
                .copy("Protobufs/IPNSRecord.proto"),
            ]
        ),
        .testTarget(
            name: "LibP2PKadDHTTests",
            dependencies: [
                "LibP2PKadDHT",
                .product(name: "LibP2PNoise", package: "swift-libp2p-noise"),
                .product(name: "LibP2PYAMUX", package: "swift-libp2p-yamux"),
                .product(name: "LibP2PCrypto", package: "swift-libp2p-crypto"),
            ]
        ),
    ]
)
