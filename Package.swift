// swift-tools-version: 5.5
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
        .package(url: "https://github.com/swift-libp2p/swift-libp2p.git", .upToNextMinor(from: "0.1.0")),
        .package(url: "https://github.com/swift-libp2p/swift-libp2p-noise.git", .upToNextMajor(from: "0.1.0")),
        .package(url: "https://github.com/swift-libp2p/swift-libp2p-mplex.git", .upToNextMajor(from: "0.1.0")),

        .package(url: "https://github.com/swift-libp2p/swift-libp2p-crypto.git", .upToNextMajor(from: "0.0.1")),
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
                .copy("Protobufs/DHT.proto")
                //.copy("Protobufs/RPC2.proto")
            ]
        ),
        .testTarget(
            name: "LibP2PKadDHTTests",
            dependencies: [
                "LibP2PKadDHT",
                .product(name: "LibP2PNoise", package: "swift-libp2p-noise"),
                .product(name: "LibP2PMPLEX", package: "swift-libp2p-mplex"),
                .product(name: "LibP2PCrypto", package: "swift-libp2p-crypto"),
            ]
        ),
    ]
)
