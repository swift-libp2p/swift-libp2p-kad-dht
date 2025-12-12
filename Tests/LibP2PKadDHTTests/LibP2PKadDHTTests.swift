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

import CID
import CryptoSwift
import LibP2P
import LibP2PNoise
import LibP2PYAMUX
import Multihash
import Testing

@testable import LibP2PKadDHT

@Suite("Libp2p KadDHT Tests", .serialized)
final class LibP2PKadDHTTests {

    @Test func testEventLoopArray() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { try! group.syncShutdownGracefully() }
        let arr = EventLoopArray(String.self, on: group.next())

        for val in (0..<10) {
            let _ = group.next().submit {
                arr.append("Item[\(val)]")
                arr.append("Thing[\(val)]")
            }
        }

        sleep(1)

        #expect(try arr.count().wait() == 20)

        print(try arr.all().wait())

        try arr.removeAll(where: { str in
            str.contains("Thing")
        }).wait()

        #expect(try arr.count().wait() == 10)

        print(try arr.all().wait())

        for _ in (0..<10) {
            group.next().scheduleTask(
                in: .milliseconds(Int64.random(in: 0...100)),
                {
                    arr.remove(at: 0)
                }
            )
        }

        sleep(1)

        #expect(try arr.count().wait() == 0)

        print(try arr.all().wait())
    }

    @Test func testEventLoopDictionary() throws {
        let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        defer { try! group.syncShutdownGracefully() }
        let d = EventLoopDictionary<String, String>(on: group.next())

        for (idx, val) in (0..<10).enumerated() {
            let _ = group.next().submit {
                d.append(key: "Item[\(val)]", value: "Thing[\(idx)]")
                d.append(key: "Item2[\(val)]", value: "Thing2[\(idx)]")
            }
        }

        sleep(1)

        #expect(try d.count().wait() == 20)

        print(try d.all().wait())

        try d.removeAll(where: { key, val in
            val.contains("Thing2")
        }).wait()

        #expect(try d.count().wait() == 10)

        print(try d.all().wait())

        for idx in (0..<10) {
            group.next().scheduleTask(
                in: .milliseconds(Int64.random(in: 0...100)),
                {
                    d.removeValue(forKey: "Item[\(idx)]")
                }
            )
        }

        sleep(1)

        #expect(try d.count().wait() == 0)

        print(try d.all().wait())
    }

    @Test func testPubKeyRecordValidator() throws {
        let dhtRecordData = Data(
            hex:
                "0a262f706b2f1220b04a57d40eca138809f139a76b12044333c3740391c9bf1ce9d8e21a79210bfd12ab04080012a60430820222300d06092a864886f70d01010105000382020f003082020a0282020100a1f5c0e7c0d5e556afc0e84566f8c565773adb548ddc219ca9688613a0096c2dfd069804c84968545b9c9df19dd131cc8408b7781df7ddfaf208a42a821523ce03955164a62dcab6bd10dd26f8507517567ca128f00a056d8636b9549ddb59ca727628775c90bd91d6251adbdfd36bf68a09c3bfe69e1b1587e8f31a4b55afc8095e7b6f6683165f9c0ef0ad1b22d8b73749ee02aa46566cd5f7a9ff6eb1099fe36b363abd4e1293108a6d473a349e77aca15e49b20ffe61b4222eb3a634e8481d71a7fdceea88a2044fa5cedde1dee314e27880bc713ca578814684e85e0d21cff40e23c341f13ee1a06452f284664999862973e51d692b578cd9b7de89d786ad6baebcf8dfc343db8eda434a15929591917c52bf16741359149d0e7092bc919928f1d5b25cb48b0f90a7a05b0eb29adca993f893c6fb137a53a5c470a8a309b574bb4fd80879bde7dcc237eaf2ce9a17b9193032df99c8bf551987561ee264a09730f9029610571625e0d0e1e2a7f90469a6a480ed08cf9b4c3af0567bfe9abf470079d8cc7d7f22efc83598f86c9e0678caf79e2299a99c47c8d057e7f3b8af40185c8dd499a1c167c358d7ab83af6581944ce0b8b6bd2cfe4bf80c8c9e7f61fe94816df79e12ae5e82c588f894b86fd599da5912f8754de2a23f2d1529845a5570a72d8d8537325b95dd3c69d9ca30b8186c20170d10955b7da216822c7302030100012a1e323032322d30392d31325431343a34363a35322e3839323937333034325a"
        )

        // Ensure we can instantiate a DHT.Record Protobuf
        let dhtRecord = try DHT.Record(serializedBytes: dhtRecordData)

        let pubKeyRecordValidator = KadDHT.PubKeyValidator()
        #expect(throws: Never.self) {
            try pubKeyRecordValidator.validate(key: dhtRecord.key.byteArray, value: dhtRecordData.byteArray)
        }
    }

    @Test func testPeerRecord() throws {
        let recordData = Data(
            hex:
                "080012a60430820222300d06092a864886f70d01010105000382020f003082020a0282020100a1f5c0e7c0d5e556afc0e84566f8c565773adb548ddc219ca9688613a0096c2dfd069804c84968545b9c9df19dd131cc8408b7781df7ddfaf208a42a821523ce03955164a62dcab6bd10dd26f8507517567ca128f00a056d8636b9549ddb59ca727628775c90bd91d6251adbdfd36bf68a09c3bfe69e1b1587e8f31a4b55afc8095e7b6f6683165f9c0ef0ad1b22d8b73749ee02aa46566cd5f7a9ff6eb1099fe36b363abd4e1293108a6d473a349e77aca15e49b20ffe61b4222eb3a634e8481d71a7fdceea88a2044fa5cedde1dee314e27880bc713ca578814684e85e0d21cff40e23c341f13ee1a06452f284664999862973e51d692b578cd9b7de89d786ad6baebcf8dfc343db8eda434a15929591917c52bf16741359149d0e7092bc919928f1d5b25cb48b0f90a7a05b0eb29adca993f893c6fb137a53a5c470a8a309b574bb4fd80879bde7dcc237eaf2ce9a17b9193032df99c8bf551987561ee264a09730f9029610571625e0d0e1e2a7f90469a6a480ed08cf9b4c3af0567bfe9abf470079d8cc7d7f22efc83598f86c9e0678caf79e2299a99c47c8d057e7f3b8af40185c8dd499a1c167c358d7ab83af6581944ce0b8b6bd2cfe4bf80c8c9e7f61fe94816df79e12ae5e82c588f894b86fd599da5912f8754de2a23f2d1529845a5570a72d8d8537325b95dd3c69d9ca30b8186c20170d10955b7da216822c730203010001"
        )

        let pub = try PeerID(marshaledPublicKey: recordData)
        print(pub)
        print(pub.b58String)
        print(pub.cidString)
    }

    @Test func testExtractNamespaceFromKey() throws {
        let peerID = "QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
        //let peerID = "QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN"

        let key = try "/pk/".bytes + PeerID(cid: peerID).id  // or CID(...).multihash.value
        print(key)
        print("/pk/\(peerID)".bytes)

        print(self.extractNamespace(key) ?? [])

        print(String(data: Data(self.extractNamespace(key)!), encoding: .utf8) ?? "NIL")

        #expect(
            key == [
                47, 112, 107, 47, 18, 32, 176, 74, 87, 212, 14, 202, 19, 136, 9, 241, 57, 167, 107, 18, 4, 67, 51, 195,
                116, 3, 145, 201, 191, 28, 233, 216, 226, 26, 121, 33, 11, 253,
            ]
        )
    }

    /// Extracts a namespace from the front of a Key if one exists...
    ///
    /// - Note: "/" in utf8 == 47
    private func extractNamespace(_ key: [UInt8]) -> [UInt8]? {
        guard key.first == UInt8(47) else { return nil }
        guard let idx = key.dropFirst().firstIndex(of: UInt8(47)) else { return nil }
        return Array(key[1..<idx])
    }

    @Test func testTimeRecievedStringToDate1() throws {
        let timeReceived1 = "2022-09-12T14:46:52.892973042Z"
        let timeReceived2 = "2022-09-12T14:46:52.892973041Z"
        let timeReceived3 = "2022-09-12T14:46:52.892973043Z"
        let timeReceivedSame = "2022-09-12T14:46:52.892973042Z"

        let date1 = try RFC3339Date(string: timeReceived1)
        let date2 = try RFC3339Date(string: timeReceived2)
        let date3 = try RFC3339Date(string: timeReceived3)
        let dateSame = try RFC3339Date(string: timeReceivedSame)

        #expect(date1.string == timeReceived1)
        #expect(date2.string == timeReceived2)
        #expect(date3.string == timeReceived3)
        #expect(dateSame.string == timeReceivedSame)

        #expect(date1 == dateSame)
        #expect(date2 < date1)
        #expect(date3 > date1)

        let now1 = RFC3339Date()
        print(now1.string)
        let now2 = RFC3339Date()
        print(now2.string)

        /// The RFC3339Dates will NOT be equal as long as we keep the print statements between the initializers (this is consistant with how Date() works, two immediate calls can result in equal values)
        #expect(now1 < now2)
        #expect(now1.string != now2.string)
    }
}

struct TestHelper {
    static var externalIntegrationTestsEnabled: Bool {
        if let b = ProcessInfo.processInfo.environment["PerformExternalIntegrationTests"], b == "true" {
            return true
        }
        return false
    }

    static var internalIntegrationTestsEnabled: Bool {
        if let b = ProcessInfo.processInfo.environment["PerformInternalIntegrationTests"], b == "true" {
            return true
        }
        return false
    }
}

extension Trait where Self == ConditionTrait {
    /// This test is only available when the `PerformExternalIntegrationTests` environment variable is set to `true`
    public static var externalIntegrationTestsEnabled: Self {
        enabled(
            if: TestHelper.externalIntegrationTestsEnabled,
            "This test is only available when the `PerformExternalIntegrationTests` environment variable is set to `true`"
        )
    }

    /// This test is only available when the `PerformInternalIntegrationTests` environment variable is set to `true`
    public static var internalIntegrationTestsEnabled: Self {
        enabled(
            if: TestHelper.internalIntegrationTestsEnabled,
            "This test is only available when the `PerformInternalIntegrationTests` environment variable is set to `true`"
        )
    }
}
