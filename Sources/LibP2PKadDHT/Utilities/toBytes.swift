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

import Foundation
import CoreFoundation

protocol UIntToBytesConvertable {
    var toBytes: [UInt8] { get }
}

extension UIntToBytesConvertable {
    func toByteArr<T: BinaryInteger>(endian: T, count: Int) -> [UInt8] {
        var _endian = endian
        let bytePtr = withUnsafePointer(to: &_endian) {
            $0.withMemoryRebound(to: UInt8.self, capacity: count) {
                UnsafeBufferPointer(start: $0, count: count)
            }
        }
        return [UInt8](bytePtr)
    }
}

extension UInt64: UIntToBytesConvertable {
    var toBytes: [UInt8] {
        if CFByteOrderGetCurrent() == Int(CFByteOrderLittleEndian.rawValue) {
            return toByteArr(endian: self.littleEndian,
                             count: MemoryLayout<UInt64>.size)
        } else {
            return toByteArr(endian: self.bigEndian,
                             count: MemoryLayout<UInt64>.size)
        }
    }
}

extension ContiguousBytes {
    var toBytes: [UInt8] {
        self.withUnsafeBytes { Array($0) }
    }

    var asData: Data {
        Data(self.toBytes)
    }
}
