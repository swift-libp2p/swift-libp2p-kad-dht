//
//  ID.swift
//  
//
//  Created by Brandon Toms on 4/29/22.
//

import LibP2P
import CryptoSwift

typealias ID = [UInt8]

extension ID {
    func commonPrefixLength(with:ID) -> Int {
        self.commonPrefixLengthBits(with: with)
    }
    
    func commonPrefixLengthBytes(with:ID) -> Int {
        self.enumerated().first { elem in
            elem.element != with[elem.offset]
        }?.offset ?? 0
    }
    
//    func commonPrefixLengthBits(with:ID) -> Int {
//        let clpBytes = self.enumerated().first { elem in
//            elem.element != with[elem.offset]
//        }?.offset ?? self.count
//
//        var bits = clpBytes * 8
//        if clpBytes < self.count {
//            let bits1:[Bit] = self[clpBytes].bits()
//            let bits2:[Bit] = with[clpBytes].bits()
//
//            let clpBits = bits1.enumerated().first { elem in
//                elem.element != bits2[elem.offset]
//            }?.offset ?? 8
//            bits += clpBits
//        }
//
//        return bits
//    }
    
    func commonPrefixLengthBits(with key:ID) -> Int {
        distanceBetween(key: self, and: key).zeroPrefixLength()
    }
    
    private func distanceBetween(key key1:[UInt8], and key2:[UInt8]) -> [UInt8] {
        guard key1.count == key2.count else { print("Error: Keys must be the same length"); return [] }
        return key1.enumerated().map { idx, byte in key2[idx] ^ byte }
    }
}

