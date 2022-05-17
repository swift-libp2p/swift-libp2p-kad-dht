//
//  ID.swift
//  
//
//  Created by Brandon Toms on 4/29/22.
//

import LibP2P

typealias ID = [UInt8]

extension ID {
    func commonPrefixLength(with:ID) -> Int {
        self.enumerated().first { elem in
            elem.element != with[elem.offset]
        }?.offset ?? 0
    }
}
