//
//  Validator.swift
//  
//
//  Created by Brandon Toms on 10/22/22.
//

import LibP2P

public protocol Validator {
    func validate(key:[UInt8], value:[UInt8]) throws
    func select(key:[UInt8], values:[[UInt8]]) throws -> Int
}

extension DHT {
    struct BaseValidator:Validator {
        let validateFunction:((_ key:[UInt8], _ value:[UInt8]) throws -> Void)
        let selectFunction:((_ key:[UInt8], _ values:[[UInt8]]) throws -> Int)
        
        init(validationFunction:@escaping (_ key:[UInt8], _ value:[UInt8]) throws -> Void, selectFunction:@escaping (_ key:[UInt8], _ values:[[UInt8]]) throws -> Int) {
            self.validateFunction = validationFunction
            self.selectFunction = selectFunction
        }
        
        func validate(key:[UInt8], value:[UInt8]) throws {
            return try self.validateFunction(key, value)
        }
        
        func select(key:[UInt8], values:[[UInt8]]) throws -> Int {
            return try selectFunction(key, values)
        }
    }
    
    struct PubKeyValidator:Validator {
        func validate(key: [UInt8], value: [UInt8]) throws {
            let record = try DHT.Record(contiguousBytes: value)
            guard Data(key) == record.key else { throw NSError(domain: "Validator::Key Mismatch. Expected \(Data(key)) got \(record.key) ", code: 0) }
            let _ = try PeerID(marshaledPublicKey: Data(record.value))
        }
        
        func select(key: [UInt8], values: [[UInt8]]) throws -> Int {
            let records = values.map { try? DHT.Record(contiguousBytes: $0) }
            guard !records.compactMap({ $0 }).isEmpty else { throw NSError(domain: "Validator::No Records to select", code: 0) }
            guard records.count > 1 && records[0] != nil else { return 0 }

            var bestValueIndex:Int = 0
            var bestValue:DHT.Record? = nil
            for (index, record) in records.enumerated() {
                guard let record = record else { continue }
                guard let newRecord = try? RFC3339Date(string: record.timeReceived) else { continue }
                
                if let currentBest = bestValue {
                    guard let currentRecord = try? RFC3339Date(string: currentBest.timeReceived) else { continue }
                    // If this record is more recent then our currentBest, update our current best!
                    if newRecord > currentRecord {
                        bestValue = record
                        bestValueIndex = index
                    }
                } else {
                    // If we don't have a current best, set it!
                    bestValue = record
                    bestValueIndex = index
                }
            }
            
            guard bestValue != nil else { throw NSError(domain: "Validator::Failed to select a valid record", code: 0) }
            return bestValueIndex
        }
    }
    
    struct IPNSValidator:Validator {
        func validate(key: [UInt8], value: [UInt8]) throws {
            let record = try DHT.Record(contiguousBytes: value)
            guard Data(key) == record.key else { throw NSError(domain: "Validator::Key Mismatch. Expected \(Data(key)) got \(record.key) ", code: 0) }
            let _ = try IpnsEntry(contiguousBytes: record.value)
        }
        
        func select(key: [UInt8], values: [[UInt8]]) throws -> Int {
            let records = values.map { try? DHT.Record(contiguousBytes: $0) }
            guard !records.compactMap({ $0 }).isEmpty else { throw NSError(domain: "Validator::No Records to select", code: 0) }
            guard records.count > 1 && records[0] != nil else { return 0 }
            
            var bestValueIndex:Int = 0
            var bestValue:DHT.Record? = nil
            for (index, record) in records.enumerated() {
                guard let record = record else { continue }
                guard let newRecord = try? RFC3339Date(string: record.timeReceived) else { continue }
                
                if let currentBest = bestValue {
                    guard let currentRecord = try? RFC3339Date(string: currentBest.timeReceived) else { continue }
                    // If this record is more recent then our currentBest, update our current best!
                    if newRecord > currentRecord {
                        bestValue = record
                        bestValueIndex = index
                    }
                } else {
                    // If we don't have a current best, set it!
                    bestValue = record
                    bestValueIndex = index
                }
            }
            
            guard bestValue != nil else { throw NSError(domain: "Validator::Failed to select a valid record", code: 0) }
            return bestValueIndex
        }
    }
}
