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

import NIOCore

public class EventLoopDictionary<Key, Value> where Key:Hashable {
    public typealias Element = (key: Key, value: Value)
    
    private let eventLoop:EventLoop
    private var store:[Key:Value]
    
    init(on el:EventLoop) {
        self.store = Dictionary<Key, Value>()
        self.eventLoop = el
    }
    
    init(key:Key.Type, value:Value.Type, on el:EventLoop) {
        self.store = Dictionary<Key, Value>()
        self.eventLoop = el
    }

    @discardableResult func append(key:Key, value:Value) -> EventLoopFuture<Void> {
        self.eventLoop.submit {
            self.store[key] = value
        }
    }
    
    @discardableResult func updateValue(_ value:Value, forKey key:Key) -> EventLoopFuture<Value?> {
        self.eventLoop.submit {
            self.store.updateValue(value, forKey: key)
        }
    }
    
    func getValue(forKey key:Key) -> EventLoopFuture<Value?> {
        self.eventLoop.submit {
            self.store[key]
        }
    }
    
    func getValue(forKey key:Key, default:Value) -> EventLoopFuture<Value> {
        self.eventLoop.submit {
            if let val = self.store[key] {
                return val
            } else {
                self.store[key] = `default`
                return `default`
            }
        }
    }
    
    func all() -> EventLoopFuture<[Element]> {
        self.eventLoop.submit {
            self.store.map { $0 }
        }
    }
    
    func count() -> EventLoopFuture<Int> {
        self.eventLoop.submit {
            self.store.count
        }
    }

    @discardableResult func removeValue(forKey key:Key) -> EventLoopFuture<Value?> {
        self.eventLoop.submit {
            self.store.removeValue(forKey: key)
        }
    }

    @discardableResult func removeAll(where shouldBeRemoved:@escaping (Element) throws -> Bool) -> EventLoopFuture<Void> {
        return self.eventLoop.submit {
            let elementsToBeRemoved = try self.store.filter(shouldBeRemoved)
            for element in elementsToBeRemoved { self.store.removeValue(forKey: element.key) }
        }
    }
    
    func filter(where shouldBeRemoved:@escaping (Element) throws -> Bool) -> EventLoopFuture<Dictionary<Key,Value>> {
        return self.eventLoop.submit {
            try self.store.filter(shouldBeRemoved)
        }
    }
    
    func mapValues<T>(_ transform:@escaping (Value) throws -> T) rethrows -> EventLoopFuture<[Key : T]> {
        self.eventLoop.submit {
            try self.store.mapValues(transform)
        }
    }
    
    func compactMapValues<T>(_ transform:@escaping (Value) throws -> T?) rethrows -> EventLoopFuture<[Key : T]> {
        self.eventLoop.submit {
            try self.store.compactMapValues(transform)
        }
    }
}

extension EventLoopDictionary where Key == KadDHT.Key, Value == DHT.Record {
    func addKeyIfSpaceOrCloser(key kid:KadDHT.Key, value:DHT.Record, usingValidator validator:Validator, maxStoreSize:Int, targetKey:KadDHT.Key) -> EventLoopFuture<EventLoopDictionary.StoreResult> {
        self.eventLoop.submit {
            if let existingRecord = self.store[kid] {
                /// Store the best record...
                let values = [existingRecord, value].compactMap { try? $0.serializedData().bytes }
                let bestIndex = (try? validator.select(key: kid.original, values: values)) ?? 0
                let best = (try? DHT.Record(contiguousBytes: values[bestIndex])) ?? existingRecord
                
                /// Update the value...
                self.store[kid] = best
                return best == existingRecord ? .alreadyExists : .updatedValue
                                
            } else if self.store.count < maxStoreSize {
                /// We have space, so lets add it...
                self.store[kid] = value
                return .excessSpace
                
            } else {
                /// Fetch all current keys, sort by distance to us, if this key is closer than the furthest one, replace it
                let keys = self.store.keys.sorted { lhs, rhs in
                    targetKey.compareDistancesFromSelf(to: lhs, and: rhs) == .firstKey
                }
                
                if let furthestKey = keys.last, targetKey.compareDistancesFromSelf(to: kid, and: furthestKey) == .firstKey {
                    /// The new key is closer than our furthest key so lets drop the furthest and add the new key
                    let old = self.store.removeValue(forKey: furthestKey)
                    self.store[kid] = value
                    return .storedCloser(furthestKey, old)
                } else {
                    /// This new value is further away then all of our current keys, lets drop it...
                    return .notStoredFurther
                }
            }
        }
    }
}

extension EventLoopDictionary where Key == KadDHT.Key, Value == [DHT.Message.Peer] {
    /// Randomly prunes entries until the store is at the count specified
    func prune(toAmount:Int) -> EventLoopFuture<Void> {
        self.eventLoop.submit {
            let amount = max(0, toAmount)
            while self.store.count > amount {
                let _ = self.store.popFirst()
            }
        }
    }
}

extension EventLoopDictionary {
    enum StoreResult {
        case excessSpace
        case alreadyExists
        case updatedValue
        case storedCloser(KadDHT.Key, DHT.Record?)
        case notStoredFurther
        
        var wasAdded:Bool {
            switch self {
            case .notStoredFurther:
                return false
            default:
                return true
            }
        }
    }
}
