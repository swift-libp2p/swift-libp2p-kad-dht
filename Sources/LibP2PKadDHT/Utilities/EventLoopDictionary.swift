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
import NIOCore

/// An eventloop bound dictionary
///
/// - Note: Because we're bound to a specific eventloop we know we're thread safe here
public final class EventLoopDictionary<Key, Value>: @unchecked Sendable
where Key: Hashable & Sendable, Value: Sendable {
    public typealias Element = (key: Key, value: Value)

    private let eventLoop: EventLoop
    private var store: [Key: Value]

    init(on el: EventLoop) {
        self.store = [Key: Value]()
        self.eventLoop = el
    }

    init(key: Key.Type, value: Value.Type, on el: EventLoop) {
        self.store = [Key: Value]()
        self.eventLoop = el
    }

    @discardableResult func append(key: Key, value: Value) -> EventLoopFuture<Void> {
        self.eventLoop.submit {
            self.store[key] = value
        }
    }

    @discardableResult func updateValue(_ value: Value, forKey key: Key) -> EventLoopFuture<Value?> {
        self.eventLoop.submit {
            self.store.updateValue(value, forKey: key)
        }
    }

    func getValue(forKey key: Key) -> EventLoopFuture<Value?> {
        self.eventLoop.submit {
            self.store[key]
        }
    }

    func getValue(forKey key: Key, default: Value) -> EventLoopFuture<Value> {
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

    @discardableResult func removeValue(forKey key: Key) -> EventLoopFuture<Value?> {
        self.eventLoop.submit {
            self.store.removeValue(forKey: key)
        }
    }

    @discardableResult func removeAll(
        where shouldBeRemoved: @Sendable @escaping (Element) throws -> Bool
    ) -> EventLoopFuture<Void> {
        self.eventLoop.submit {
            let elementsToBeRemoved = try self.store.filter(shouldBeRemoved)
            for element in elementsToBeRemoved { self.store.removeValue(forKey: element.key) }
        }
    }

    func filter(where shouldBeRemoved: @Sendable @escaping (Element) throws -> Bool) -> EventLoopFuture<[Key: Value]> {
        self.eventLoop.submit {
            try self.store.filter(shouldBeRemoved)
        }
    }

    func mapValues<T>(_ transform: @Sendable @escaping (Value) throws -> T) rethrows -> EventLoopFuture<[Key: T]> {
        self.eventLoop.submit {
            try self.store.mapValues(transform)
        }
    }

    func compactMapValues<T>(
        _ transform: @Sendable @escaping (Value) throws -> T?
    ) rethrows -> EventLoopFuture<[Key: T]> {
        self.eventLoop.submit {
            try self.store.compactMapValues(transform)
        }
    }
}

extension EventLoopDictionary where Key == KadDHT.Key, Value == DHT.Record {

    /// Returns the record held for `kid`, unless it has aged past `maxAge`, in which case the entry
    /// is evicted and `nil` returned.
    func getUnexpiredValue(
        forKey kid: Key,
        maxAge: TimeInterval,
        now: Date = Date()
    ) -> EventLoopFuture<Value?> {
        self.eventLoop.submit {
            guard let record = self.store[kid] else { return nil }
            guard KadDHT.isExpired(record, maxAge: maxAge, now: now) else { return record }
            self.store.removeValue(forKey: kid)
            return nil
        }
    }

    /// Every record that hasn't aged past `maxAge`.
    func unexpiredValues(maxAge: TimeInterval, now: Date = Date()) -> EventLoopFuture<[Element]> {
        self.eventLoop.submit {
            self.store.filter { !KadDHT.isExpired($0.value, maxAge: maxAge, now: now) }.map { $0 }
        }
    }

    /// Evicts every record that has aged past `maxAge`, returning how many were dropped.
    @discardableResult
    func removeExpiredValues(maxAge: TimeInterval, now: Date = Date()) -> EventLoopFuture<Int> {
        self.eventLoop.submit {
            let expired = self.store.filter { KadDHT.isExpired($0.value, maxAge: maxAge, now: now) }
            for entry in expired { self.store.removeValue(forKey: entry.key) }
            return expired.count
        }
    }

    /// Attempts to add the record for the given key using the validator provided
    func addKeyIfSpaceOrCloser(
        key kid: KadDHT.Key,
        value: DHT.Record,
        usingValidator validator: Validator,
        maxStoreSize: Int,
        targetKey: KadDHT.Key
    ) -> EventLoopFuture<EventLoopDictionary.StoreResult> {
        self.eventLoop.submit {
            if let existingRecord = self.store[kid] {
                /// We have an existing record for this key, lets make sure we keep the best one...
                let values = [existingRecord, value].compactMap { try? $0.serializedData().byteArray }

                let best: DHT.Record
                if values.isEmpty {
                    best = existingRecord
                } else {
                    /// ask the validator for the index of the best record
                    let selected = (try? validator.select(key: kid.original, values: values)) ?? 0
                    /// ensure the index is valid for our values
                    let bestIndex = values.indices.contains(selected) ? selected : 0
                    /// set the best record
                    best = (try? DHT.Record(serializedBytes: values[bestIndex])) ?? existingRecord
                }

                /// Update the store with the best record...
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

                if let furthestKey = keys.last,
                    targetKey.compareDistancesFromSelf(to: kid, and: furthestKey) == .firstKey
                {
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
    func prune(toAmount: Int) -> EventLoopFuture<Void> {
        self.eventLoop.submit {
            let amount = max(0, toAmount)
            while self.store.count > amount {
                let _ = self.store.popFirst()
            }
        }
    }
}

extension EventLoopDictionary {
    enum StoreResult: Sendable {
        case excessSpace
        case alreadyExists
        case updatedValue
        case storedCloser(KadDHT.Key, DHT.Record?)
        case notStoredFurther

        var wasAdded: Bool {
            switch self {
            case .notStoredFurther:
                return false
            default:
                return true
            }
        }
    }
}
