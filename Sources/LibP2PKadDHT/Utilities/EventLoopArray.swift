//
//  EventLoopArray.swift
//
//
//  Created by Brandon Toms on 7/24/23.
//

import NIOCore

public class EventLoopArray<Element> {
    private let eventLoop:EventLoop
    private var store:[Element]
    
    init(_:Element.Type, on el:EventLoop) {
        self.store = Array<Element>()
        self.eventLoop = el
    }
    
    @discardableResult func append(_ value:Element) -> EventLoopFuture<Void> {
        self.eventLoop.submit {
            self.store.append(value)
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
    
    @discardableResult func remove(at idx:Int) -> EventLoopFuture<Element> {
        self.eventLoop.submit {
            self.store.remove(at: idx)
        }
    }
    
    @discardableResult func removeAll(where shouldBeRemoved:@escaping (Element) throws -> Bool) -> EventLoopFuture<Void> {
        return self.eventLoop.submit {
            try self.store.removeAll(where: shouldBeRemoved)
        }
    }
}
