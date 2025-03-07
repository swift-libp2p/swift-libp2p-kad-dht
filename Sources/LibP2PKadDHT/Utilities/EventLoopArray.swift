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

public class EventLoopArray<Element> {
    private let eventLoop: EventLoop
    private var store: [Element]

    init(_: Element.Type, on el: EventLoop) {
        self.store = [Element]()
        self.eventLoop = el
    }

    @discardableResult func append(_ value: Element) -> EventLoopFuture<Void> {
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

    @discardableResult func remove(at idx: Int) -> EventLoopFuture<Element> {
        self.eventLoop.submit {
            self.store.remove(at: idx)
        }
    }

    @discardableResult func removeAll(
        where shouldBeRemoved: @escaping (Element) throws -> Bool
    ) -> EventLoopFuture<Void> {
        self.eventLoop.submit {
            try self.store.removeAll(where: shouldBeRemoved)
        }
    }
}
