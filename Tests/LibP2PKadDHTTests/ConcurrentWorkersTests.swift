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

import CryptoSwift
import LibP2P
import LibP2PCrypto
import XCTest

@testable import LibP2PKadDHT

class ConcurrentWorkersTests: XCTestCase {

    override func setUpWithError() throws {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDownWithError() throws {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    /// This test spawns a group of workers that work on a single (thread protected) list of 'work'.
    /// Each worker checks for work to be done, recursively, then performs the work on their own eventloop and returns when there is no more work to be done.
    /// - Note: This example allocates all work up front (doesn't add new work to the queue over time).
    /// - Note: Our LookupLists use a different algorithm than this...
    func testEventLoopGroupConcurrentWorkers() throws {

        let group = MultiThreadedEventLoopGroup(numberOfThreads: 4)

        let workers: Int = 4

        var stuffToDo: [(taskDuration: UInt32, processed: Bool)] = (0..<10).map {
            i -> (taskDuration: UInt32, processed: Bool) in
            (UInt32.random(in: 10_000...1_000_000), false)
        }

        print(stuffToDo)

        let mainLoop = group.next()

        func nextTask() -> EventLoopFuture<UInt32?> {
            mainLoop.submit {
                guard let next = stuffToDo.firstIndex(where: { $0.processed == false }) else { return nil }
                print("Dequeing task \(next) for work")
                stuffToDo[next].processed = true
                return stuffToDo[next].taskDuration
            }
        }

        func recursivelyWork(on: EventLoop) -> EventLoopFuture<Void> {
            nextTask().flatMap { task in
                on.flatSubmit {
                    guard let task = task else { return on.makeSucceededVoidFuture() }
                    self.doSomeWork(duration: task)
                    return recursivelyWork(on: on)
                }
            }
        }

        let workExpectation = expectation(description: "waiting for work")

        (0..<workers).compactMap { _ -> EventLoopFuture<Void> in
            print("Deploying Worker")
            return recursivelyWork(on: group.next())
        }.flatten(on: mainLoop).whenComplete { result in
            switch result {
            case .failure(let error):
                print("Error: \(error)")
            case .success:
                print("All done with work")
                print(stuffToDo)
            }
            workExpectation.fulfill()
        }

        waitForExpectations(timeout: 11, handler: nil)
        print("Shutting down event loop")
        try! group.syncShutdownGracefully()
    }

    private func doSomeWork(duration: UInt32) {
        usleep(duration)
    }

    //    class WorkerGroup {
    //        let maxConcurrentWorkers:Int
    //        let group:MultiThreadedEventLoopGroup
    //        var tasks:[(Int) -> EventLoopFuture<[Int]>]
    //    }

    //    struct Worker {
    //        let eventloop:EventLoop
    //        var isWorking:Bool = false
    //
    //        mutating func markWorking(_ working:Bool) {
    //            isWorking = working
    //        }
    //    }
    //
    //    /// This test spawns a group of workers that work on a single (thread protected) list of 'work'.
    //    /// Each worker checks for work to be done, recursively, then performs the work on their own eventloop and returns when there is no more work to be done.
    //    func testEventLoopGroupConcurrentWorkersSmart() throws {
    //
    //        let group = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
    //
    //        let maxWorkers:Int = 2
    //        var workers:[Worker] = []
    //
    //        var stuffToDo:[(taskDuration:UInt32, processed:Bool)] = (0..<10).map { i -> (taskDuration:UInt32, processed:Bool) in
    //            return (UInt32.random(in: 2...5), false)
    //        }
    //
    //        let mainLoop = group.next()
    //        let timeoutPerTask:TimeAmount = .seconds(6)
    //        let timeoutPerGroup:TimeAmount = .minutes(1)
    //
    //        /// Create our workers
    //        workers = (0..<maxWorkers).map { _ in Worker(eventloop: group.next()) }
    //
    //        func nextTask() -> EventLoopFuture<UInt32?> {
    //            mainLoop.submit {
    //                guard let next = stuffToDo.firstIndex(where: { $0.processed == false }) else { return nil }
    //                print("Dequeing task \(next) for work")
    //                stuffToDo[next].processed = true
    //                return stuffToDo[next].taskDuration
    //            }
    //        }
    //
    //        func nextWorker() -> Worker? {
    //            mainLoop.submit {
    //                workers.first(where: { $0.isWorking == false })
    //            }
    //        }
    //
    //        func recursivelyWork(on:EventLoop) -> EventLoopFuture<Void> {
    //            nextTask().flatMap { task in
    //                on.flatSubmit {
    //                    guard let task = task else { return on.makeSucceededVoidFuture() }
    //                    self.doSomeWork(duration: task)
    //                    return recursivelyWork(on: on)
    //                }
    //            }
    //        }
    //
    //        let workExpectation = expectation(description: "waiting for work")
    //
    //        (0..<workers).compactMap { _ -> EventLoopFuture<Void> in
    //            print("Deploying Worker")
    //            return recursivelyWork(on: group.next())
    //        }.flatten(on: mainLoop).whenComplete { result in
    //            switch result {
    //            case .failure(let error):
    //                print("Error: \(error)")
    //            case .success:
    //                print("All done with work")
    //                print(stuffToDo)
    //            }
    //            workExpectation.fulfill()
    //        }
    //
    //        waitForExpectations(timeout: 120, handler: nil)
    //        print("Shutting down event loop")
    //        try! group.syncShutdownGracefully()
    //    }

}
