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

import LibP2P
import RoutingKit

extension RoutesBuilder {
    // MARK: Path
    
    /// Creates a new `Router` that will automatically prepend the supplied path components.
    ///
    ///     let users = router.grouped("user")
    ///     // Adding "user/auth/" route to router.
    ///     users.get("auth") { ... }
    ///     // adding "user/profile/" route to router
    ///     users.get("profile") { ... }
    ///
    /// - parameters:
    ///     - path: Group path components separated by commas.
    /// - returns: Newly created `Router` wrapped in the path.
    public func namespace(_ path: PathComponent..., validator:some Validator) -> RoutesBuilder {
        return self.grouped(path, handlers: [.validator(validator)])
    }
    
    /// Creates a new `Router` that will automatically prepend the supplied path components.
    ///
    ///     router.group("user") { users in
    ///         // Adding "user/auth/" route to router.
    ///         users.get("auth") { ... }
    ///         // adding "user/profile/" route to router
    ///         users.get("profile") { ... }
    ///     }
    ///
    /// - parameters:
    ///     - path: Group path components separated by commas.
    ///     - configure: Closure to configure the newly created `Router`.
    public func namespace(_ path: PathComponent..., validator:some Validator, configure: (RoutesBuilder) throws -> ()) rethrows {
        return try group(path, handlers: [.validator(validator)], configure: configure)
    }
}

public enum DHTMethod: Equatable, Sendable {
    static var key:String = "LibP2P.DHT.Method"
    internal enum HasBody {
        case yes
        case no
        case unlikely
    }
    
    case GET
    case PUT
}

extension RoutesBuilder {
    @discardableResult
    public func get<Response>(
        _ path: PathComponent...,
        use closure: @escaping (Request) throws -> Response
    ) -> Route
        where Response: ResponseEncodable
    {
        return self.on(.GET, path, use: closure)
    }

    @discardableResult
    public func get<Response>(
        _ path: [PathComponent],
        use closure: @escaping (Request) throws -> Response
    ) -> Route
        where Response: ResponseEncodable
    {
        return self.on(.GET, path, use: closure)
    }
    
    @discardableResult
    public func put<Response>(
        _ path: PathComponent...,
        use closure: @escaping (Request) throws -> Response
    ) -> Route
        where Response: ResponseEncodable
    {
        return self.on(.PUT, path, use: closure)
    }
    
    @discardableResult
    public func put<Response>(
        _ path: [PathComponent],
        use closure: @escaping (Request) throws -> Response
    ) -> Route
        where Response: ResponseEncodable
    {
        return self.on(.PUT, path, use: closure)
    }
    
    
    @discardableResult
    public func on<Response>(
        _ method: DHTMethod,
        _ path: PathComponent...,
        body: PayloadStreamStrategy = .stream,
        handlers: [Application.ChildChannelHandlers.Provider] = [],
        use closure: @escaping (Request) throws -> Response
    ) -> Route
        where Response: ResponseEncodable
    {
        return self.on(method, path, body: body, handlers: handlers, use: { request in
            return try closure(request)
        })
    }
    
    @discardableResult
    public func on<Response>(
        _ method: DHTMethod,
        _ path: [PathComponent],
        body: PayloadStreamStrategy = .stream,
        handlers: [Application.ChildChannelHandlers.Provider] = [],
        use closure: @escaping (Request) throws -> Response
    ) -> Route
        where Response: ResponseEncodable
    {
        let responder = BasicResponder { request in
            return try closure(request)
                .encodeResponse(for: request)
        }
        let route = Route(
            path: path,
            responder: responder,
            handlers: handlers,
            requestType: Request.self,
            responseType: Response.self
            //userInfo: [DHTMethod.key: method]
        )
        self.add(route)
        return route
    }
}


//extension RoutesBuilder {
//    @discardableResult
//    public func onPutRequest<Response>(
//        _ path: PathComponent...,
//        body: PayloadStreamStrategy = .stream,
//        validator: Validator.Type,
//        handlers: [Application.ChildChannelHandlers.Provider] = [],
//        use closure: @escaping (Request) throws -> Response
//    ) -> Route
//        where Response: ResponseEncodable
//    {
//        return self.on(path, body: body, handlers: handlers, use: { request in
//            return try closure(request)
//        })
//    }
//
//    @discardableResult
//    public func onPutRequest<Response>(
//        _ path: [PathComponent],
//        body: PayloadStreamStrategy = .stream,
//        validator: Validator.Type,
//        handlers: [Application.ChildChannelHandlers.Provider] = [],
//        use closure: @escaping (Request) throws -> Response
//    ) -> Route
//        where Response: ResponseEncodable
//    {
//        let responder = BasicResponder { request in
//            return try closure(request)
//                .encodeResponse(for: request)
//        }
//        let route = Route(
//            path: path,
//            responder: responder,
//            handlers: handlers,
//            requestType: Request.self,
//            responseType: Response.self
//        )
//        self.add(route)
//        return route
//    }
//}


public final class DHTRoute: CustomStringConvertible {
    public var method: DHTMethod
    public var path: [PathComponent]
    public var responder: Responder
    public var requestType: Any.Type
    public var responseType: Any.Type
    
    public var userInfo: [AnyHashable: Any]

    public var description: String {
        let path = self.path.map { "\($0)" }.joined(separator: "/")
        return "\(self.method) /\(path)"
    }
    
    public init(
        method: DHTMethod,
        path: [PathComponent],
        responder: Responder,
        handlers: [Application.ChildChannelHandlers.Provider],
        requestType: Any.Type,
        responseType: Any.Type
    ) {
        self.method = method
        self.path = path
        self.responder = responder
        self.requestType = requestType
        self.responseType = responseType
        self.userInfo = [:]
    }
       
    @discardableResult
    public func description(_ string: String) -> DHTRoute {
        self.userInfo["description"] = string
        return self
    }
}
