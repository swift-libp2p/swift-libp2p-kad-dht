//
//  Records+Ext.swift
//
//
//  Created by Brandon Toms on 7/24/23.
//

import LibP2P

extension DHT.Record: DHTRecord { }

extension DHTRecord {
    func toProtobuf() -> DHT.Record {
        guard self as? DHT.Record == nil else { return self as! DHT.Record }
        return DHT.Record.with { rec in
            rec.key = self.key
            rec.value = self.value
            rec.author = self.author
            rec.signature = self.signature
            rec.timeReceived = self.timeReceived
        }
    }
}

extension DHT.Message.Peer: CustomStringConvertible {
    var description: String {
        if let pid = try? PeerID(fromBytesID: self.id.bytes) {
            return """
            \(pid) (\(self.connectionToString(self.connection.rawValue))) [
                \(self.addrs.map { addyBytes -> String in
                    if let ma = try? Multiaddr(addyBytes) {
                        return ma.description
                    } else {
                        return "Invalid Multiaddr"
                    }
                }.joined(separator: "\n"))
            ]
            """
        } else {
            return "Invalid DHT.Message.Peer"
        }
    }

    func toPeerInfo() throws -> PeerInfo {
        return PeerInfo(
            peer: try PeerID(fromBytesID: self.id.bytes),
            addresses: try self.addrs.map {
                try Multiaddr($0)
            }
        )
    }

    init(_ peer: PeerInfo, connection: DHT.Message.ConnectionType = .canConnect) throws {
        self.id = Data(peer.peer.id)
        self.addrs = try peer.addresses.map {
            try $0.binaryPacked()
        }
        self.connection = connection
    }

    private func connectionToString(_ type: Int) -> String {
        switch type {
            case 0: return "Not Connected"
            case 1: return "Connected"
            case 2: return "Can Connect"
            case 3: return "Cannot Connect"
            default: return "Invalid Connection Type"
        }
    }
}

extension DHT.Record: CustomStringConvertible {
    public var description: String {
        let header = "--- 📒 DHT Record 📒 ---"
        return """
        \n
        \(header)
        Key: \(self.key.asString(base: .base16))
        Value: \(self.value.asString(base: .base16))
        Time Received: \(self.timeReceived)
        \(String(repeating: "-", count: header.count + 2))
        """
    }
}

extension IpnsEntry: CustomStringConvertible {
    public var description: String {
        let header = "--- 🌎 IPNS Record 🌎 ---"
        return """
        \n
        \(header)
        Bytes: \(self.value.asString(base: .base16))
        Signature<V1>: \(self.signatureV1.asString(base: .base16))
        Validity<EOL>: \(self.validity.asString(base: .base16))
        Sequence: \(self.sequence)
        TTL: \(self.ttl)
        \(String(repeating: "-", count: header.count + 2))
        """
    }
}
