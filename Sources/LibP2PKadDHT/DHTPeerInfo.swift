//
//  DHTPeerInfo.swift
//
//
//  Created by Brandon Toms on 9/9/22.
//

import LibP2PCore

struct DHTPeerInfo: Equatable {
    /// The original PeerID of this DHT Peer
    let id: PeerID

    /// LastUsefulAt is the time instant at which the peer was last "useful" to us.
    ///
    /// Please see the DHT docs for the definition of usefulness.
    var lastUsefulAt: TimeInterval?

    /// LastSuccessfulOutboundQueryAt is the time instant at which we last got a successful query response from the peer.
    var lastSuccessfulOutboundQueryAt: TimeInterval?

    /// AddedAt is the time this peer was added to the routing table.
    let addedAt: TimeInterval

    /// The ID of the peer in the DHT XOR keyspace (XOR(PeerID))
    let dhtID: KadDHT.Key

    /// If a bucket is full, this peer can be replaced to make space for a new peer.
    var replaceable: Bool
}

extension Array where Element == DHTPeerInfo {
    func sortedAbsolutely(using keyspace: KadDHT.Key.KeySpace = .xor) -> [DHTPeerInfo] {
        let targetKey = KadDHT.Key(preHashedBytes: Array<UInt8>(repeating: 0, count: 32))
        return self.sorted { lhs, rhs in
            let comp = targetKey.compareDistancesFromSelf(to: lhs.dhtID, and: rhs.dhtID)
            return comp == .firstKey
        }
    }
}
