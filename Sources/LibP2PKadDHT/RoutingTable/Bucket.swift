//
//  Bucket.swift
//  
//
//  Created by Brandon Toms on 4/29/22.
//

import LibP2P

typealias Bucket = Array<DHTPeerInfo>

extension Bucket {
    
    /// returns all peers in the bucket
    func peers() -> [DHTPeerInfo] {
        self
    }
        
    mutating func updateAllWith(_ updateFunc:(inout DHTPeerInfo) -> Void) {
        for i in 0..<self.count {
            updateFunc(&self[i])
        }
    }
    
    func peerIDs() -> [PeerID] {
        self.map { $0.id }
    }
    
    func getPeer(_ id:PeerID) -> DHTPeerInfo? {
        self.first(where: { $0.id == id })
    }
    
    @discardableResult
    mutating func getPeer(_ id:PeerID, modifier:(inout DHTPeerInfo) -> Void) -> Bool {
        guard let match = self.firstIndex(where: { $0.id == id }) else { return false }
        modifier(&self[match])
        return true
    }
    
    func getPeer(_ id:DHTPeerInfo) -> DHTPeerInfo? {
        self.first(where: { $0.dhtID == id.dhtID })
    }
    
    @discardableResult
    mutating func getPeer(_ id:DHTPeerInfo, modifier:(inout DHTPeerInfo) -> Void) -> Bool {
        guard let match = self.firstIndex(where: { $0.dhtID == id.dhtID }) else { return false }
        modifier(&self[match])
        return true
    }
    
    /// Replaces a `DHTPeerInfo` with another modified version of the `DHTPeerInfo`
    /// - Note: The two `DHTPeerInfo`s must have the same Key, otherwise calling this method is a no op
    private mutating func replace(_ element:Element, at idx:Int) {
        guard self[idx].dhtID == element.dhtID else { return }
        self.remove(at: idx)
        self.insert(element, at: idx)
    }
    
    mutating func remove(_ id:PeerID) -> Bool {
        var didRemove:Bool = false
        self.removeAll(where: {
            if $0.id == id {
                didRemove = true
                return true
            }
            return false
        })
        return didRemove
    }
    
    mutating func pushFront(_ peer:DHTPeerInfo) {
        self.insert(peer, at: 0)
    }
    
    /// splits a buckets peers into two buckets, the methods receiver will have
    /// peers with CPL equal to cpl, the returned bucket will have peers with CPL
    /// greater than cpl (returned bucket has closer peers)
    mutating func split(commonPrefixLength:Int, targetID:ID) -> Bucket {
        var newBucket:Bucket = []
        self.removeAll { dhtPeer in
            if dhtPeer.dhtID.bytes.commonPrefixLength(with: targetID) > commonPrefixLength {
                newBucket.append(dhtPeer)
                return true
            }
            return false
        }
        return newBucket
    }
    
    func maxCommonPrefixLength(target:ID) -> Int {
        self.max { lhs, rhs in
            lhs.dhtID.bytes.commonPrefixLength(with: target) > rhs.dhtID.bytes.commonPrefixLength(with: target)
        }!.dhtID.bytes.commonPrefixLength(with: target)
    }
    
}
