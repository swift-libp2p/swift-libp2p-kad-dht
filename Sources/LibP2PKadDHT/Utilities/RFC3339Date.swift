//
//  RFC3339Date.swift
//  
//
//  Created by Brandon Toms on 9/30/22.
//

import Foundation

struct RFC3339Date:Equatable, Comparable {
    private static var strFormat:String = "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"
    private static var locale:Locale = Locale(identifier: "en_US_POSIX")
    
    /// The original RFC3339 String that was parsed if available
    private var originalString:String?
    public private(set) var date:Date
    public var string:String {
        originalString ?? formatter.string(from: date)
    }
    
    /// The Stored Date Formatter
    private let formatter:DateFormatter
    
    public init(string: String) throws {
        self.formatter = DateFormatter()
        self.formatter.locale = RFC3339Date.locale
        self.formatter.dateFormat = RFC3339Date.strFormat
        
        guard let date = self.formatter.date(from: string) else {
            throw NSError(domain: "Invalid RFC3339 Date String", code: 0)
        }
        
        let nanoString = string[string.lastIndex(of: ".")!...].dropFirst().dropLast()
        guard nanoString.count == 9 else { throw NSError(domain: "Invalid RFC3339 Date String", code: 0) }
        
        self.originalString = string
        self.date = date
    }
    
    public init() {
        self.formatter = DateFormatter()
        self.formatter.locale = RFC3339Date.locale
        self.formatter.dateFormat = RFC3339Date.strFormat
        
        self.originalString = nil
        self.date = Date()
    }
    
    public init(date: Date) {
        self.formatter = DateFormatter()
        self.formatter.locale = RFC3339Date.locale
        self.formatter.dateFormat = RFC3339Date.strFormat
        
        self.originalString = nil
        self.date = Date()
    }
    
    static func == (lhs:RFC3339Date, rhs:RFC3339Date) -> Bool {
        switch (lhs.originalString, rhs.originalString) {
        case (.some(let ogL), .some(let ogR)):
            return ogL == ogR
        default:
            return lhs.date == rhs.date
        }
    }
    
    static func < (lhs: RFC3339Date, rhs: RFC3339Date) -> Bool {
        switch (lhs.originalString, rhs.originalString) {
        case (.some(let ogL), .some(let ogR)):
            guard lhs.date == rhs.date else {
                return lhs.date < rhs.date
            }
            // Parse out the nanoseconds from the original strings and compare them...
            let nanoLeftString = ogL[ogL.lastIndex(of: ".")!...].dropFirst().dropLast()
            let nanoLeft = UInt64(nanoLeftString)!
            
            let nanoRightString = ogR[ogR.lastIndex(of: ".")!...].dropFirst().dropLast()
            let nanoRight = UInt64(nanoRightString)!
            
            return nanoLeft < nanoRight
        default:
            return lhs.date < rhs.date
        }
    }
}
