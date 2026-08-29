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

/// An RFC3339 timestamp, wire-compatible with the `timeReceived` field go writes onto `DHT.Record`.
///
/// go emits `util.FormatRFC3339(t)`, which is `t.UTC().Format(time.RFC3339Nano)`.
///
/// 1. `RFC3339Nano` omits trailing zeros in the fractional-seconds component. The fraction is
///    therefore 0–9 digits wide, or can be completely absent
/// 2. The timezone is always UTC
///
/// - Note: Whole seconds and the fractional part are stored separately rather than folded into a single
/// `Date`. `Date` is a `Double`, so around the present epoch it can only resolve a few hundred
/// nanoseconds — not enough to order two records emitted in the same microsecond. Comparison uses
/// the exact `(wholeSeconds, nanoseconds)` pair instead.
struct RFC3339Date: Equatable, Comparable {
    /// Whole-second portion of the layout. The fraction and zone are handled separately.
    private static let strFormat: String = "yyyy-MM-dd'T'HH:mm:ss"
    private static let locale: Locale = Locale(identifier: "en_US_POSIX")
    private static let formatter: DateFormatter = {
        let f = DateFormatter()
        f.locale = RFC3339Date.locale
        f.dateFormat = RFC3339Date.strFormat
        /// Without this, a trailing `Z` is interpreted as local time.
        f.timeZone = TimeZone(secondsFromGMT: 0)
        return f
    }()

    private static let nanosecondsPerSecond: Int = 1_000_000_000

    /// The original RFC3339 string that was parsed, if any.
    ///
    /// Retained so `string` round-trips byte-for-byte.
    private let originalString: String?

    /// The instant truncated to whole seconds.
    private let wholeSeconds: Date

    /// The fractional part, in nanoseconds. Always `0..<1_000_000_000`.
    private let nanos: Int

    /// The full instant, fraction included.
    ///
    /// - Warning: Lossy above ~microsecond resolution.
    ///   Use `==` / `<` for ordering rather than comparing `date` values.
    public var date: Date {
        self.wholeSeconds.addingTimeInterval(Double(self.nanos) / Double(Self.nanosecondsPerSecond))
    }

    public var nanoseconds: Int? {
        self.nanos
    }

    public var string: String {
        self.originalString ?? self.toString()
    }

    public init(string: String) throws {
        let (wholeSeconds, nanos) = try Self.parse(string)
        self.originalString = string
        self.wholeSeconds = wholeSeconds
        self.nanos = nanos
    }

    public init() {
        self.init(date: Date())
    }

    public init(date: Date) {
        self.originalString = nil
        /// Split the instant into whole seconds and a nanosecond remainder.
        let interval = date.timeIntervalSince1970
        let whole = interval.rounded(.down)
        self.wholeSeconds = Date(timeIntervalSince1970: whole)
        let fraction = interval - whole
        /// keeps the remainder non-negative for pre-1970 dates.
        self.nanos = min(
            Self.nanosecondsPerSecond - 1,
            max(0, Int((fraction * Double(Self.nanosecondsPerSecond)).rounded(.down)))
        )
    }

    /// Parses `yyyy-MM-ddTHH:mm:ss[.fraction](Z|±hh:mm)`.
    ///
    /// The fraction is optional and of any width; more than 9 digits is truncated to nanoseconds
    /// rather than rejected, matching Go's `time.Parse`, which accepts an arbitrary-precision
    /// fraction and discards sub-nanosecond digits.
    private static func parse(_ string: String) throws -> (whole: Date, nanos: Int) {
        /// Strip the zone designator first so it can't be confused with the fraction.
        let (withoutZone, offsetSeconds) = try Self.splitZone(from: Substring(string), of: string)
        let (base, nanos) = try Self.splitFraction(from: withoutZone, of: string)

        guard let whole = Self.formatter.date(from: String(base)) else {
            throw Errors.invalidDateString(string)
        }

        /// Rebase onto UTC.
        return (whole.addingTimeInterval(-Double(offsetSeconds)), nanos)
    }

    /// Removes the trailing zone designator (`Z` or `±hh:mm`), returning the remaining body and the
    /// zone's offset from UTC in seconds.
    ///
    /// - Parameter string: The full input, used only to build the error.
    private static func splitZone(
        from body: Substring,
        of string: String
    ) throws -> (body: Substring, offsetSeconds: Int) {
        if let last = body.last, last == "Z" || last == "z" {
            return (body.dropLast(), 0)
        }

        /// A `-` inside the date portion isn't a zone sign; a zone is always the trailing
        /// `±hh:mm`, i.e. exactly 6 characters.
        guard let signIndex = body.lastIndex(where: { $0 == "+" || $0 == "-" }),
            body.distance(from: signIndex, to: body.endIndex) == 6,
            let magnitude = Self.zoneMagnitudeSeconds(body[body.index(after: signIndex)...])
        else {
            /// RFC3339 requires an offset; a naked local timestamp is ambiguous.
            throw Errors.invalidDateString(string)
        }

        let offsetSeconds = body[signIndex] == "-" ? -magnitude : magnitude
        return (body[..<signIndex], offsetSeconds)
    }

    /// Interprets an unsigned `hh:mm` zone body as a count of seconds, or `nil` if it isn't well formed.
    private static func zoneMagnitudeSeconds(_ zone: Substring) -> Int? {
        let parts = zone.split(separator: ":", omittingEmptySubsequences: false)
        guard parts.count == 2, parts[0].count == 2, parts[1].count == 2,
            let hours = Int(parts[0]), let minutes = Int(parts[1]),
            hours < 24, minutes < 60
        else {
            return nil
        }
        return hours * 3600 + minutes * 60
    }

    /// Separates the optional fractional-seconds component from a zone-stripped body, returning the
    /// whole-seconds portion and the fraction in nanoseconds.
    ///
    /// - Parameter string: The full input, used only to build the error.
    private static func splitFraction(
        from body: Substring,
        of string: String
    ) throws -> (base: Substring, nanos: Int) {
        guard let dot = body.firstIndex(of: ".") else { return (body, 0) }

        let digits = body[body.index(after: dot)...]
        guard !digits.isEmpty, digits.allSatisfy({ $0.isASCII && $0.isNumber }) else {
            throw Errors.invalidDateString(string)
        }
        /// Right-pad to 9 so "5" means 500ms, and truncate anything finer than a nanosecond.
        let padded =
            digits.count >= 9
            ? String(digits.prefix(9))
            : String(digits) + String(repeating: "0", count: 9 - digits.count)
        guard let nanos = Int(padded) else { throw Errors.invalidDateString(string) }

        return (body[..<dot], nanos)
    }

    /// Formats as `RFC3339Nano` does: trailing zeros trimmed from the fraction, and the `.` dropped
    /// entirely when the fraction is zero.
    private func toString() -> String {
        let base = Self.formatter.string(from: self.wholeSeconds)
        guard self.nanos > 0 else { return "\(base)Z" }

        var fraction = String(format: "%09d", self.nanos)
        while fraction.hasSuffix("0") { fraction.removeLast() }
        return "\(base).\(fraction)Z"
    }

    enum Errors: Error, CustomStringConvertible {
        case invalidDateString(String)

        var description: String {
            switch self {
            case .invalidDateString(let string): return "Invalid RFC3339 date string '\(string)'"
            }
        }
    }

    static func == (lhs: RFC3339Date, rhs: RFC3339Date) -> Bool {
        lhs.wholeSeconds == rhs.wholeSeconds && lhs.nanos == rhs.nanos
    }

    static func < (lhs: RFC3339Date, rhs: RFC3339Date) -> Bool {
        guard lhs.wholeSeconds == rhs.wholeSeconds else {
            return lhs.wholeSeconds < rhs.wholeSeconds
        }
        return lhs.nanos < rhs.nanos
    }
}
