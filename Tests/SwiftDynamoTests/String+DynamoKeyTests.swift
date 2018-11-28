//
//  String+DynamoKeyTests.swift
//  SwiftDynamo
//

import XCTest
@testable import SwiftDynamo

class StringDynamoKeyTests: XCTestCase {

    func testDynamoKeyTests() {
        XCTAssertEqual([].dynamoKey, "")
        XCTAssertEqual(["one"].dynamoKey, "one")
        XCTAssertEqual(["one", "two"].dynamoKey, "one.two")
        XCTAssertEqual(["one", "two", "three", "four", "five", "six"].dynamoKey, "one.two.three.four.five.six")
    }
    
    func testDropAsDynamoKeyPrefix() {
        XCTAssertEqual(["one", "two"].dropAsDynamoKeyPrefix(from: "one.two.three.four.five.six")!,
                       "three.four.five.six")
        XCTAssertEqual([].dropAsDynamoKeyPrefix(from: "one.two.three.four.five.six")!,
                       "one.two.three.four.five.six")
        XCTAssertEqual(["four", "two"].dropAsDynamoKeyPrefix(from: "one.two.three.four.five.six"), nil)
    }
    
    func testDynamoKeyPrefixTests() {
        XCTAssertEqual([].dynamoKeyPrefix, "")
        XCTAssertEqual(["one"].dynamoKeyPrefix, "one.")
        XCTAssertEqual(["one", "two"].dynamoKeyPrefix, "one.two.")
        XCTAssertEqual(["one", "two", "three", "four", "five", "six"].dynamoKeyPrefix, "one.two.three.four.five.six.")
    }
    
    func testDynamoKeyWithPrefixedVersionTests() {
        XCTAssertEqual([].dynamoKeyWithPrefixedVersion(8, minimumFieldWidth: 5), "v00008")
        XCTAssertEqual(["one"].dynamoKeyWithPrefixedVersion(8, minimumFieldWidth: 5), "v00008.one")
        XCTAssertEqual(["one", "two"].dynamoKeyWithPrefixedVersion(8, minimumFieldWidth: 5), "v00008.one.two")
        XCTAssertEqual(["one", "two", "three", "four", "five", "six"].dynamoKeyWithPrefixedVersion(8, minimumFieldWidth: 5),
                       "v00008.one.two.three.four.five.six")
        
        XCTAssertEqual(["one", "two"].dynamoKeyWithPrefixedVersion(8, minimumFieldWidth: 2), "v08.one.two")
        XCTAssertEqual(["one", "two"].dynamoKeyWithPrefixedVersion(4888, minimumFieldWidth: 2), "v4888.one.two")
    }

    static var allTests = [
        ("testDynamoKeyTests", testDynamoKeyTests),
        ("testDropAsDynamoKeyPrefix", testDropAsDynamoKeyPrefix),
        ("testDynamoKeyPrefixTests", testDynamoKeyPrefixTests),
        ("testDynamoKeyWithPrefixedVersionTests", testDynamoKeyWithPrefixedVersionTests)
    ]
}
