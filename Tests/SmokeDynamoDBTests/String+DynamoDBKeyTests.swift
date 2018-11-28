// Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").
// You may not use this file except in compliance with the License.
// A copy of the License is located at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.
//
//  String+DynamoDBKeyTests.swift
//  SmokeDynamoDBTests
//

import XCTest
@testable import SmokeDynamoDB

class StringDynamoDBKeyTests: XCTestCase {

    func testDynamoDBKeyTests() {
        XCTAssertEqual([].dynamodbKey, "")
        XCTAssertEqual(["one"].dynamodbKey, "one")
        XCTAssertEqual(["one", "two"].dynamodbKey, "one.two")
        XCTAssertEqual(["one", "two", "three", "four", "five", "six"].dynamodbKey, "one.two.three.four.five.six")
    }
    
    func testDropAsDynamoDBKeyPrefix() {
        XCTAssertEqual(["one", "two"].dropAsDynamoDBKeyPrefix(from: "one.two.three.four.five.six")!,
                       "three.four.five.six")
        XCTAssertEqual([].dropAsDynamoDBKeyPrefix(from: "one.two.three.four.five.six")!,
                       "one.two.three.four.five.six")
        XCTAssertEqual(["four", "two"].dropAsDynamoDBKeyPrefix(from: "one.two.three.four.five.six"), nil)
    }
    
    func testDynamoDBKeyPrefixTests() {
        XCTAssertEqual([].dynamodbKeyPrefix, "")
        XCTAssertEqual(["one"].dynamodbKeyPrefix, "one.")
        XCTAssertEqual(["one", "two"].dynamodbKeyPrefix, "one.two.")
        XCTAssertEqual(["one", "two", "three", "four", "five", "six"].dynamodbKeyPrefix, "one.two.three.four.five.six.")
    }
    
    func testDynamoDBKeyWithPrefixedVersionTests() {
        XCTAssertEqual([].dynamodbKeyWithPrefixedVersion(8, minimumFieldWidth: 5), "v00008")
        XCTAssertEqual(["one"].dynamodbKeyWithPrefixedVersion(8, minimumFieldWidth: 5), "v00008.one")
        XCTAssertEqual(["one", "two"].dynamodbKeyWithPrefixedVersion(8, minimumFieldWidth: 5), "v00008.one.two")
        XCTAssertEqual(["one", "two", "three", "four", "five", "six"].dynamodbKeyWithPrefixedVersion(8, minimumFieldWidth: 5),
                       "v00008.one.two.three.four.five.six")
        
        XCTAssertEqual(["one", "two"].dynamodbKeyWithPrefixedVersion(8, minimumFieldWidth: 2), "v08.one.two")
        XCTAssertEqual(["one", "two"].dynamodbKeyWithPrefixedVersion(4888, minimumFieldWidth: 2), "v4888.one.two")
    }

    static var allTests = [
        ("testDynamoDBKeyTests", testDynamoDBKeyTests),
        ("testDropAsDynamoDBKeyPrefix", testDropAsDynamoDBKeyPrefix),
        ("testDynamoDBKeyPrefixTests", testDynamoDBKeyPrefixTests),
        ("testDynamoDBKeyWithPrefixedVersionTests", testDynamoDBKeyWithPrefixedVersionTests)
    ]
}
