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
//  DynamoDBEncoderDecoderTests.swift
//  SmokeDynamoDBTests
//

import XCTest
@testable import SmokeDynamoDB

private let dynamodbEncoder = DynamoDBEncoder()
private let dynamodbDecoder = DynamoDBDecoder()

struct CoreAccountAttributes: Codable {
    var description: String
    var mappedValues: [String: String]
    var notificationTargets: NotificationTargets
}

extension CoreAccountAttributes: Equatable {
    static func ==(lhs: CoreAccountAttributes, rhs: CoreAccountAttributes) -> Bool {
        return lhs.description == rhs.description && lhs.notificationTargets == rhs.notificationTargets
            && lhs.mappedValues == rhs.mappedValues
    }
}

struct NotificationTargets: Codable {
    var currentIDs: [String]
    var maximum: Int
}

extension NotificationTargets: Equatable {
    static func ==(lhs: NotificationTargets, rhs: NotificationTargets) -> Bool {
        return lhs.currentIDs == rhs.currentIDs && lhs.maximum == rhs.maximum
    }
}

typealias DatabaseItemType = StandardTypedDatabaseItem<CoreAccountAttributes>

class DynamoDBEncoderDecoderTests: XCTestCase {
    
    func testEncoderDecoder() {
        let notificationTargets = NotificationTargets(currentIDs: [], maximum: 20)
        let attributes = CoreAccountAttributes(description: "Description",
                                               mappedValues: ["A": "one", "B": "two"],
                                               notificationTargets: notificationTargets)
        
        // create key and database item to create
        let key = StandardCompositePrimaryKey(partitionKey: "partitionKey", sortKey: "sortKey")
        let newDatabaseItem: DatabaseItemType = StandardTypedDatabaseItem.newItem(withKey: key, andValue: attributes)
        
        let encodedAttributeValue = try! dynamodbEncoder.encode(newDatabaseItem)
        
        let output: DatabaseItemType = try! dynamodbDecoder.decode(encodedAttributeValue)
        
        XCTAssertEqual(newDatabaseItem.rowValue, output.rowValue)
        XCTAssertEqual("partitionKey", output.compositePrimaryKey.partitionKey)
        XCTAssertEqual("sortKey", output.compositePrimaryKey.sortKey)
    }
    
    static var allTests = [
        ("testEncoderDecoder", testEncoderDecoder),
    ]
}
