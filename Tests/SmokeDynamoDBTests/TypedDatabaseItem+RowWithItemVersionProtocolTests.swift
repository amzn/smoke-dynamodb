// Copyright 2018-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
//  TypedDatabaseItem+RowWithItemVersionProtocolTests.swift
//  SmokeDynamoDBTests
//

import Foundation

import XCTest
@testable import SmokeDynamoDB

private let ORIGINAL_PAYLOAD = "Payload"
private let UPDATED_PAYLOAD = "Updated"

class TypedDatabaseItemRowWithItemVersionProtocolTests: XCTestCase {

    func testCreateUpdatedRowWithItemVersion() throws {
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: rowWithItemVersion)
        
        let updatedItem = try databaseItem.createUpdatedRowWithItemVersion(withValue: "Updated",
                                                                            conditionalStatusVersion: nil)
        
        XCTAssertEqual(1, databaseItem.rowStatus.rowVersion)
        XCTAssertEqual(1, databaseItem.rowValue.itemVersion)
        XCTAssertEqual(ORIGINAL_PAYLOAD, databaseItem.rowValue.rowValue)
        XCTAssertEqual(2, updatedItem.rowStatus.rowVersion)
        XCTAssertEqual(2, updatedItem.rowValue.itemVersion)
        XCTAssertEqual(UPDATED_PAYLOAD, updatedItem.rowValue.rowValue)
    }
    
    func testCreateUpdatedRowWithItemVersionWithCorrectConditionalVersion() throws {
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: rowWithItemVersion)
        
        let updatedItem = try databaseItem.createUpdatedRowWithItemVersion(withValue: "Updated",
                                                                            conditionalStatusVersion: 1)
        
        XCTAssertEqual(1, databaseItem.rowStatus.rowVersion)
        XCTAssertEqual(1, databaseItem.rowValue.itemVersion)
        XCTAssertEqual(ORIGINAL_PAYLOAD, databaseItem.rowValue.rowValue)
        XCTAssertEqual(2, updatedItem.rowStatus.rowVersion)
        XCTAssertEqual(2, updatedItem.rowValue.itemVersion)
        XCTAssertEqual(UPDATED_PAYLOAD, updatedItem.rowValue.rowValue)
    }
    
    func testCreateUpdatedRowWithItemVersionWithIncorrectConditionalVersion() {
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: rowWithItemVersion)
        
        do {
            _ = try databaseItem.createUpdatedRowWithItemVersion(withValue: "Updated",
                                                                 conditionalStatusVersion: 8)
            
            XCTFail("Expected error not thrown.")
        } catch SmokeDynamoDBError.concurrencyError {
            return
        } catch {
            XCTFail("Unexpected error thrown: '\(error)'.")
        }
    }

    static var allTests = [
        ("testCreateUpdatedRowWithItemVersion", testCreateUpdatedRowWithItemVersion),
        ("testCreateUpdatedRowWithItemVersionWithCorrectConditionalVersion",
         testCreateUpdatedRowWithItemVersionWithCorrectConditionalVersion),
        ("testCreateUpdatedRowWithItemVersionWithIncorrectConditionalVersion",
         testCreateUpdatedRowWithItemVersionWithIncorrectConditionalVersion)
    ]
}
