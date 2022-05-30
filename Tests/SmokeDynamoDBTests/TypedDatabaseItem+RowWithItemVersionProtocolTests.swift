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
private let ORIGINAL_TIME_TO_LIVE: Int64 = 123456789
private let UPDATED_PAYLOAD = "Updated"
private let UPDATED_TIME_TO_LIVE: Int64 = 234567890

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
        XCTAssertNil(databaseItem.timeToLive)
        XCTAssertEqual(2, updatedItem.rowStatus.rowVersion)
        XCTAssertEqual(2, updatedItem.rowValue.itemVersion)
        XCTAssertEqual(UPDATED_PAYLOAD, updatedItem.rowValue.rowValue)
        XCTAssertNil(updatedItem.timeToLive)
    }
    
    func testCreateUpdatedRowWithItemVersionWithTimeToLive() throws {
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                       sortKey: "sortKey")
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = TypedDatabaseItem.newItem(withKey: compositeKey,
                                                     andValue: rowWithItemVersion,
                                                     andTimeToLive: StandardTimeToLive(timeToLiveTimestamp: 123456789))
        
        let updatedItem = try databaseItem.createUpdatedRowWithItemVersion(withValue: "Updated",
                                                                           conditionalStatusVersion: nil,
                                                                           andTimeToLive: StandardTimeToLive(timeToLiveTimestamp: 234567890))
        
        XCTAssertEqual(1, databaseItem.rowStatus.rowVersion)
        XCTAssertEqual(1, databaseItem.rowValue.itemVersion)
        XCTAssertEqual(ORIGINAL_PAYLOAD, databaseItem.rowValue.rowValue)
        XCTAssertEqual(ORIGINAL_TIME_TO_LIVE, databaseItem.timeToLive?.timeToLiveTimestamp)
        XCTAssertEqual(2, updatedItem.rowStatus.rowVersion)
        XCTAssertEqual(2, updatedItem.rowValue.itemVersion)
        XCTAssertEqual(UPDATED_PAYLOAD, updatedItem.rowValue.rowValue)
        XCTAssertEqual(UPDATED_TIME_TO_LIVE, updatedItem.timeToLive?.timeToLiveTimestamp)
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
    
    func testStringFieldDifference() throws {
        let theStruct = TestTypeA(firstly: "firstly's", secondly: "secondly's")
        let payloadA = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        let payloadB = TestTypeC(theString: "eigthly's", theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey's",
                                                       sortKey: "sortKey's")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        for escapeSingleQuote in [true, false] {
            let differences = try table.diffItems(newItem: databaseItemB,
                                                  existingItem: databaseItemA,
                                                  escapeSingleQuote: escapeSingleQuote)
            let pathMap = differences.pathMap

            if escapeSingleQuote {
                XCTAssertEqual(pathMap["theString"], .update(path: "theString", value: "'eigthly''s'"))
            } else {
                XCTAssertEqual(pathMap["theString"], .update(path: "theString", value: "'eigthly's'"))
            }
            XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
        }
    }
    
    func testNumberFieldDifference() throws {
        let theStruct = TestTypeA(firstly: "firstly's", secondly: "secondly's")
        let payloadA = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        let payloadB = TestTypeC(theString: "firstly's", theNumber: 12, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        for escapeSingleQuote in [true, false] {
            let differences = try table.diffItems(newItem: databaseItemB,
                                                  existingItem: databaseItemA,
                                                  escapeSingleQuote: escapeSingleQuote)
            let pathMap = differences.pathMap

            XCTAssertEqual(pathMap["theNumber"], .update(path: "theNumber", value: "12"))
            XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
        }
    }
    
    func testStructFieldDifference() throws {
        let theStructA = TestTypeA(firstly: "firstly's", secondly: "secondly's")
        let theStructB = TestTypeA(firstly: "eigthly's", secondly: "secondly's")
        let payloadA = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStructA, theList: ["thirdly's", "fourthly's"])
        let payloadB = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStructB, theList: ["thirdly's", "fourthly's"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey's",
                                                       sortKey: "sortKey's")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        for escapeSingleQuote in [true, false] {
            let differences = try table.diffItems(newItem: databaseItemB,
                                                  existingItem: databaseItemA,
                                                  escapeSingleQuote: escapeSingleQuote)
            let pathMap = differences.pathMap

            if escapeSingleQuote {
                XCTAssertEqual(pathMap["theStruct.firstly"], .update(path: "theStruct.firstly", value: "'eigthly''s'"))
            } else {
                XCTAssertEqual(pathMap["theStruct.firstly"], .update(path: "theStruct.firstly", value: "'eigthly's'"))
            }
            XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
        }
    }
    
    func testListFieldDifference() throws {
        let theStruct = TestTypeA(firstly: "firstly's", secondly: "secondly's")
        let payloadA = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        let payloadB = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "eigthly's", "ninthly's", "tenthly's"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey's",
                                                       sortKey: "sortKey's")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        for escapeSingleQuote in [true, false] {
            let differences = try table.diffItems(newItem: databaseItemB,
                                                  existingItem: databaseItemA,
                                                  escapeSingleQuote: escapeSingleQuote)
            let pathMap = differences.pathMap

            if escapeSingleQuote {
                XCTAssertEqual(pathMap["theList[1]"], .update(path: "theList[1]", value: "'eigthly''s'"))
                XCTAssertEqual(pathMap["theList"], .listAppend(path: "theList", value: "['ninthly''s', 'tenthly''s']"))
            } else {
                XCTAssertEqual(pathMap["theList[1]"], .update(path: "theList[1]", value: "'eigthly's'"))
                XCTAssertEqual(pathMap["theList"], .listAppend(path: "theList", value: "['ninthly's', 'tenthly's']"))
            }
            XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
        }
    }
    
    func testStringFieldAddition() throws {
        let theStruct = TestTypeA(firstly: "firstly's", secondly: "secondly's")
        let payloadA = TestTypeC(theString: nil, theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        let payloadB = TestTypeC(theString: "eigthly's", theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey's",
                                                       sortKey: "sortKey's")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        for escapeSingleQuote in [true, false] {
            let differences = try table.diffItems(newItem: databaseItemB,
                                                  existingItem: databaseItemA,
                                                  escapeSingleQuote: escapeSingleQuote)
            let pathMap = differences.pathMap

            if escapeSingleQuote {
                XCTAssertEqual(pathMap["theString"], .update(path: "theString", value: "'eigthly''s'"))
            } else {
                XCTAssertEqual(pathMap["theString"], .update(path: "theString", value: "'eigthly's'"))
            }
            XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
        }
    }
    
    func testNumberFieldAddition() throws {
        let theStruct = TestTypeA(firstly: "firstly's", secondly: "secondly's")
        let payloadA = TestTypeC(theString: "firstly's", theNumber: nil, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        let payloadB = TestTypeC(theString: "firstly's", theNumber: 12, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        for escapeSingleQuote in [true, false] {
            let differences = try table.diffItems(newItem: databaseItemB,
                                                  existingItem: databaseItemA,
                                                  escapeSingleQuote: escapeSingleQuote)
            let pathMap = differences.pathMap

            XCTAssertEqual(pathMap["theNumber"], .update(path: "theNumber", value: "12"))
            XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
        }
    }
    
    func testStructFieldAddition() throws {
        let theStruct = TestTypeA(firstly: "firstly's", secondly: "secondly's")
        let payloadA = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: nil, theList: ["thirdly's", "fourthly's"])
        let payloadB = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey's",
                                                       sortKey: "sortKey's")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        for escapeSingleQuote in [true, false] {
            let differences = try table.diffItems(newItem: databaseItemB,
                                                  existingItem: databaseItemA,
                                                  escapeSingleQuote: escapeSingleQuote)
            let pathMap = differences.pathMap

            guard case .update(_, let value) = pathMap["theStruct"] else {
                XCTFail()
                return
            }

            if escapeSingleQuote {
                XCTAssertTrue((value == "{'firstly': 'firstly''s', 'secondly': 'secondly''s'}") ||
                              (value == "{'secondly': 'secondly''s', 'firstly': 'firstly''s'}"))
            } else {
                XCTAssertTrue((value == "{'firstly': 'firstly's', 'secondly': 'secondly's'}") ||
                              (value == "{'secondly': 'secondly's', 'firstly': 'firstly's'}"))
            }

            XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
        }
    }
    
    func testListFieldAddition() throws {
        let theStruct = TestTypeA(firstly: "firstly's", secondly: "secondly's")
        let payloadA = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: nil)
        let payloadB = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "eigthly's", "ninthly's", "tenthly's"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey's",
                                                       sortKey: "sortKey's")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        for escapeSingleQuote in [true, false] {
            let differences = try table.diffItems(newItem: databaseItemB,
                                                  existingItem: databaseItemA,
                                                  escapeSingleQuote: escapeSingleQuote)
            let pathMap = differences.pathMap

            XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
            if escapeSingleQuote {
                XCTAssertEqual(pathMap["theList"], .update(path: "theList", value: "['thirdly''s', 'eigthly''s', 'ninthly''s', 'tenthly''s']"))
            } else {
                XCTAssertEqual(pathMap["theList"], .update(path: "theList", value: "['thirdly's', 'eigthly's', 'ninthly's', 'tenthly's']"))
            }
        }
    }
    
    func testStringFieldRemoval() throws {
        let theStruct = TestTypeA(firstly: "firstly's", secondly: "secondly's")
        let payloadA = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        let payloadB = TestTypeC(theString: nil, theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey's",
                                                       sortKey: "sortKey's")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        for escapeSingleQuote in [true, false] {
            let differences = try table.diffItems(newItem: databaseItemB,
                                                  existingItem: databaseItemA,
                                                  escapeSingleQuote: escapeSingleQuote)
            let pathMap = differences.pathMap

            XCTAssertEqual(pathMap["theString"], .remove(path: "theString"))
            XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
        }
    }
    
    func testNumberFieldRemoval() throws {
        let theStruct = TestTypeA(firstly: "firstly's", secondly: "secondly's")
        let payloadA = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        let payloadB = TestTypeC(theString: "firstly's", theNumber: nil, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey's",
                                                       sortKey: "sortKey's")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        for escapeSingleQuote in [true, false] {
            let differences = try table.diffItems(newItem: databaseItemB,
                                                  existingItem: databaseItemA,
                                                  escapeSingleQuote: escapeSingleQuote)
            let pathMap = differences.pathMap

            XCTAssertEqual(pathMap["theNumber"], .remove(path: "theNumber"))
            XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
        }
    }
    
    func testStructFieldRemoval() throws {
        let theStruct = TestTypeA(firstly: "firstly's", secondly: "secondly's")
        let payloadA = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        let payloadB = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: nil, theList: ["thirdly's", "fourthly's"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey's",
                                                       sortKey: "sortKey's")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        for escapeSingleQuote in [true, false] {
            let differences = try table.diffItems(newItem: databaseItemB,
                                                  existingItem: databaseItemA,
                                                  escapeSingleQuote: escapeSingleQuote)
            let pathMap = differences.pathMap

            XCTAssertEqual(pathMap["theStruct"], .remove(path: "theStruct"))
            XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
        }
    }
    
    func testListFieldRemoval() throws {
        let theStruct = TestTypeA(firstly: "firstly's", secondly: "secondly's")
        let payloadA = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        let payloadB = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: nil)
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey's",
                                                       sortKey: "sortKey's")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        for escapeSingleQuote in [true, false] {
            let differences = try table.diffItems(newItem: databaseItemB,
                                                  existingItem: databaseItemA,
                                                  escapeSingleQuote: escapeSingleQuote)
            let pathMap = differences.pathMap

            XCTAssertEqual(pathMap["theList"], .remove(path: "theList"))
            XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
        }
    }
    
    func testListFieldDifferenceExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly's", secondly: "secondly's")
        let payloadA = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        let payloadB = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "eigthly's", "ninthly's", "tenthly's"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey's",
                                                       sortKey: "sortKey's")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()
        
        let expression = try table.getUpdateExpression(tableName: tableName,
                                                       newItem: databaseItemB,
                                                       existingItem: databaseItemA,
                                                       escapeSingleQuote: false)
        XCTAssertEqual(expression, "UPDATE \"TableName\" "
                                 + "SET \"theList[1]\"='eigthly's' "
                                 + "SET \"theList\"=list_append(theList,['ninthly's', 'tenthly's']) "
                                 + "WHERE PK='partitionKey's' AND SK='sortKey's' "
                                 + "AND RowVersion=1")

        let escapedExpression = try table.getUpdateExpression(tableName: tableName,
                                                              newItem: databaseItemB,
                                                              existingItem: databaseItemA,
                                                              escapeSingleQuote: true)
        XCTAssertEqual(escapedExpression, "UPDATE \"TableName\" "
                                        + "SET \"theList[1]\"='eigthly''s' "
                                        + "SET \"theList\"=list_append(theList,['ninthly''s', 'tenthly''s']) "
                                        + "WHERE PK='partitionKey''s' AND SK='sortKey''s' "
                                        + "AND RowVersion=1")
    }
    
    func testListFieldAdditionExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly's", secondly: "secondly's")
        let payloadA = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: nil)
        let payloadB = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "eigthly's", "ninthly's", "tenthly's"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey's",
                                                       sortKey: "sortKey's")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()
        
        let expression = try table.getUpdateExpression(tableName: tableName,
                                                       newItem: databaseItemB,
                                                       existingItem: databaseItemA,
                                                       escapeSingleQuote: false)
        XCTAssertEqual(expression, "UPDATE \"TableName\" "
                                 + "SET \"theList\"=['thirdly's', 'eigthly's', 'ninthly's', 'tenthly's'] "
                                 + "WHERE PK='partitionKey's' AND SK='sortKey's' "
                                 + "AND RowVersion=1")

        let escapedExpression = try table.getUpdateExpression(tableName: tableName,
                                                              newItem: databaseItemB,
                                                              existingItem: databaseItemA,
                                                              escapeSingleQuote: true)
        XCTAssertEqual(escapedExpression, "UPDATE \"TableName\" "
                                        + "SET \"theList\"=['thirdly''s', 'eigthly''s', 'ninthly''s', 'tenthly''s'] "
                                        + "WHERE PK='partitionKey''s' AND SK='sortKey''s' "
                                        + "AND RowVersion=1")
    }
    
    func testListFieldRemovalExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly's", secondly: "secondly's")
        let payloadA = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        let payloadB = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: nil)
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey's",
                                                       sortKey: "sortKey's")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()
        
        let expression = try table.getUpdateExpression(tableName: tableName,
                                                       newItem: databaseItemB,
                                                       existingItem: databaseItemA,
                                                       escapeSingleQuote: false)
        XCTAssertEqual(expression, "UPDATE \"TableName\" "
                                 + "REMOVE \"theList\" "
                                 + "WHERE PK='partitionKey's' AND SK='sortKey's' "
                                 + "AND RowVersion=1")

        let escapedExpression = try table.getUpdateExpression(tableName: tableName,
                                                              newItem: databaseItemB,
                                                              existingItem: databaseItemA,
                                                              escapeSingleQuote: true)
        XCTAssertEqual(escapedExpression, "UPDATE \"TableName\" "
                                        + "REMOVE \"theList\" "
                                        + "WHERE PK='partitionKey''s' AND SK='sortKey''s' "
                                        + "AND RowVersion=1")
    }
    
    func testDeleteItemExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly's", secondly: "secondly's")
        let payloadA = TestTypeC(theString: "firstly's", theNumber: 4, theStruct: theStruct, theList: ["thirdly's", "fourthly's"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey's",
                                                       sortKey: "sortKey's")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()
        
        let expression = try table.getDeleteExpression(tableName: tableName,
                                                       existingItem: databaseItemA,
                                                       escapeSingleQuote: false)
        XCTAssertEqual(expression, "DELETE FROM \"TableName\" "
                                 + "WHERE PK='partitionKey's' AND SK='sortKey's' "
                                 + "AND RowVersion=1")

        let escapedExpression = try table.getDeleteExpression(tableName: tableName,
                                                              existingItem: databaseItemA,
                                                              escapeSingleQuote: true)
        XCTAssertEqual(escapedExpression, "DELETE FROM \"TableName\" "
                                        + "WHERE PK='partitionKey''s' AND SK='sortKey''s' "
                                        + "AND RowVersion=1")
    }
    
    func testDeleteKeyExpression() throws {
        let tableName = "TableName"

        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey's",
                                                       sortKey: "sortKey's")

        let table = InMemoryDynamoDBCompositePrimaryKeyTable()
        
        let expression = try table.getDeleteExpression(tableName: tableName,
                                                       existingKey: compositeKey,
                                                       escapeSingleQuote: false)
        XCTAssertEqual(expression, "DELETE FROM \"TableName\" "
                                 + "WHERE PK='partitionKey's' AND SK='sortKey's'")

        let expressionEscaped = try table.getDeleteExpression(tableName: tableName,
                                                              existingKey: compositeKey,
                                                              escapeSingleQuote: true)
        XCTAssertEqual(expressionEscaped, "DELETE FROM \"TableName\" "
                                        + "WHERE PK='partitionKey''s' AND SK='sortKey''s'")
    }
}

extension Array where Element == AttributeDifference {
    var pathMap: [String: AttributeDifference] {
        var newPathMap: [String: AttributeDifference] = [:]
        self.forEach { attributeDifference in
            newPathMap[attributeDifference.path] = attributeDifference
        }
        
        return newPathMap
    }
}
