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
import NIO

private let ORIGINAL_PAYLOAD = "Payload"
private let UPDATED_PAYLOAD = "Updated"

class TypedDatabaseItemRowWithItemVersionProtocolTests: XCTestCase {
    var eventLoopGroup: EventLoopGroup?
    var eventLoop: EventLoop!
    
    override func setUp() {
        super.setUp()
        
        let newEventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        eventLoop = newEventLoopGroup.next()
        eventLoopGroup = newEventLoopGroup
    }

    override func tearDown() {
        super.tearDown()
        
        try? eventLoopGroup?.syncShutdownGracefully()
        eventLoop = nil
    }


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
    
    func testStringFieldDifference() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "eigthly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: self.eventLoop)
        
        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap
        
        XCTAssertEqual(pathMap["theString"], .update(path: "theString", value: "'eigthly'"))
        XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
    }
    
    func testNumberFieldDifference() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 12, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: self.eventLoop)
        
        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap
        
        XCTAssertEqual(pathMap["theNumber"], .update(path: "theNumber", value: "12"))
        XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
    }
    
    func testStructFieldDifference() throws {
        let theStructA = TestTypeA(firstly: "firstly", secondly: "secondly")
        let theStructB = TestTypeA(firstly: "eigthly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStructA, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStructB, theList: ["thirdly", "fourthly"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: self.eventLoop)
        
        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap
        
        XCTAssertEqual(pathMap["theStruct.firstly"], .update(path: "theStruct.firstly", value: "'eigthly'"))
        XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
    }
    
    func testListFieldDifference() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "eigthly", "ninthly", "tenthly"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: self.eventLoop)
        
        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap
        
        XCTAssertEqual(pathMap["theList[1]"], .update(path: "theList[1]", value: "'eigthly'"))
        XCTAssertEqual(pathMap["theList"], .listAppend(path: "theList", value: "['ninthly', 'tenthly']"))
        XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
    }
    
    func testStringFieldAddition() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: nil, theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "eigthly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: self.eventLoop)
        
        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap
        
        XCTAssertEqual(pathMap["theString"], .update(path: "theString", value: "'eigthly'"))
        XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
    }
    
    func testNumberFieldAddition() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: nil, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 12, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: self.eventLoop)
        
        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap
        
        XCTAssertEqual(pathMap["theNumber"], .update(path: "theNumber", value: "12"))
        XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
    }
    
    func testStructFieldAddition() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: nil, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: self.eventLoop)
        
        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap
        
        guard case .update(_, let value) = pathMap["theStruct"] else {
            XCTFail()
            return
        }
        
        let valueMatches = (value == "{'firstly': 'firstly', 'secondly': 'secondly'}") ||
            (value == "{'secondly': 'secondly', 'firstly': 'firstly'}")
        
        XCTAssertTrue(valueMatches)
        XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
    }
    
    func testListFieldAddition() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: nil)
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "eigthly", "ninthly", "tenthly"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: self.eventLoop)
        
        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap
        
        XCTAssertEqual(pathMap["theList"], .update(path: "theList", value: "['thirdly', 'eigthly', 'ninthly', 'tenthly']"))
        XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
    }
    
    func testStringFieldRemoval() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: nil, theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: self.eventLoop)
        
        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap
        
        XCTAssertEqual(pathMap["theString"], .remove(path: "theString"))
        XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
    }
    
    func testNumberFieldRemoval() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: nil, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: self.eventLoop)
        
        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap
        
        XCTAssertEqual(pathMap["theNumber"], .remove(path: "theNumber"))
        XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
    }
    
    func testStructFieldRemoval() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: nil, theList: ["thirdly", "fourthly"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: self.eventLoop)
        
        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap
        
        XCTAssertEqual(pathMap["theStruct"], .remove(path: "theStruct"))
        XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
    }
    
    func testListFieldRemoval() throws {
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: nil)
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = databaseItemA.createUpdatedItem(withValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: self.eventLoop)
        
        let differences = try table.diffItems(newItem: databaseItemB,
                                              existingItem: databaseItemA)
        let pathMap = differences.pathMap
        
        XCTAssertEqual(pathMap["theList"], .remove(path: "theList"))
        XCTAssertEqual(pathMap["RowVersion"], .update(path: "RowVersion", value: "2"))
    }
    
    func testListFieldDifferenceExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "eigthly", "ninthly", "tenthly"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: self.eventLoop)
        
        let expression = try table.getUpdateExpression(tableName: tableName,
                                                       newItem: databaseItemB,
                                                       existingItem: databaseItemA)
        XCTAssertEqual(expression, "UPDATE \"TableName\" "
                                 + "SET theList[1]='eigthly' "
                                 + "SET theList=list_append(theList,['ninthly', 'tenthly'] "
                                 + "WHERE PK='partitionKey' AND SK='sortKey' "
                                 + "AND RowVersion=1")
    }
    
    func testListFieldAdditionExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: nil)
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "eigthly", "ninthly", "tenthly"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: self.eventLoop)
        
        let expression = try table.getUpdateExpression(tableName: tableName,
                                                       newItem: databaseItemB,
                                                       existingItem: databaseItemA)
        XCTAssertEqual(expression, "UPDATE \"TableName\" "
                                 + "SET theList=['thirdly', 'eigthly', 'ninthly', 'tenthly'] "
                                 + "WHERE PK='partitionKey' AND SK='sortKey' "
                                 + "AND RowVersion=1")
    }
    
    func testListFieldRemovalExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        let payloadB = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: nil)
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        let databaseItemB = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadB)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: self.eventLoop)
        
        let expression = try table.getUpdateExpression(tableName: tableName,
                                                       newItem: databaseItemB,
                                                       existingItem: databaseItemA)
        XCTAssertEqual(expression, "UPDATE \"TableName\" "
                                 + "REMOVE theList "
                                 + "WHERE PK='partitionKey' AND SK='sortKey' "
                                 + "AND RowVersion=1")
    }
    
    func testDeleteItemExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: self.eventLoop)
        
        let expression = try table.getDeleteExpression(tableName: tableName,
                                                       existingItem: databaseItemA)
        XCTAssertEqual(expression, "DELETE FROM \"TableName\" "
                                 + "WHERE PK='partitionKey' AND SK='sortKey' "
                                 + "AND RowVersion=1")
    }
    
    func testDeleteKeyExpression() throws {
        let tableName = "TableName"
        let theStruct = TestTypeA(firstly: "firstly", secondly: "secondly")
        let payloadA = TestTypeC(theString: "firstly", theNumber: 4, theStruct: theStruct, theList: ["thirdly", "fourthly"])
        
        let compositeKey = StandardCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let databaseItemA = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: payloadA)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: self.eventLoop)
        
        let expression = try table.getDeleteExpression(tableName: tableName,
                                                       existingItem: databaseItemA)
        XCTAssertEqual(expression, "DELETE FROM \"TableName\" "
                                 + "WHERE PK='partitionKey' AND SK='sortKey' "
                                 + "AND RowVersion=1")
    }
    
    static var allTests = [
        ("testCreateUpdatedRowWithItemVersion", testCreateUpdatedRowWithItemVersion),
        ("testCreateUpdatedRowWithItemVersionWithCorrectConditionalVersion",
         testCreateUpdatedRowWithItemVersionWithCorrectConditionalVersion),
        ("testCreateUpdatedRowWithItemVersionWithIncorrectConditionalVersion",
         testCreateUpdatedRowWithItemVersionWithIncorrectConditionalVersion),
        ("testStringFieldDifference", testStringFieldDifference),
        ("testNumberFieldDifference", testNumberFieldDifference),
        ("testStructFieldDifference", testStructFieldDifference),
        ("testListFieldDifference", testListFieldDifference),
        ("testStringFieldAddition", testStringFieldAddition),
        ("testNumberFieldAddition", testNumberFieldAddition),
        ("testStructFieldAddition", testStructFieldAddition),
        ("testListFieldAddition", testListFieldAddition),
        ("testStringFieldRemoval", testStringFieldRemoval),
        ("testNumberFieldRemoval", testNumberFieldRemoval),
        ("testStructFieldRemoval", testStructFieldRemoval),
        ("testListFieldRemoval", testListFieldRemoval),
        ("testListFieldDifferenceExpression", testListFieldDifferenceExpression),
        ("testListFieldAdditionExpression", testListFieldAdditionExpression),
        ("testDeleteItemExpression", testDeleteItemExpression),
    ]
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
