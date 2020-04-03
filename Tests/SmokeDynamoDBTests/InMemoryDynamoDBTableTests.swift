// Copyright 2018-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
//  InMemoryDynamoDBTableTests.swift
//  SmokeDynamoDBTests
//

import XCTest
@testable import SmokeDynamoDB
import SmokeHTTPClient
import DynamoDBModel

class InMemoryDynamoDBTableTests: XCTestCase {
    
    func testInsertAndUpdateSync() throws {
        let table = InMemoryDynamoDBTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                        sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItemSync(databaseItem))
        
        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try table.getItemSync(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        let updatedPayload = TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
        let updatedDatabaseItem = retrievedItem.createUpdatedItem(withValue: updatedPayload)
        XCTAssertNoThrow(try table.updateItemSync(newItem: updatedDatabaseItem, existingItem: retrievedItem))
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try table.getItemSync(forKey: key)!
        
        XCTAssertEqual(updatedDatabaseItem.compositePrimaryKey.sortKey, secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(updatedDatabaseItem.rowValue.firstly, secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual(updatedDatabaseItem.rowValue.secondly, secondRetrievedItem.rowValue.secondly)
    }
    
    func testInsertAndUpdateAsync() throws {
        let table = InMemoryDynamoDBTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                        sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItemSync(databaseItem))
        
        var retrievedItemOptional: StandardTypedDatabaseItem<TestTypeA>?
        try table.getItemAsync(forKey: key) { (result: SmokeDynamoDBErrorResult<StandardTypedDatabaseItem<TestTypeA>?>) in
            if case .success(let output) = result {
                retrievedItemOptional = output
            }
        }
        
        guard let retrievedItem = retrievedItemOptional else {
            return XCTFail()
        }
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        var isCompleted = false
        func completionHandler(error: Error?) {
            if error != nil {
                XCTFail()
            }
            
            isCompleted = true
        }
        
        let updatedPayload = TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
        let updatedDatabaseItem = retrievedItem.createUpdatedItem(withValue: updatedPayload)
        XCTAssertNoThrow(try table.updateItemAsync(newItem: updatedDatabaseItem,
                                                existingItem: retrievedItem, completion: completionHandler))
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try table.getItemSync(forKey: key)!
        
        XCTAssertEqual(updatedDatabaseItem.compositePrimaryKey.sortKey, secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(updatedDatabaseItem.rowValue.firstly, secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual(updatedDatabaseItem.rowValue.secondly, secondRetrievedItem.rowValue.secondly)
        XCTAssertTrue(isCompleted)
    }
    
    func testDoubleInsertSync() {
        let table = InMemoryDynamoDBTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                        sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItemSync(databaseItem))
        
        do {
           try table.insertItemSync(databaseItem)
            XCTFail()
        } catch SmokeDynamoDBError.conditionalCheckFailed {
            // expected error
        } catch {
            XCTFail("Unexpected error \(error)")
        }
    }
    
    func testDoubleInsertAsync() throws {
        let table = InMemoryDynamoDBTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                        sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        var isInsert1Completed = false
        func completionHandler(error: Error?) {
            if error != nil {
                XCTFail()
            }
            
            isInsert1Completed = true
        }
        
        XCTAssertNoThrow(try table.insertItemAsync(databaseItem, completion: completionHandler))
        
        var isInsert2Completed = false
        try table.insertItemAsync(databaseItem) { error in
            guard let theError = error else {
                return XCTFail("Expected error not thrown")
            }
            guard case SmokeDynamoDBError.conditionalCheckFailed = theError else {
                return XCTFail("Unexpected error \(theError)")
            }
            
            isInsert2Completed = true
        }
        
        XCTAssertTrue(isInsert1Completed)
        XCTAssertTrue(isInsert2Completed)
    }
    
    func testUpdateWithoutInsertSync() {
        let table = InMemoryDynamoDBTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                        sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let updatedPayload = TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        do {
            try table.updateItemSync(newItem: databaseItem.createUpdatedItem(withValue: updatedPayload),
                                      existingItem: databaseItem)
            XCTFail()
        } catch SmokeDynamoDBError.conditionalCheckFailed {
            // expected error
        } catch {
            XCTFail("Unexpected error \(error)")
        }
    }
    
    func testUpdateWithoutInsertAsync() throws {
        let table = InMemoryDynamoDBTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                        sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let updatedPayload = TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        try table.updateItemAsync(newItem: databaseItem.createUpdatedItem(withValue: updatedPayload),
                                  existingItem: databaseItem) { error in
            guard let theError = error else {
                return XCTFail("Expected error not thrown")
            }
            guard case SmokeDynamoDBError.conditionalCheckFailed = theError else {
                return XCTFail("Unexpected error \(theError)")
            }
        }
    }
  
    func testPaginatedQuerySync() throws {
        let table = InMemoryDynamoDBTable()
        
        var items: [StandardTypedDatabaseItem<TestTypeA>] = []
        
        // add to the database a lot of items - a number that isn't a multiple of the pagination page size
        for index in 0..<1376 {
            let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                        sortKey: "sortId_\(index)")
            let payload = TestTypeA(firstly: "firstly_\(index)", secondly: "secondly_\(index)")
            let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
            
            XCTAssertNoThrow(try table.insertItemSync(databaseItem))
            items.append(databaseItem)
        }
        
        var retrievedItems: [StandardPolymorphicDatabaseItem<TestCodableTypes>] = []
        
        var exclusiveStartKey: String?
        
        // get everything back from the database
        while true {
            let paginatedItems: ([StandardPolymorphicDatabaseItem<TestCodableTypes>], String?) =
                try table.querySync(forPartitionKey: "partitionId",
                          sortKeyCondition: nil,
                          limit: 100,
                          exclusiveStartKey: exclusiveStartKey)
            
            retrievedItems += paginatedItems.0
            
            // if there are more items
            if let lastEvaluatedKey = paginatedItems.1 {
                exclusiveStartKey = lastEvaluatedKey
            } else {
                // we have all the items
                break
            }
        }
        
        XCTAssertEqual(items.count, retrievedItems.count)
        
        for index in 0..<items.count {
            let originalItem = items[index]
            let retrievedItem = items[index]
            
            XCTAssertEqual(originalItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
            XCTAssertEqual(originalItem.rowValue.firstly, retrievedItem.rowValue.firstly)
            XCTAssertEqual(originalItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        }
    }
    
    func testPaginatedQueryAsync() throws {
        let table = InMemoryDynamoDBTable()
        
        var items: [StandardTypedDatabaseItem<TestTypeA>] = []
        
        // add to the database a lot of items - a number that isn't a multiple of the pagination page size
        for index in 0..<1376 {
            let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                        sortKey: "sortId_\(index)")
            let payload = TestTypeA(firstly: "firstly_\(index)", secondly: "secondly_\(index)")
            let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
            
            XCTAssertNoThrow(try table.insertItemSync(databaseItem))
            items.append(databaseItem)
        }
        
        var retrievedItems: [StandardPolymorphicDatabaseItem<TestCodableTypes>] = []
        
        var exclusiveStartKey: String?
        
        // get everything back from the database
        repeat {
            func handleQueryOutput(result: SmokeDynamoDBErrorResult<([StandardPolymorphicDatabaseItem<TestCodableTypes>], String?)>) {
                switch result {
                case .success(let paginatedItems):
                    retrievedItems += paginatedItems.0
                    exclusiveStartKey = paginatedItems.1
                case .failure(_):
                    XCTFail()
                }
            }
            
            try table.queryAsync(forPartitionKey: "partitionId",
                          sortKeyCondition: nil,
                          limit: 100,
                          exclusiveStartKey: exclusiveStartKey, completion: handleQueryOutput)
        } while exclusiveStartKey != nil
        
        XCTAssertEqual(items.count, retrievedItems.count)
        
        for index in 0..<items.count {
            let originalItem = items[index]
            let retrievedItem = items[index]
            
            XCTAssertEqual(originalItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
            XCTAssertEqual(originalItem.rowValue.firstly, retrievedItem.rowValue.firstly)
            XCTAssertEqual(originalItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        }
    }
    
    func testQuerySync() throws {
        let table = InMemoryDynamoDBTable()
        
        var items: [StandardTypedDatabaseItem<TestTypeA>] = []
        
        // add to the database a lot of items - a number that isn't a multiple of the pagination page size
        for index in 0..<1376 {
            let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                        sortKey: "sortId_\(index)")
            let payload = TestTypeA(firstly: "firstly_\(index)", secondly: "secondly_\(index)")
            let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
            
            XCTAssertNoThrow(try table.insertItemSync(databaseItem))
            items.append(databaseItem)
        }
        
        let retrievedItems: [StandardPolymorphicDatabaseItem<TestCodableTypes>] =
            try table.querySync(forPartitionKey: "partitionId",
                                 sortKeyCondition: nil)
        
        XCTAssertEqual(items.count, retrievedItems.count)
        
        for index in 0..<items.count {
            let originalItem = items[index]
            let retrievedItem = items[index]
            
            XCTAssertEqual(originalItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
            XCTAssertEqual(originalItem.rowValue.firstly, retrievedItem.rowValue.firstly)
            XCTAssertEqual(originalItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        }
    }
    
    func testQueryAsync() throws {
        let table = InMemoryDynamoDBTable()
        
        var items: [StandardTypedDatabaseItem<TestTypeA>] = []
        
        // add to the database a lot of items - a number that isn't a multiple of the pagination page size
        for index in 0..<1376 {
            let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                        sortKey: "sortId_\(index)")
            let payload = TestTypeA(firstly: "firstly_\(index)", secondly: "secondly_\(index)")
            let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
            
            XCTAssertNoThrow(try table.insertItemSync(databaseItem))
            items.append(databaseItem)
        }
        
        var retrievedItems: [StandardPolymorphicDatabaseItem<TestCodableTypes>] = []
        func handleQueryResult(result: SmokeDynamoDBErrorResult<[StandardPolymorphicDatabaseItem<TestCodableTypes>]>) {
            switch result {
            case .success(let queryItems):
                retrievedItems.append(contentsOf: queryItems)
            case .failure:
                XCTFail()
            }
        }
        try table.queryAsync(forPartitionKey: "partitionId",
                              sortKeyCondition: nil, completion: handleQueryResult)
        
        XCTAssertEqual(items.count, retrievedItems.count)
        
        for index in 0..<items.count {
            let originalItem = items[index]
            let retrievedItem = items[index]
            
            XCTAssertEqual(originalItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
            XCTAssertEqual(originalItem.rowValue.firstly, retrievedItem.rowValue.firstly)
            XCTAssertEqual(originalItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        }
    }
  
    static var allTests = [
        ("testInsertAndUpdateSync", testInsertAndUpdateSync),
        ("testInsertAndUpdateAsync", testInsertAndUpdateAsync),
        ("testDoubleInsertSync", testDoubleInsertSync),
        ("testDoubleInsertAsync", testDoubleInsertAsync),
        ("testUpdateWithoutInsertSync", testUpdateWithoutInsertSync),
        ("testUpdateWithoutInsertSync", testUpdateWithoutInsertSync),
        ("testPaginatedQuerySync", testPaginatedQuerySync),
        ("testPaginatedQueryAsync", testPaginatedQueryAsync),
        ("testQuerySync", testQuerySync),
        ("testQueryAsync", testQueryAsync),
    ]
}
