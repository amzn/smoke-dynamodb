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
//  DynamoDBCompositePrimaryKeyTableUpdateItemConditionallyAtKeyTests.swift
//  SmokeDynamoDBTests
//

import XCTest
@testable import SmokeDynamoDB

class DynamoDBCompositePrimaryKeyTableUpdateItemConditionallyAtKeyTests: XCTestCase {
    
    func updatedPayloadProvider(item: TestTypeA) -> TestTypeA {
        return TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
    }
    
    func getUpdatedPayloadProviderAsync() -> ((TestTypeA) async throws -> TestTypeA) {
        func provider(item: TestTypeA) async throws -> TestTypeA {
            return TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
        }
        
        return provider
    }
    
    func testUpdateItemConditionallyAtKey() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        try await table.insertItem(databaseItem)
        
        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! await table.getItem(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: updatedPayloadProvider)
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! await table.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithAsyncProvider() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        try await table.insertItem(databaseItem)
        
        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! await table.getItem(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        let asyncUpdatedPayloadProvider = getUpdatedPayloadProviderAsync()
        try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: asyncUpdatedPayloadProvider)
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! await table.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithAcceptableConcurrency() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        try await table.insertItem(databaseItem)
        
        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! await table.getItem(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: updatedPayloadProvider)
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! await table.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithAcceptableConcurrencyWithAsyncProvider() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        try await table.insertItem(databaseItem)
        
        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! await table.getItem(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        let asyncUpdatedPayloadProvider = getUpdatedPayloadProviderAsync()
        try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: asyncUpdatedPayloadProvider)
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! await table.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnacceptableConcurrency() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 100,
                                                     simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        try await table.insertItem(databaseItem)
        
        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! await table.getItem(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: updatedPayloadProvider)
            
            XCTFail("Expected concurrency error not thrown.")
        } catch SmokeDynamoDBError.concurrencyError {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! await table.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnacceptableConcurrencyWithAsyncProvider() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 100,
                                                     simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        try await table.insertItem(databaseItem)
        
        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! await table.getItem(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        let asyncUpdatedPayloadProvider = getUpdatedPayloadProviderAsync()
        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: asyncUpdatedPayloadProvider)
            
            XCTFail("Expected concurrency error not thrown.")
        } catch SmokeDynamoDBError.concurrencyError {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! await table.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    enum TestError: Error {
        case everythingIsWrong
    }
    
    func testUpdateItemConditionallyAtKeyWithFailingUpdate() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 100,
                                                     simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        try await table.insertItem(databaseItem)
        
        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! await table.getItem(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        var passCount = 0
        
        func failingUpdatedPayloadProvider(item: TestTypeA) throws -> TestTypeA {
            if passCount < 5 {
                passCount += 1
                return TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
            } else {
                // fail before the retry limit with a custom error
                throw TestError.everythingIsWrong
            }
        }
        
        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: failingUpdatedPayloadProvider)
            
            XCTFail("Expected everythingIsWrong error not thrown.")
        } catch TestError.everythingIsWrong {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! await table.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithFailingUpdateWithAsyncProvider() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 100,
                                                     simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        try await table.insertItem(databaseItem)
        
        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! await table.getItem(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        var passCount = 0
        
        func failingUpdatedPayloadProvider(item: TestTypeA) async throws -> TestTypeA {
            if passCount < 5 {
                passCount += 1
                return TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
            } else {
                // fail before the retry limit with a custom error
                throw TestError.everythingIsWrong
            }
        }
        
        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: failingUpdatedPayloadProvider)
            
            XCTFail("Expected everythingIsWrong error not thrown.")
        } catch TestError.everythingIsWrong {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! await table.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnknownItem() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        
        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: updatedPayloadProvider)
            
            XCTFail("Expected concurrency error not thrown.")
        } catch SmokeDynamoDBError.conditionalCheckFailed {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA>? = try! await table.getItem(forKey: key)
        
        XCTAssertNil(secondRetrievedItem)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnknownItemWithAsyncProvider() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        
        let asyncUpdatedPayloadProvider = getUpdatedPayloadProviderAsync()
        do {
            try await table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: asyncUpdatedPayloadProvider)
            
            XCTFail("Expected concurrency error not thrown.")
        } catch SmokeDynamoDBError.conditionalCheckFailed {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA>? = try! await table.getItem(forKey: key)
        
        XCTAssertNil(secondRetrievedItem)
    }
}
