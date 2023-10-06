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
import NIO

class DynamoDBCompositePrimaryKeyTableUpdateItemConditionallyAtKeyTests: XCTestCase {
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
    
    func updatedPayloadProvider(item: TestTypeA) -> TestTypeA {
        return TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
    }
    
    func getUpdatedPayloadProviderWithEventLoopFuture(on eventLoop: EventLoop) -> ((TestTypeA) -> EventLoopFuture<TestTypeA>) {
        func provider(item: TestTypeA) -> EventLoopFuture<TestTypeA> {
            return eventLoop.makeSucceededFuture(TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2"))
        }
        
        return provider
    }
    
    typealias TestTypeADatabaseItem = StandardTypedDatabaseItem<TestTypeA>
    func updatedItemProvider(item: TestTypeADatabaseItem) -> TestTypeADatabaseItem {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        return TestTypeADatabaseItem.newItem(
            withKey: key,
            andValue: TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2"))
    }
    
    func getUpdatedItemProviderWithEventLoopFuture(on eventLoop: EventLoop) -> ((TestTypeADatabaseItem) -> EventLoopFuture<TestTypeADatabaseItem>) {
        func provider(item: TestTypeADatabaseItem) -> EventLoopFuture<TestTypeADatabaseItem> {
            let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                  sortKey: "sortId")
            let item = TestTypeADatabaseItem.newItem(
                withKey: key,
                andValue: TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2"))
            return eventLoop.makeSucceededFuture(item)
        }
        
        return provider
    }
    
    func testUpdateItemConditionallyAtKey() {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        
        let retrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        XCTAssertNoThrow(try table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: updatedPayloadProvider).wait())
        
        let secondRetrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithItemProvider() {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        
        let retrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        XCTAssertNoThrow(try table.conditionallyUpdateItem(forKey: key, updatedItemProvider: updatedItemProvider).wait())
        
        let secondRetrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithAsyncProvider() {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        
        let retrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        let asyncUpdatedPayloadProvider = getUpdatedPayloadProviderWithEventLoopFuture(on: self.eventLoop)
        XCTAssertNoThrow(try table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: asyncUpdatedPayloadProvider).wait())
        
        let secondRetrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithAsyncItemProvider() {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        
        let retrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        let asyncUpdatedItemProvider = getUpdatedItemProviderWithEventLoopFuture(on: self.eventLoop)
        XCTAssertNoThrow(try table.conditionallyUpdateItem(forKey: key, updatedItemProvider: asyncUpdatedItemProvider).wait())
        
        let secondRetrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithAcceptableConcurrency() {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable, eventLoop: eventLoop,
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        
        let retrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        XCTAssertNoThrow(try table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: updatedPayloadProvider).wait())
        
        let secondRetrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithAcceptableConcurrencyWithItemProvider() {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable, eventLoop: eventLoop,
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        
        let retrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        XCTAssertNoThrow(try table.conditionallyUpdateItem(forKey: key, updatedItemProvider: updatedItemProvider).wait())
        
        let secondRetrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithAcceptableConcurrencyWithAsyncProvider() {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable, eventLoop: eventLoop,
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        
        let retrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        let asyncUpdatedPayloadProvider = getUpdatedPayloadProviderWithEventLoopFuture(on: self.eventLoop)
        XCTAssertNoThrow(try table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: asyncUpdatedPayloadProvider).wait())
        
        let secondRetrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithAcceptableConcurrencyWithAsyncItemProvider() {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable, eventLoop: eventLoop,
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        
        let retrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        let asyncUpdatedItemProvider = getUpdatedItemProviderWithEventLoopFuture(on: self.eventLoop)
        XCTAssertNoThrow(try table.conditionallyUpdateItem(forKey: key, updatedItemProvider: asyncUpdatedItemProvider).wait())
        
        let secondRetrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnacceptableConcurrency() {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable, eventLoop: eventLoop,
                                                                        simulateConcurrencyModifications: 100,
                                                                        simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        
        let retrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        do {
            try table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: updatedPayloadProvider).wait()
            
            XCTFail("Expected concurrency error not thrown.")
        } catch SmokeDynamoDBError.concurrencyError {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnacceptableConcurrencyWithItemProvider() {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable, eventLoop: eventLoop,
                                                                        simulateConcurrencyModifications: 100,
                                                                        simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        
        let retrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        do {
            try table.conditionallyUpdateItem(forKey: key, updatedItemProvider: updatedItemProvider).wait()
            
            XCTFail("Expected concurrency error not thrown.")
        } catch SmokeDynamoDBError.concurrencyError {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnacceptableConcurrencyWithAsyncProvider() {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable, eventLoop: eventLoop,
                                                                        simulateConcurrencyModifications: 100,
                                                                        simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        
        let retrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        let asyncUpdatedPayloadProvider = getUpdatedPayloadProviderWithEventLoopFuture(on: self.eventLoop)
        do {
            try table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: asyncUpdatedPayloadProvider).wait()
            
            XCTFail("Expected concurrency error not thrown.")
        } catch SmokeDynamoDBError.concurrencyError {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnacceptableConcurrencyWithAsyncItemProvider() {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable, eventLoop: eventLoop,
                                                                        simulateConcurrencyModifications: 100,
                                                                        simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        
        let retrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        let asyncUpdatedItemProvider = getUpdatedItemProviderWithEventLoopFuture(on: self.eventLoop)
        do {
            try table.conditionallyUpdateItem(forKey: key, updatedItemProvider: asyncUpdatedItemProvider).wait()
            
            XCTFail("Expected concurrency error not thrown.")
        } catch SmokeDynamoDBError.concurrencyError {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    enum TestError: Error {
        case everythingIsWrong
    }
    
    func testUpdateItemConditionallyAtKeyWithFailingUpdate() {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable, eventLoop: eventLoop,
                                                                        simulateConcurrencyModifications: 100,
                                                                        simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        
        let retrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
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
            try table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: failingUpdatedPayloadProvider).wait()
            
            XCTFail("Expected everythingIsWrong error not thrown.")
        } catch TestError.everythingIsWrong {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithFailingUpdateWithItemProvider() {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable, eventLoop: eventLoop,
                                                                        simulateConcurrencyModifications: 100,
                                                                        simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        
        let retrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        var passCount = 0
        
        func failingUpdatedItemProvider(item: TestTypeADatabaseItem) throws -> TestTypeADatabaseItem {
            if passCount < 5 {
                passCount += 1
                let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                      sortKey: "sortId")
                return TestTypeADatabaseItem.newItem(
                    withKey: key,
                    andValue: TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2"))
            } else {
                // fail before the retry limit with a custom error
                throw TestError.everythingIsWrong
            }
        }
        
        do {
            try table.conditionallyUpdateItem(forKey: key, updatedItemProvider: failingUpdatedItemProvider).wait()
            
            XCTFail("Expected everythingIsWrong error not thrown.")
        } catch TestError.everythingIsWrong {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithFailingUpdateWithAsyncProvider() {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable, eventLoop: eventLoop,
                                                                        simulateConcurrencyModifications: 100,
                                                                        simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        
        let retrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        var passCount = 0
        
        func failingUpdatedPayloadProvider(item: TestTypeA) -> EventLoopFuture<TestTypeA> {
            if passCount < 5 {
                passCount += 1
                return eventLoop.makeSucceededFuture(TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2"))
            } else {
                // fail before the retry limit with a custom error
                return eventLoop.makeFailedFuture(TestError.everythingIsWrong)
            }
        }
        
        do {
            try table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: failingUpdatedPayloadProvider).wait()
            
            XCTFail("Expected everythingIsWrong error not thrown.")
        } catch TestError.everythingIsWrong {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithFailingUpdateWithAsyncItemProvider() {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable, eventLoop: eventLoop,
                                                                        simulateConcurrencyModifications: 100,
                                                                        simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        
        let retrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        var passCount = 0
        
        func failingUpdatedItemProvider(item: TestTypeADatabaseItem) -> EventLoopFuture<TestTypeADatabaseItem> {
            if passCount < 5 {
                passCount += 1
                let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                      sortKey: "sortId")
                let item = TestTypeADatabaseItem.newItem(
                    withKey: key,
                    andValue: TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2"))
                return eventLoop.makeSucceededFuture(item)
            } else {
                // fail before the retry limit with a custom error
                return eventLoop.makeFailedFuture(TestError.everythingIsWrong)
            }
        }
        
        do {
            try table.conditionallyUpdateItem(forKey: key, updatedItemProvider: failingUpdatedItemProvider).wait()
            
            XCTFail("Expected everythingIsWrong error not thrown.")
        } catch TestError.everythingIsWrong {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem = try! table.getItem(forKey: key).wait()!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnknownItem() {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        
        do {
            try table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: updatedPayloadProvider).wait()
            
            XCTFail("Expected concurrency error not thrown.")
        } catch SmokeDynamoDBError.conditionalCheckFailed {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem? = try! table.getItem(forKey: key).wait()
        
        XCTAssertNil(secondRetrievedItem)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnknownItemWithItemProvider() {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        
        do {
            try table.conditionallyUpdateItem(forKey: key, updatedItemProvider: updatedItemProvider).wait()
            
            XCTFail("Expected concurrency error not thrown.")
        } catch SmokeDynamoDBError.conditionalCheckFailed {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem? = try! table.getItem(forKey: key).wait()
        
        XCTAssertNil(secondRetrievedItem)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnknownItemWithAsyncProvider() {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        
        let asyncUpdatedPayloadProvider = getUpdatedPayloadProviderWithEventLoopFuture(on: self.eventLoop)
        do {
            try table.conditionallyUpdateItem(forKey: key, updatedPayloadProvider: asyncUpdatedPayloadProvider).wait()
            
            XCTFail("Expected concurrency error not thrown.")
        } catch SmokeDynamoDBError.conditionalCheckFailed {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem? = try! table.getItem(forKey: key).wait()
        
        XCTAssertNil(secondRetrievedItem)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnknownItemWithAsyncItemProvider() {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                              sortKey: "sortId")
        
        let asyncUpdatedItemProvider = getUpdatedItemProviderWithEventLoopFuture(on: self.eventLoop)
        do {
            try table.conditionallyUpdateItem(forKey: key, updatedItemProvider: asyncUpdatedItemProvider).wait()
            
            XCTFail("Expected concurrency error not thrown.")
        } catch SmokeDynamoDBError.conditionalCheckFailed {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: TestTypeADatabaseItem? = try! table.getItem(forKey: key).wait()
        
        XCTAssertNil(secondRetrievedItem)
    }
}
