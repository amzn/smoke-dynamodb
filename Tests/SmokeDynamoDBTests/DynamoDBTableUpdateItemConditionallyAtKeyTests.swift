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
//  DynamoDBTableUpdateItemConditionallyAtKeyTests.swift
//  SmokeDynamoDBTests
//

import XCTest
@testable import SmokeDynamoDB

@available(swift, deprecated: 2.0,
           renamed: "DynamoDBCompositePrimaryKeyTableUpdateItemConditionallyAtKeyTests")
class DynamoDBTableUpdateItemConditionallyAtKeyTests: XCTestCase {
    
    func updatedPayloadProvider(item: TestTypeA) -> TestTypeA {
        return TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
    }
    
    func testUpdateItemConditionallyAtKeySync() {
        let table = InMemoryDynamoDBTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                     sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItemSync(databaseItem))
        
        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! table.getItemSync(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        XCTAssertNoThrow(try table.conditionallyUpdateItemSync(forKey: key, updatedPayloadProvider: updatedPayloadProvider))
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! table.getItemSync(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyAsync() {
        let table = InMemoryDynamoDBTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                     sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItemSync(databaseItem))
        
        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! table.getItemSync(forKey: key)!
        
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
        
        XCTAssertNoThrow(try table.conditionallyUpdateItemAsync(forKey: key,
                                                                      updatedPayloadProvider: updatedPayloadProvider,
                                                                      completion: completionHandler))
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! table.getItemSync(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
        XCTAssertTrue(isCompleted)
    }
    
    func testUpdateItemConditionallyAtKeyWithAcceptableConcurrencySync() {
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: InMemoryDynamoDBTable(),
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                     sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItemSync(databaseItem))
        
        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! table.getItemSync(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        XCTAssertNoThrow(try table.conditionallyUpdateItemSync(forKey: key, updatedPayloadProvider: updatedPayloadProvider))
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! table.getItemSync(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithAcceptableConcurrencyAsync() {
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: InMemoryDynamoDBTable(),
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                     sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItemSync(databaseItem))
        
        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! table.getItemSync(forKey: key)!
        
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
        
        XCTAssertNoThrow(try table.conditionallyUpdateItemAsync(forKey: key,
                                                                      updatedPayloadProvider: updatedPayloadProvider,
                                                                      completion: completionHandler))
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! table.getItemSync(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
        XCTAssertTrue(isCompleted)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnacceptableConcurrencySync() {
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: InMemoryDynamoDBTable(),
                                                     simulateConcurrencyModifications: 100,
                                                     simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                     sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItemSync(databaseItem))
        
        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! table.getItemSync(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        do {
            try table.conditionallyUpdateItemSync(forKey: key, updatedPayloadProvider: updatedPayloadProvider)
            
            XCTFail("Expected concurrency error not thrown.")
        } catch SmokeDynamoDBError.concurrencyError {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! table.getItemSync(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnacceptableConcurrencyAsync() {
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: InMemoryDynamoDBTable(),
                                                     simulateConcurrencyModifications: 100,
                                                     simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                     sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItemSync(databaseItem))
        
        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! table.getItemSync(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        var errorThrown = false
        do {
            try table.conditionallyUpdateItemAsync(forKey: key, updatedPayloadProvider: updatedPayloadProvider) { error in
                if let error = error, case SmokeDynamoDBError.concurrencyError = error {
                    errorThrown = true
                }
            }
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        XCTAssertTrue(errorThrown)
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! table.getItemSync(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    enum TestError: Error {
        case everythingIsWrong
    }
    
    func testUpdateItemConditionallyAtKeyWithFailingUpdateSync() {
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: InMemoryDynamoDBTable(),
                                                     simulateConcurrencyModifications: 100,
                                                     simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                     sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItemSync(databaseItem))
        
        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! table.getItemSync(forKey: key)!
        
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
            try table.conditionallyUpdateItemSync(forKey: key, updatedPayloadProvider: failingUpdatedPayloadProvider)
            
            XCTFail("Expected everythingIsWrong error not thrown.")
        } catch TestError.everythingIsWrong {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! table.getItemSync(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithFailingUpdateAsync() {
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: InMemoryDynamoDBTable(),
                                                     simulateConcurrencyModifications: 100,
                                                     simulateOnInsertItem: false)
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                     sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try table.insertItemSync(databaseItem))
        
        let retrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! table.getItemSync(forKey: key)!
        
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
            try table.conditionallyUpdateItemAsync(forKey: key, updatedPayloadProvider: failingUpdatedPayloadProvider) { error in
                guard let theError = error, case TestError.everythingIsWrong = theError else {
                    return XCTFail("Expected everythingIsWrong error not thrown.")
                }
            }
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA> = try! table.getItemSync(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnknownItemSync() {
        let table = InMemoryDynamoDBTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                     sortKey: "sortId")
        
        do {
            try table.conditionallyUpdateItemSync(forKey: key, updatedPayloadProvider: updatedPayloadProvider)
            
            XCTFail("Expected concurrency error not thrown.")
        } catch SmokeDynamoDBError.conditionalCheckFailed {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA>? = try! table.getItemSync(forKey: key)
        
        XCTAssertNil(secondRetrievedItem)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnknownItemAsync() {
        let table = InMemoryDynamoDBTable()
        
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                                     sortKey: "sortId")
        
        do {
            try table.conditionallyUpdateItemAsync(forKey: key, updatedPayloadProvider: updatedPayloadProvider)  { error in
                guard let theError = error, case SmokeDynamoDBError.conditionalCheckFailed = theError else {
                    return XCTFail("Expected concurrency error not thrown.")
                }
            }
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: StandardTypedDatabaseItem<TestTypeA>? = try! table.getItemSync(forKey: key)
        
        XCTAssertNil(secondRetrievedItem)
    }
    
    static var allTests = [
        ("testUpdateItemConditionallyAtKeySync", testUpdateItemConditionallyAtKeySync),
        ("testUpdateItemConditionallyAtKeyAsync", testUpdateItemConditionallyAtKeyAsync),
        ("testUpdateItemConditionallyAtKeyWithAcceptableConcurrencySync", testUpdateItemConditionallyAtKeyWithAcceptableConcurrencySync),
        ("testUpdateItemConditionallyAtKeyWithAcceptableConcurrencyAsync", testUpdateItemConditionallyAtKeyWithAcceptableConcurrencyAsync),
        ("testUpdateItemConditionallyAtKeyWithUnacceptableConcurrencySync", testUpdateItemConditionallyAtKeyWithUnacceptableConcurrencySync),
        ("testUpdateItemConditionallyAtKeyWithUnacceptableConcurrencyAsync", testUpdateItemConditionallyAtKeyWithUnacceptableConcurrencyAsync),
        ("testUpdateItemConditionallyAtKeyWithFailingUpdateSync", testUpdateItemConditionallyAtKeyWithFailingUpdateSync),
        ("testUpdateItemConditionallyAtKeyWithFailingUpdateAsync", testUpdateItemConditionallyAtKeyWithFailingUpdateAsync),
        ("testUpdateItemConditionallyAtKeyWithUnknownItemSync", testUpdateItemConditionallyAtKeyWithUnknownItemSync),
        ("testUpdateItemConditionallyAtKeyWithUnknownItemAsync", testUpdateItemConditionallyAtKeyWithUnknownItemAsync),
    ]
}
