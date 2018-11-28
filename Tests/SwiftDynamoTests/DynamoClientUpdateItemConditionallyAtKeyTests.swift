//
//  DynamoClientUpdateItemConditionallyAtKeyTests.swift
//  SwiftDynamoTests
//

import XCTest
@testable import SwiftDynamo

class DynamoClientUpdateItemConditionallyAtKeyTests: XCTestCase {
    
    func testUpdateItemConditionallyAtKey() {
        let client = InMemoryDynamoClient()
        
        let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId",
                                                     sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = DefaultIdentityTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try client.insertItem(databaseItem))
        
        let retrievedItem: DefaultIdentityTypedDatabaseItem<TestTypeA> = try! client.getItem(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        XCTAssertNoThrow(try client.updateItemConditionallyAtKey(key) { payload in
            return TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
        })
        
        let secondRetrievedItem: DefaultIdentityTypedDatabaseItem<TestTypeA> = try! client.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithAcceptableConcurrency() {
        let client = SimulateConcurrencyDynamoClient(wrappedDynamoClient: InMemoryDynamoClient(),
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)
        
        let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId",
                                                     sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = DefaultIdentityTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try client.insertItem(databaseItem))
        
        let retrievedItem: DefaultIdentityTypedDatabaseItem<TestTypeA> = try! client.getItem(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        XCTAssertNoThrow(try client.updateItemConditionallyAtKey(key) { payload in
            return TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
        })
        
        let secondRetrievedItem: DefaultIdentityTypedDatabaseItem<TestTypeA> = try! client.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual("firstlyX2", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondlyX2", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnacceptableConcurrency() {
        let client = SimulateConcurrencyDynamoClient(wrappedDynamoClient: InMemoryDynamoClient(),
                                                     simulateConcurrencyModifications: 100,
                                                     simulateOnInsertItem: false)
        
        let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId",
                                                     sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = DefaultIdentityTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try client.insertItem(databaseItem))
        
        let retrievedItem: DefaultIdentityTypedDatabaseItem<TestTypeA> = try! client.getItem(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        do {
            try client.updateItemConditionallyAtKey(key) { payload in
                return TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
            }
            
            XCTFail("Expected concurrency error not thrown.")
        } catch SwiftDynamoError.concurrencyError {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: DefaultIdentityTypedDatabaseItem<TestTypeA> = try! client.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    enum TestError: Error {
        case everythingIsWrong
    }
    
    func testUpdateItemConditionallyAtKeyWithFailingUpdate() {
        let client = SimulateConcurrencyDynamoClient(wrappedDynamoClient: InMemoryDynamoClient(),
                                                     simulateConcurrencyModifications: 100,
                                                     simulateOnInsertItem: false)
        
        let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId",
                                                     sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = DefaultIdentityTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try client.insertItem(databaseItem))
        
        let retrievedItem: DefaultIdentityTypedDatabaseItem<TestTypeA> = try! client.getItem(forKey: key)!
        
        XCTAssertEqual(databaseItem.compositePrimaryKey.sortKey, retrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(databaseItem.rowValue.firstly, retrievedItem.rowValue.firstly)
        XCTAssertEqual(databaseItem.rowValue.secondly, retrievedItem.rowValue.secondly)
        
        var passCount = 0
        do {
            try client.updateItemConditionallyAtKey(key) { (payload: TestTypeA) in
                if passCount < 5 {
                    passCount += 1
                    return TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
                } else {
                    // fail before the retry limit with a custom error
                    throw TestError.everythingIsWrong
                }
            }
            
            XCTFail("Expected everythingIsWrong error not thrown.")
        } catch TestError.everythingIsWrong {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: DefaultIdentityTypedDatabaseItem<TestTypeA> = try! client.getItem(forKey: key)!
        
        XCTAssertEqual("sortId", secondRetrievedItem.compositePrimaryKey.sortKey)
        // Check the item hasn't been updated
        XCTAssertEqual("firstly", secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual("secondly", secondRetrievedItem.rowValue.secondly)
    }
    
    func testUpdateItemConditionallyAtKeyWithUnknownItem() {
        let client = InMemoryDynamoClient()
        
        let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId",
                                                     sortKey: "sortId")
        
        do {
            try client.updateItemConditionallyAtKey(key) { payload in
                return TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
            }
            
            XCTFail("Expected concurrency error not thrown.")
        } catch SwiftDynamoError.conditionalCheckFailed {
            // expected error thrown
        } catch {
            XCTFail("Unexpected error thrown: \(error).")
        }
        
        let secondRetrievedItem: DefaultIdentityTypedDatabaseItem<TestTypeA>? = try! client.getItem(forKey: key)
        
        XCTAssertNil(secondRetrievedItem)
    }
    
    static var allTests = [
        ("testUpdateItemConditionallyAtKey", testUpdateItemConditionallyAtKey),
        ("testUpdateItemConditionallyAtKeyWithAcceptableConcurrency", testUpdateItemConditionallyAtKeyWithAcceptableConcurrency),
        ("testUpdateItemConditionallyAtKeyWithUnacceptableConcurrency", testUpdateItemConditionallyAtKeyWithUnacceptableConcurrency),
        ("testUpdateItemConditionallyAtKeyWithFailingUpdate", testUpdateItemConditionallyAtKeyWithFailingUpdate),
        ("testUpdateItemConditionallyAtKeyWithUnknownItem", testUpdateItemConditionallyAtKeyWithUnknownItem),
    ]
}
