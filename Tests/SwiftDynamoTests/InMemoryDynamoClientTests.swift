//
//  InMemoryDynamoClientTests.swift
//  SwiftDynamo
//

import XCTest
@testable import SwiftDynamo

struct TestCodableTypes: PossibleItemTypes {
    public static var types: [Codable.Type] = [TestTypeA.self]
}

class InMemoryDynamoClientTests: XCTestCase {
    
    func testInsertAndUpdate() {
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
        
        let updatedPayload = TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
        let updatedDatabaseItem = retrievedItem.createUpdatedItem(withValue: updatedPayload)
        XCTAssertNoThrow(try client.updateItem(newItem: updatedDatabaseItem, existingItem: retrievedItem))
        
        let secondRetrievedItem: DefaultIdentityTypedDatabaseItem<TestTypeA> = try! client.getItem(forKey: key)!
        
        XCTAssertEqual(updatedDatabaseItem.compositePrimaryKey.sortKey, secondRetrievedItem.compositePrimaryKey.sortKey)
        XCTAssertEqual(updatedDatabaseItem.rowValue.firstly, secondRetrievedItem.rowValue.firstly)
        XCTAssertEqual(updatedDatabaseItem.rowValue.secondly, secondRetrievedItem.rowValue.secondly)
    }
    
    func testDoubleInsert() {
        let client = InMemoryDynamoClient()
        
        let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId",
                                                        sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let databaseItem = DefaultIdentityTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        XCTAssertNoThrow(try client.insertItem(databaseItem))
        
        do {
           try client.insertItem(databaseItem)
            XCTFail()
        } catch SwiftDynamoError.conditionalCheckFailed(_) {
            // expected error
        } catch {
            XCTFail("Unexpected error \(error)")
        }
    }
    
    func testUpdateWithoutInsert() {
        let client = InMemoryDynamoClient()
        
        let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId",
                                                        sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let updatedPayload = TestTypeA(firstly: "firstlyX2", secondly: "secondlyX2")
        let databaseItem = DefaultIdentityTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        do {
            try client.updateItem(newItem: databaseItem.createUpdatedItem(withValue: updatedPayload),
                                  existingItem: databaseItem)
            XCTFail()
        } catch SwiftDynamoError.conditionalCheckFailed(_) {
            // expected error
        } catch {
            XCTFail("Unexpected error \(error)")
        }
    }
    
    func testPaginatedQuery() {
        let client = InMemoryDynamoClient()
        
        var items: [DefaultIdentityTypedDatabaseItem<TestTypeA>] = []
        
        // add to the database a lot of items - a number that isn't a multiple of the pagination page size
        for index in 0..<1376 {
            let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId",
                                                        sortKey: "sortId_\(index)")
            let payload = TestTypeA(firstly: "firstly_\(index)", secondly: "secondly_\(index)")
            let databaseItem = DefaultIdentityTypedDatabaseItem.newItem(withKey: key, andValue: payload)
            
            XCTAssertNoThrow(try client.insertItem(databaseItem))
            items.append(databaseItem)
        }
        
        var retrievedItems: [DefaultIdentityPolymorphicDatabaseItem<TestCodableTypes>] = []
        
        var exclusiveStartKey: String?
        
        // get everything back from the database
        while true {
            let paginatedItems: ([DefaultIdentityPolymorphicDatabaseItem<TestCodableTypes>], String?) =
                try! client.query(forPartitionKey: "partitionId",
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
  
    static var allTests = [
        ("testInsertAndUpdate", testInsertAndUpdate),
        ("testDoubleInsert", testDoubleInsert),
        ("testUpdateWithoutInsert", testUpdateWithoutInsert),
        ("testPaginatedQuery", testPaginatedQuery),
    ]
}
