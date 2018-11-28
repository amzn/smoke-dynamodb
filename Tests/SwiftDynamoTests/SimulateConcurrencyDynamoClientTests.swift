//
//  SimulateConcurrencyDynamoClientTests.swift
//  SwiftDynamo
//

import XCTest
@testable import SwiftDynamo

private typealias DatabaseRowType = DefaultIdentityTypedDatabaseItem<TestTypeA>
private typealias QueryRowType = DefaultIdentityPolymorphicDatabaseItem<ExpectedTypes>

struct ExpectedTypes: PossibleItemTypes {
    static var types: [Codable.Type] = [TestTypeA.self]
}

class SimulateConcurrencyDynamoClientTests: XCTestCase {
    
    func testSimulateConcurrencyOnInsert() {
        let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = DefaultIdentityTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let client = SimulateConcurrencyDynamoClient(wrappedDynamoClient: InMemoryDynamoClient(),
                                                     simulateConcurrencyModifications: 5)
        
        XCTAssertThrowsError(try client.insertItem(databaseItem))
    }

    fileprivate func verifyWithUpdate(client: SimulateConcurrencyDynamoClient,
                                      databaseItem: TypedDatabaseItem<DefaultDynamoRowIdentity, TestTypeA>,
                                      key: DefaultIdentityCompositePrimaryKey,
                                      expectedFailureCount: Int) throws {
        try client.insertItem(databaseItem)
        var errorCount = 0
        
        for _ in 0..<10 {
            guard let item: DatabaseRowType = try! client.getItem(forKey: key) else {
                return XCTFail("Expected to retrieve item and there was none")
            }
            
            do {
                try client.updateItem(newItem: item.createUpdatedItem(withValue: item.rowValue), existingItem: item)
            } catch {
                errorCount += 1
            }
        }
        
        // should fail the expected number of times
        XCTAssertEqual(expectedFailureCount, errorCount)
        
        try client.deleteItem(forKey: key)
        
        let nowDeletedItem: DatabaseRowType? = try! client.getItem(forKey: key)
        XCTAssertNil(nowDeletedItem)
    }
    
    func testSimulateConcurrencyWithUpdate() {
        let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = DefaultIdentityTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let client = SimulateConcurrencyDynamoClient(wrappedDynamoClient: InMemoryDynamoClient(),
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)
        
        XCTAssertNoThrow(try verifyWithUpdate(client: client, databaseItem: databaseItem, key: key, expectedFailureCount: 5))
    }
    
    func testSimulateWithNoConcurrency() {
        let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = DefaultIdentityTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let client = SimulateConcurrencyDynamoClient(wrappedDynamoClient: InMemoryDynamoClient(),
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false,
                                                     simulateOnUpdateItem: false)
        
        XCTAssertNoThrow(try verifyWithUpdate(client: client, databaseItem: databaseItem, key: key, expectedFailureCount: 0))
    }
    
    func testSimulateConcurrencyWithQuery() {
        let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = DefaultIdentityTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let client = SimulateConcurrencyDynamoClient(wrappedDynamoClient: InMemoryDynamoClient(),
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)
        
        XCTAssertNoThrow(try client.insertItem(databaseItem))
        var errorCount = 0
        
        for _ in 0..<10 {
            let query: [QueryRowType] = try! client.query(forPartitionKey: "partitionId",
                                                      sortKeyCondition: .equals("sortId"))
            
            guard query.count == 1, let firstValue = query[0].rowValue as? TestTypeA else {
                return XCTFail("Expected to retrieve item and there wasn't the correct number or type.")
            }
            
            let firstQuery = query[0]
            let existingItem = DatabaseRowType(compositePrimaryKey: firstQuery.compositePrimaryKey,
                                               createDate: firstQuery.createDate,
                                               rowStatus: firstQuery.rowStatus,
                                               rowValue: firstQuery.rowValue as! TestTypeA)
            let item: DatabaseRowType
            
            do {
                item = try firstQuery.createUpdatedItem(withValue: firstValue)
            } catch {
                return XCTFail("Unexpectedly failed to convert types \(error)")
            }
            
            do {
                try client.updateItem(newItem: item, existingItem: existingItem)
            } catch {
                errorCount += 1
            }
        }
        
        // should only fail five times
        XCTAssertEqual(5, errorCount)
        
        XCTAssertNoThrow(try client.deleteItem(forKey: key))
        
        let nowDeletedItem: DatabaseRowType? = try! client.getItem(forKey: key)
        XCTAssertNil(nowDeletedItem)
    }
    
    func testSimulateClobberConcurrencyWithGet() throws {
        let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = DefaultIdentityTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let wrappedClient = InMemoryDynamoClient()
        let client = SimulateConcurrencyDynamoClient(wrappedDynamoClient: wrappedClient,
                                                     simulateConcurrencyModifications: 5)
        
        XCTAssertNoThrow(try wrappedClient.insertItem(databaseItem))
        
        for _ in 0..<10 {
            guard let item: DatabaseRowType = try! client.getItem(forKey: key) else {
                return XCTFail("Expected to retrieve item and there was none")
            }
            
            XCTAssertEqual(databaseItem.rowStatus.rowVersion, item.rowStatus.rowVersion)
            
            try wrappedClient.updateItem(newItem: item.createUpdatedItem(withValue: item.rowValue), existingItem: item)
            XCTAssertNoThrow(try client.clobberItem(item))
        }
        
        XCTAssertNoThrow(try client.deleteItem(forKey: key))
        
        let nowDeletedItem: DatabaseRowType? = try! client.getItem(forKey: key)
        XCTAssertNil(nowDeletedItem)
    }
  
    static var allTests = [
        ("testSimulateConcurrencyOnInsert", testSimulateConcurrencyOnInsert),
        ("testSimulateConcurrencyWithUpdate", testSimulateConcurrencyWithUpdate),
        ("testSimulateWithNoConcurrency", testSimulateWithNoConcurrency),
        ("testSimulateConcurrencyWithQuery", testSimulateConcurrencyWithQuery),
        ("testSimulateClobberConcurrencyWithGet", testSimulateClobberConcurrencyWithGet)
    ]
}
