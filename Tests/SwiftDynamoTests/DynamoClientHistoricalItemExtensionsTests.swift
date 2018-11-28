//
//  DynamoClientHistoricalItemExtensionsTests.swift
//  Dynamo Historical Client Extension Tests
//
//  Created by Van Pelt, Samuel on 3/8/18.
//

import XCTest
@testable import SwiftDynamo

private typealias DatabaseRowType =
    TypedDatabaseItem<DefaultDynamoRowIdentity, RowWithItemVersion<TestTypeA>>

/**
 * For these tests, a primary item Provider should always return a default value for nil arguments. The Provider Provider requires a non-nil default in order to initialize a Provider.
 */
fileprivate func primaryItemProviderProvider(_ defaultItem: DatabaseRowType) ->
                (DatabaseRowType?) -> DatabaseRowType {
    func primaryItemProvider(_ item: DatabaseRowType?) ->
                    DatabaseRowType {
        guard let item = item else {
            return defaultItem
        }
        
        let newItemRowValue = item.rowValue.createUpdatedItem(withVersion: item.rowValue.itemVersion + 1,
                                                              withValue: defaultItem.rowValue.rowValue)
        return item.createUpdatedItem(withValue: newItemRowValue)
    }
    
    return primaryItemProvider
}

let dKey = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
let dPayload = TestTypeA(firstly: "firstly", secondly: "secondly")
let dVersionedPayload = RowWithItemVersion.newItem(withValue: dPayload)

let defaultItem = DefaultIdentityTypedDatabaseItem.newItem(withKey: dKey, andValue: dVersionedPayload)

private let testPrimaryItemProvider = primaryItemProviderProvider(defaultItem)

fileprivate func testHistoricalItemProvider(_ item: DatabaseRowType) -> DatabaseRowType {
    
    return DatabaseRowType.newItem(withKey: DefaultIdentityCompositePrimaryKey(partitionKey: "historical.\(item.compositePrimaryKey.partitionKey)",
                                                                               sortKey: "v0000\(item.rowValue.itemVersion).\(item.compositePrimaryKey.sortKey)"),
                                   andValue: item.rowValue)
}

class DynamoHistoricalClientTests: XCTestCase {

    func testInsertItemSuccess() throws {
        
        let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)
        
        let databaseItem = DefaultIdentityTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = testHistoricalItemProvider(databaseItem)
        
        let client = InMemoryDynamoClient()
        
        try client.insertItemWithHistoricalRow(primaryItem: databaseItem, historicalItem: historicalItem)
        let inserted : DatabaseRowType = try client.getItem(forKey: databaseItem.compositePrimaryKey)!
        XCTAssertEqual(inserted.compositePrimaryKey.partitionKey, databaseItem.compositePrimaryKey.partitionKey)
        XCTAssertEqual(inserted.compositePrimaryKey.sortKey, databaseItem.compositePrimaryKey.sortKey)
    }
    
    func testInsertItemFailure() throws {
        let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)
        
        let databaseItem = DefaultIdentityTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = testHistoricalItemProvider(databaseItem)
        
        let client = InMemoryDynamoClient()
        
        try client.insertItemWithHistoricalRow(primaryItem: databaseItem, historicalItem: historicalItem)
        
        do {
            // Second insert will fail.
            try client.insertItemWithHistoricalRow(primaryItem: databaseItem, historicalItem: historicalItem)
        } catch SwiftDynamoError.conditionalCheckFailed(_) {
            // Success
        } catch {
             return XCTFail("Unexpected exception")
        }
        
    }
    
    func testUpdateItemSuccess() throws {
        let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)
        
        let databaseItem = DefaultIdentityTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = testHistoricalItemProvider(databaseItem)
        
        let client = InMemoryDynamoClient()
        
        try client.insertItemWithHistoricalRow(primaryItem: databaseItem, historicalItem: historicalItem)
        
        let updatedPayload = versionedPayload.createUpdatedItem(withValue: versionedPayload.rowValue)
        let updatedItem = databaseItem.createUpdatedItem(withValue: updatedPayload)
        try client.updateItemWithHistoricalRow(primaryItem: updatedItem, existingItem: databaseItem, historicalItem: testHistoricalItemProvider(updatedItem))

        let inserted : DatabaseRowType = try client.getItem(forKey: key)!
        XCTAssertEqual(inserted.compositePrimaryKey.partitionKey, databaseItem.compositePrimaryKey.partitionKey)
        XCTAssertEqual(inserted.compositePrimaryKey.sortKey, databaseItem.compositePrimaryKey.sortKey)
        
    }
    
    func testUpdateItemFailure() throws {
        let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)
        
        let databaseItem = DefaultIdentityTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = testHistoricalItemProvider(databaseItem)
        
        let client = InMemoryDynamoClient()
        
        try client.insertItemWithHistoricalRow(primaryItem: databaseItem, historicalItem: historicalItem)
        
        let updatedPayload = versionedPayload.createUpdatedItem(withValue: versionedPayload.rowValue)
        let updatedItem = databaseItem.createUpdatedItem(withValue: updatedPayload)
        try client.updateItemWithHistoricalRow(primaryItem: updatedItem, existingItem: databaseItem, historicalItem: testHistoricalItemProvider(updatedItem))
        
        do {
            // Second update will fail.
            try client.updateItemWithHistoricalRow(primaryItem: databaseItem.createUpdatedItem(withValue: versionedPayload), existingItem: databaseItem, historicalItem: historicalItem)
        } catch SwiftDynamoError.conditionalCheckFailed(_) {
            // Success
        } catch {
            return XCTFail("Unexpected exception")
        }
    }
    
    func testClobberItemSuccess() throws {
        let client = InMemoryDynamoClient()
        
        let databaseItem = testPrimaryItemProvider(nil)
        
        try client.clobberItemWithHistoricalRow(primaryItemProvider: testPrimaryItemProvider, historicalItemProvider: testHistoricalItemProvider)
        let inserted : DatabaseRowType = (try client.getItem(forKey: databaseItem.compositePrimaryKey))!
        XCTAssertEqual(inserted.compositePrimaryKey.partitionKey, databaseItem.compositePrimaryKey.partitionKey)
        XCTAssertEqual(inserted.compositePrimaryKey.sortKey, databaseItem.compositePrimaryKey.sortKey)
    }
    
    func testClobberItemSuccessAfterRetry() throws {
        
        let databaseItem = testPrimaryItemProvider(nil)
        
        let wrappedClient = InMemoryDynamoClient()
        let client = SimulateConcurrencyDynamoClient(wrappedDynamoClient: wrappedClient,
                                                     simulateConcurrencyModifications: 5)
        
        try client.clobberItemWithHistoricalRow(primaryItemProvider: testPrimaryItemProvider, historicalItemProvider: testHistoricalItemProvider)
        let inserted : DatabaseRowType = try client.getItem(forKey: databaseItem.compositePrimaryKey)!
        XCTAssertTrue(inserted.rowStatus.rowVersion > databaseItem.rowStatus.rowVersion)
        
        
    }
    
    func testClobberItemFailure() throws {
        
        let wrappedClient = InMemoryDynamoClient()
        let client = SimulateConcurrencyDynamoClient(wrappedDynamoClient: wrappedClient,
                                                     simulateConcurrencyModifications: 12)
        
        do {
            try client.clobberItemWithHistoricalRow(primaryItemProvider: testPrimaryItemProvider, historicalItemProvider: testHistoricalItemProvider, withRetries: 9)
            
            XCTFail("Expected error not thrown.")
        } catch SwiftDynamoError.concurrencyError(_) {
            // Success
        } catch {
            return XCTFail("Unexpected exception: \(error)")
        }
       
    }
    
    private func conditionalUpdatePrimaryItemProvider(existingItem: DatabaseRowType) throws -> DatabaseRowType {
        let rowVersion = existingItem.rowStatus.rowVersion
        let dPayload = TestTypeA(firstly: "firstly_\(rowVersion)", secondly: "secondly_\(rowVersion)")
    
        return try existingItem.createUpdatedRowWithItemVersion(
            withValue: dPayload,
            conditionalStatusVersion: nil)
    }
    
    private let historicalCompositePrimaryKey = DefaultIdentityCompositePrimaryKey(partitionKey: "historicalPartitionKey",
                                                                                   sortKey: "historicalSortKey")
    private func conditionalUpdateHistoricalItemProvider(updatedItem: DatabaseRowType) -> DatabaseRowType {
        // create an item for the history partition
        return TypedDatabaseItem.newItem(withKey: historicalCompositePrimaryKey,
                                         andValue: updatedItem.rowValue)
    }
    
    func testConditionallyUpdateItemWithHistoricalRow() throws {
        let client = InMemoryDynamoClient()
        
        let databaseItem = testPrimaryItemProvider(nil)
        try client.insertItem(databaseItem)
        
        try client.conditionallyUpdateItemWithHistoricalRow(
            compositePrimaryKey: dKey,
            primaryItemProvider: conditionalUpdatePrimaryItemProvider,
            historicalItemProvider: conditionalUpdateHistoricalItemProvider)
        
        let inserted: DatabaseRowType = (try client.getItem(forKey: databaseItem.compositePrimaryKey))!
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly_1")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly_1")
        XCTAssertEqual(inserted.rowStatus.rowVersion, 2)
        XCTAssertEqual(inserted.rowValue.itemVersion, 2)
        
        let historicalInserted: DatabaseRowType = (try client.getItem(forKey: historicalCompositePrimaryKey))!
        XCTAssertEqual(historicalInserted.rowValue.rowValue.firstly, "firstly_1")
        XCTAssertEqual(historicalInserted.rowValue.rowValue.secondly, "secondly_1")
        XCTAssertEqual(historicalInserted.rowStatus.rowVersion, 1)
        XCTAssertEqual(historicalInserted.rowValue.itemVersion, 2)
    }
    
    func testConditionallyUpdateItemWithHistoricalRowAcceptableConcurrency() throws {
        let wrappedClient = InMemoryDynamoClient()
        let client = SimulateConcurrencyDynamoClient(wrappedDynamoClient: wrappedClient,
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)
        
        let databaseItem = testPrimaryItemProvider(nil)
        try client.insertItem(databaseItem)
        
        try client.conditionallyUpdateItemWithHistoricalRow(
            compositePrimaryKey: dKey,
            primaryItemProvider: conditionalUpdatePrimaryItemProvider,
            historicalItemProvider: conditionalUpdateHistoricalItemProvider)
        
        let inserted: DatabaseRowType = (try client.getItem(forKey: databaseItem.compositePrimaryKey))!
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly_6")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly_6")
        // the row version has been updated by the SimulateConcurrencyDynamoClient an
        // additional 5 fives, item updated by conditionallyUpdateItemWithHistoricalRow
        // (which increments itemVersion) only once
        XCTAssertEqual(inserted.rowStatus.rowVersion, 7)
        XCTAssertEqual(inserted.rowValue.itemVersion, 2)
    }
    
    func testConditionallyUpdateItemWithHistoricalRowUnacceptableConcurrency() throws {
        let wrappedClient = InMemoryDynamoClient()
        let client = SimulateConcurrencyDynamoClient(wrappedDynamoClient: wrappedClient,
                                                     simulateConcurrencyModifications: 50,
                                                     simulateOnInsertItem: false)
        
        let databaseItem = testPrimaryItemProvider(nil)
        try client.insertItem(databaseItem)
        
        do {
            try client.conditionallyUpdateItemWithHistoricalRow(
                compositePrimaryKey: dKey,
                primaryItemProvider: conditionalUpdatePrimaryItemProvider,
                historicalItemProvider: conditionalUpdateHistoricalItemProvider)
            
            XCTFail("Expected error not thrown.")
        } catch SwiftDynamoError.concurrencyError(_) {
            // Success
        } catch {
            return XCTFail("Unexpected exception: \(error)")
        }
        
        let inserted: DatabaseRowType = (try client.getItem(forKey: databaseItem.compositePrimaryKey))!
        // confirm row has not been updated by conditionallyUpdateItemWithHistoricalRow
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly")
        XCTAssertEqual(inserted.rowStatus.rowVersion, 11)
        XCTAssertEqual(inserted.rowValue.itemVersion, 1)
    }
    
    enum TestError: Error {
        case everythingIsWrong
    }
    
    func testConditionallyUpdateItemWithHistoricalRowPrimaryItemProviderError() throws {
        let wrappedClient = InMemoryDynamoClient()
        let client = SimulateConcurrencyDynamoClient(wrappedDynamoClient: wrappedClient,
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)
        
        let databaseItem = testPrimaryItemProvider(nil)
        try client.insertItem(databaseItem)
        
        var providerCount = 0
        let primaryItemProvider: (DatabaseRowType) throws -> DatabaseRowType = { existingItem in
            guard providerCount < 5 else {
                throw TestError.everythingIsWrong
            }
            providerCount += 1
            
            let rowVersion = existingItem.rowStatus.rowVersion
            let dPayload = TestTypeA(firstly: "firstly_\(rowVersion)", secondly: "secondly_\(rowVersion)")
        
            return try existingItem.createUpdatedRowWithItemVersion(
                withValue: dPayload,
                conditionalStatusVersion: nil)
        }
        
        do {
            try client.conditionallyUpdateItemWithHistoricalRow(
                compositePrimaryKey: dKey,
                primaryItemProvider: primaryItemProvider,
                historicalItemProvider: conditionalUpdateHistoricalItemProvider)
            
            XCTFail("Expected error not thrown.")
        } catch TestError.everythingIsWrong {
            // Success
        } catch {
            return XCTFail("Unexpected exception: \(error)")
        }
        
        let inserted: DatabaseRowType = (try client.getItem(forKey: databaseItem.compositePrimaryKey))!
        // confirm row has not been updated by conditionallyUpdateItemWithHistoricalRow
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly")
        XCTAssertEqual(inserted.rowStatus.rowVersion, 6)
        XCTAssertEqual(inserted.rowValue.itemVersion, 1)
    }
    
    static var allTests = [
        ("testInsertItemSuccess", testInsertItemSuccess),
        ("testInsertItemFailure", testInsertItemFailure),
        ("testUpdateItemSuccess", testUpdateItemSuccess),
        ("testUpdateItemFailure", testUpdateItemFailure),
        ("testClobberItemSuccess", testClobberItemSuccess),
        ("testClobberItemFailure", testClobberItemFailure),
        ("testClobberItemSuccessAfterRetry", testClobberItemSuccessAfterRetry),
        ("testConditionallyUpdateItemWithHistoricalRow", testConditionallyUpdateItemWithHistoricalRow),
        ("testConditionallyUpdateItemWithHistoricalRowAcceptableConcurrency",
         testConditionallyUpdateItemWithHistoricalRowAcceptableConcurrency),
        ("testConditionallyUpdateItemWithHistoricalRowUnacceptableConcurrency",
         testConditionallyUpdateItemWithHistoricalRowUnacceptableConcurrency),
        ("testConditionallyUpdateItemWithHistoricalRowPrimaryItemProviderError",
         testConditionallyUpdateItemWithHistoricalRowPrimaryItemProviderError)
        
    ]
}
