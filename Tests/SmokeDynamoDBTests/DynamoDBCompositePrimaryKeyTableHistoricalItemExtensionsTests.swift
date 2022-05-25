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
//  DynamoDBCompositePrimaryKeyTableHistoricalItemExtensionsTests.swift
//      DynamoDB Historical Client Extension Tests
//  SmokeDynamoDBTests
//

import XCTest
@testable import SmokeDynamoDB
import SmokeHTTPClient
import DynamoDBModel

private typealias DatabaseRowType =
    TypedDatabaseItem<StandardPrimaryKeyAttributes, RowWithItemVersion<TestTypeA>>

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

let dKey = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
let dPayload = TestTypeA(firstly: "firstly", secondly: "secondly")
let dVersionedPayload = RowWithItemVersion.newItem(withValue: dPayload)

let defaultItem = StandardTypedDatabaseItem.newItem(withKey: dKey, andValue: dVersionedPayload)

private let testPrimaryItemProvider = primaryItemProviderProvider(defaultItem)

fileprivate func testHistoricalItemProvider(_ item: DatabaseRowType) -> DatabaseRowType {

    return DatabaseRowType.newItem(withKey: StandardCompositePrimaryKey(partitionKey: "historical.\(item.compositePrimaryKey.partitionKey)",
                                                                               sortKey: "v0000\(item.rowValue.itemVersion).\(item.compositePrimaryKey.sortKey)"),
                                   andValue: item.rowValue)
}

class CompositePrimaryKeyDynamoDBHistoricalClientTests: XCTestCase {

    func testInsertItemSuccess() async throws {

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = testHistoricalItemProvider(databaseItem)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        try await table.insertItemWithHistoricalRow(primaryItem: databaseItem, historicalItem: historicalItem)
        let inserted : DatabaseRowType = try await table.getItem(forKey: databaseItem.compositePrimaryKey)!
        XCTAssertEqual(inserted.compositePrimaryKey.partitionKey, databaseItem.compositePrimaryKey.partitionKey)
        XCTAssertEqual(inserted.compositePrimaryKey.sortKey, databaseItem.compositePrimaryKey.sortKey)
    }

    func testInsertItemFailure() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = testHistoricalItemProvider(databaseItem)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        try await table.insertItemWithHistoricalRow(primaryItem: databaseItem, historicalItem: historicalItem)

        do {
            // Second insert will fail.
            try await table.insertItemWithHistoricalRow(primaryItem: databaseItem, historicalItem: historicalItem)
        } catch SmokeDynamoDBError.conditionalCheckFailed {
            // Success
        } catch {
             return XCTFail("Unexpected exception")
        }
    }

    func testUpdateItemSuccess() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = testHistoricalItemProvider(databaseItem)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        try await table.insertItemWithHistoricalRow(primaryItem: databaseItem, historicalItem: historicalItem)

        let updatedPayload = versionedPayload.createUpdatedItem(withValue: versionedPayload.rowValue)
        let updatedItem = databaseItem.createUpdatedItem(withValue: updatedPayload)
        try await table.updateItemWithHistoricalRow(primaryItem: updatedItem, existingItem: databaseItem, historicalItem: testHistoricalItemProvider(updatedItem))

        let inserted : DatabaseRowType = try await table.getItem(forKey: key)!
        XCTAssertEqual(inserted.compositePrimaryKey.partitionKey, databaseItem.compositePrimaryKey.partitionKey)
        XCTAssertEqual(inserted.compositePrimaryKey.sortKey, databaseItem.compositePrimaryKey.sortKey)
    }

    func testUpdateItemFailure() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = testHistoricalItemProvider(databaseItem)
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        try await table.insertItemWithHistoricalRow(primaryItem: databaseItem, historicalItem: historicalItem)

        let updatedPayload = versionedPayload.createUpdatedItem(withValue: versionedPayload.rowValue)
        let updatedItem = databaseItem.createUpdatedItem(withValue: updatedPayload)
        try await table.updateItemWithHistoricalRow(primaryItem: updatedItem, existingItem: databaseItem,
                                                    historicalItem: testHistoricalItemProvider(updatedItem))

        do {
            // Second update will fail.
            try await table.updateItemWithHistoricalRow(primaryItem: databaseItem.createUpdatedItem(withValue: versionedPayload),
                                                        existingItem: databaseItem, historicalItem: historicalItem)
        } catch SmokeDynamoDBError.conditionalCheckFailed {
            // Success
        } catch {
            return XCTFail("Unexpected exception")
        }
    }

    func testClobberItemSuccess() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let databaseItem = testPrimaryItemProvider(nil)

        try await table.clobberItemWithHistoricalRow(primaryItemProvider: testPrimaryItemProvider, historicalItemProvider: testHistoricalItemProvider)
        let inserted : DatabaseRowType = (try await table.getItem(forKey: databaseItem.compositePrimaryKey))!
        XCTAssertEqual(inserted.compositePrimaryKey.partitionKey, databaseItem.compositePrimaryKey.partitionKey)
        XCTAssertEqual(inserted.compositePrimaryKey.sortKey, databaseItem.compositePrimaryKey.sortKey)
    }

    func testClobberItemSuccessAfterRetry() async throws {
        let databaseItem = testPrimaryItemProvider(nil)
        
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 5)

        try await table.clobberItemWithHistoricalRow(primaryItemProvider: testPrimaryItemProvider,
                                               historicalItemProvider: testHistoricalItemProvider)
        let inserted : DatabaseRowType = try await table.getItem(forKey: databaseItem.compositePrimaryKey)!
        XCTAssertTrue(inserted.rowStatus.rowVersion > databaseItem.rowStatus.rowVersion)
    }

    func testClobberItemFailure() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 12)

        do {
            try await table.clobberItemWithHistoricalRow(primaryItemProvider: testPrimaryItemProvider,
                                                         historicalItemProvider: testHistoricalItemProvider, withRetries: 9)

            XCTFail("Expected error not thrown.")
        } catch SmokeDynamoDBError.concurrencyError {
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
    
    private func getConditionalUpdatePrimaryItemProviderAsync() -> ((DatabaseRowType) async throws -> DatabaseRowType) {
        func provider(existingItem: DatabaseRowType) async throws -> DatabaseRowType {
            let rowVersion = existingItem.rowStatus.rowVersion
            let dPayload = TestTypeA(firstly: "firstly_\(rowVersion)", secondly: "secondly_\(rowVersion)")

            return try existingItem.createUpdatedRowWithItemVersion(
                withValue: dPayload,
                conditionalStatusVersion: nil)
        }
        
        return provider
    }

    private let historicalCompositePrimaryKey = StandardCompositePrimaryKey(partitionKey: "historicalPartitionKey",
                                                                                   sortKey: "historicalSortKey")
    private func conditionalUpdateHistoricalItemProvider(updatedItem: DatabaseRowType) -> DatabaseRowType {
        // create an item for the history partition
        return TypedDatabaseItem.newItem(withKey: historicalCompositePrimaryKey,
                                         andValue: updatedItem.rowValue)
    }

    func testConditionallyUpdateItemWithHistoricalRow() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let databaseItem = testPrimaryItemProvider(nil)
        try await table.insertItem(databaseItem)

        let updated = try await table.conditionallyUpdateItemWithHistoricalRow(
            compositePrimaryKey: dKey,
            primaryItemProvider: conditionalUpdatePrimaryItemProvider,
            historicalItemProvider: conditionalUpdateHistoricalItemProvider)

        let inserted: DatabaseRowType = (try await table.getItem(forKey: databaseItem.compositePrimaryKey))!
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly_1")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly_1")
        XCTAssertEqual(inserted.rowStatus.rowVersion, 2)
        XCTAssertEqual(inserted.rowValue.itemVersion, 2)

        XCTAssertEqual(updated.rowValue.rowValue.firstly, inserted.rowValue.rowValue.firstly)
        XCTAssertEqual(updated.rowValue.rowValue.secondly, inserted.rowValue.rowValue.secondly)
        XCTAssertEqual(updated.rowStatus.rowVersion, inserted.rowStatus.rowVersion)
        XCTAssertEqual(updated.rowValue.itemVersion, inserted.rowValue.itemVersion)

        let historicalInserted: DatabaseRowType = (try await table.getItem(forKey: historicalCompositePrimaryKey))!
        XCTAssertEqual(historicalInserted.rowValue.rowValue.firstly, "firstly_1")
        XCTAssertEqual(historicalInserted.rowValue.rowValue.secondly, "secondly_1")
        XCTAssertEqual(historicalInserted.rowStatus.rowVersion, 1)
        XCTAssertEqual(historicalInserted.rowValue.itemVersion, 2)
    }
    
    func testConditionallyUpdateItemWithHistoricalRowWithAsyncProvider() async throws {
        let table = InMemoryDynamoDBCompositePrimaryKeyTable()

        let databaseItem = testPrimaryItemProvider(nil)
        try await table.insertItem(databaseItem)

        let asyncConditionalUpdatePrimaryItemProvider = getConditionalUpdatePrimaryItemProviderAsync()
        let updated = try await table.conditionallyUpdateItemWithHistoricalRow(
            compositePrimaryKey: dKey,
            primaryItemProvider: asyncConditionalUpdatePrimaryItemProvider,
            historicalItemProvider: conditionalUpdateHistoricalItemProvider)

        let inserted: DatabaseRowType = (try await table.getItem(forKey: databaseItem.compositePrimaryKey))!
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly_1")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly_1")
        XCTAssertEqual(inserted.rowStatus.rowVersion, 2)
        XCTAssertEqual(inserted.rowValue.itemVersion, 2)

        XCTAssertEqual(updated.rowValue.rowValue.firstly, inserted.rowValue.rowValue.firstly)
        XCTAssertEqual(updated.rowValue.rowValue.secondly, inserted.rowValue.rowValue.secondly)
        XCTAssertEqual(updated.rowStatus.rowVersion, inserted.rowStatus.rowVersion)
        XCTAssertEqual(updated.rowValue.itemVersion, inserted.rowValue.itemVersion)

        let historicalInserted: DatabaseRowType = (try await table.getItem(forKey: historicalCompositePrimaryKey))!
        XCTAssertEqual(historicalInserted.rowValue.rowValue.firstly, "firstly_1")
        XCTAssertEqual(historicalInserted.rowValue.rowValue.secondly, "secondly_1")
        XCTAssertEqual(historicalInserted.rowStatus.rowVersion, 1)
        XCTAssertEqual(historicalInserted.rowValue.itemVersion, 2)
    }

    func testConditionallyUpdateItemWithHistoricalRowAcceptableConcurrency() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)

        let databaseItem = testPrimaryItemProvider(nil)
        try await table.insertItem(databaseItem)

        let updated = try await table.conditionallyUpdateItemWithHistoricalRow(
            compositePrimaryKey: dKey,
            primaryItemProvider: conditionalUpdatePrimaryItemProvider,
            historicalItemProvider: conditionalUpdateHistoricalItemProvider)

        let inserted: DatabaseRowType = (try await table.getItem(forKey: databaseItem.compositePrimaryKey))!
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly_6")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly_6")
        // the row version has been updated by the SimulateConcurrencyDynamoDBCompositePrimaryKeyTable an
        // additional 5 fives, item updated by conditionallyUpdateItemWithHistoricalRow
        // (which increments itemVersion) only once
        XCTAssertEqual(inserted.rowStatus.rowVersion, 7)
        XCTAssertEqual(inserted.rowValue.itemVersion, 2)

        XCTAssertEqual(updated.rowValue.rowValue.firstly, inserted.rowValue.rowValue.firstly)
        XCTAssertEqual(updated.rowValue.rowValue.secondly, inserted.rowValue.rowValue.secondly)
        XCTAssertEqual(updated.rowStatus.rowVersion, inserted.rowStatus.rowVersion)
        XCTAssertEqual(updated.rowValue.itemVersion, inserted.rowValue.itemVersion)
    }
    
    func testConditionallyUpdateItemWithHistoricalRowAcceptableConcurrencyWithAsyncProvider() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)

        let databaseItem = testPrimaryItemProvider(nil)
        try await table.insertItem(databaseItem)

        let asyncConditionalUpdatePrimaryItemProvider = getConditionalUpdatePrimaryItemProviderAsync()
        let updated = try await table.conditionallyUpdateItemWithHistoricalRow(
            compositePrimaryKey: dKey,
            primaryItemProvider: asyncConditionalUpdatePrimaryItemProvider,
            historicalItemProvider: conditionalUpdateHistoricalItemProvider)

        let inserted: DatabaseRowType = (try await table.getItem(forKey: databaseItem.compositePrimaryKey))!
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly_6")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly_6")
        // the row version has been updated by the SimulateConcurrencyDynamoDBCompositePrimaryKeyTable an
        // additional 5 fives, item updated by conditionallyUpdateItemWithHistoricalRow
        // (which increments itemVersion) only once
        XCTAssertEqual(inserted.rowStatus.rowVersion, 7)
        XCTAssertEqual(inserted.rowValue.itemVersion, 2)

        XCTAssertEqual(updated.rowValue.rowValue.firstly, inserted.rowValue.rowValue.firstly)
        XCTAssertEqual(updated.rowValue.rowValue.secondly, inserted.rowValue.rowValue.secondly)
        XCTAssertEqual(updated.rowStatus.rowVersion, inserted.rowStatus.rowVersion)
        XCTAssertEqual(updated.rowValue.itemVersion, inserted.rowValue.itemVersion)
    }

    func testConditionallyUpdateItemWithHistoricalRowUnacceptableConcurrency() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 50,
                                                     simulateOnInsertItem: false)

        let databaseItem = testPrimaryItemProvider(nil)
        try await table.insertItem(databaseItem)

        do {
            _ = try await table.conditionallyUpdateItemWithHistoricalRow(
                compositePrimaryKey: dKey,
                primaryItemProvider: conditionalUpdatePrimaryItemProvider,
                historicalItemProvider: conditionalUpdateHistoricalItemProvider)

            XCTFail("Expected error not thrown.")
        } catch SmokeDynamoDBError.concurrencyError {
            // Success
        } catch {
            return XCTFail("Unexpected exception: \(error)")
        }

        let inserted: DatabaseRowType = (try await table.getItem(forKey: databaseItem.compositePrimaryKey))!
        // confirm row has not been updated by conditionallyUpdateItemWithHistoricalRow
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly")
        XCTAssertEqual(inserted.rowStatus.rowVersion, 11)
        XCTAssertEqual(inserted.rowValue.itemVersion, 1)
    }
    
    func testConditionallyUpdateItemWithHistoricalRowUnacceptableConcurrencyWithAsyncProvider() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 50,
                                                     simulateOnInsertItem: false)

        let databaseItem = testPrimaryItemProvider(nil)
        try await table.insertItem(databaseItem)

        let asyncConditionalUpdatePrimaryItemProvider = getConditionalUpdatePrimaryItemProviderAsync()
        do {
            _ = try await table.conditionallyUpdateItemWithHistoricalRow(
                compositePrimaryKey: dKey,
                primaryItemProvider: asyncConditionalUpdatePrimaryItemProvider,
                historicalItemProvider: conditionalUpdateHistoricalItemProvider)

            XCTFail("Expected error not thrown.")
        } catch SmokeDynamoDBError.concurrencyError {
            // Success
        } catch {
            return XCTFail("Unexpected exception: \(error)")
        }

        let inserted: DatabaseRowType = (try await table.getItem(forKey: databaseItem.compositePrimaryKey))!
        // confirm row has not been updated by conditionallyUpdateItemWithHistoricalRow
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly")
        XCTAssertEqual(inserted.rowStatus.rowVersion, 11)
        XCTAssertEqual(inserted.rowValue.itemVersion, 1)
    }

    enum TestError: Error {
        case everythingIsWrong
    }

    func testConditionallyUpdateItemWithHistoricalRowPrimaryItemProviderError() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)

        let databaseItem = testPrimaryItemProvider(nil)
        try await table.insertItem(databaseItem)

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
            _ = try await table.conditionallyUpdateItemWithHistoricalRow(
                compositePrimaryKey: dKey,
                primaryItemProvider: primaryItemProvider,
                historicalItemProvider: conditionalUpdateHistoricalItemProvider)

            XCTFail("Expected error not thrown.")
        } catch TestError.everythingIsWrong {
            // Success
        } catch {
            return XCTFail("Unexpected exception: \(error)")
        }

        let inserted: DatabaseRowType = (try await table.getItem(forKey: databaseItem.compositePrimaryKey))!
        // confirm row has not been updated by conditionallyUpdateItemWithHistoricalRow
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly")
        XCTAssertEqual(inserted.rowStatus.rowVersion, 6)
        XCTAssertEqual(inserted.rowValue.itemVersion, 1)
    }
    
    func testConditionallyUpdateItemWithHistoricalRowPrimaryItemProviderErrorWithAsyncProvider() async throws {
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)

        let databaseItem = testPrimaryItemProvider(nil)
        try await table.insertItem(databaseItem)

        var providerCount = 0
        func primaryItemProvider(existingItem: DatabaseRowType) async throws -> DatabaseRowType {
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
            _ = try await table.conditionallyUpdateItemWithHistoricalRow(
                compositePrimaryKey: dKey,
                primaryItemProvider: primaryItemProvider,
                historicalItemProvider: conditionalUpdateHistoricalItemProvider)

            XCTFail("Expected error not thrown.")
        } catch TestError.everythingIsWrong {
            // Success
        } catch {
            return XCTFail("Unexpected exception: \(error)")
        }

        let inserted: DatabaseRowType = (try await table.getItem(forKey: databaseItem.compositePrimaryKey))!
        // confirm row has not been updated by conditionallyUpdateItemWithHistoricalRow
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly")
        XCTAssertEqual(inserted.rowStatus.rowVersion, 6)
        XCTAssertEqual(inserted.rowValue.itemVersion, 1)
    }
}
