// Copyright 2018-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
//  DynamoDBTableHistoricalItemExtensionsTests.swift
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

private let testPrimaryItemProvider = primaryItemProviderProvider(defaultItem)

fileprivate func testHistoricalItemProvider(_ item: DatabaseRowType) -> DatabaseRowType {

    return DatabaseRowType.newItem(withKey: StandardCompositePrimaryKey(partitionKey: "historical.\(item.compositePrimaryKey.partitionKey)",
                                                                               sortKey: "v0000\(item.rowValue.itemVersion).\(item.compositePrimaryKey.sortKey)"),
                                   andValue: item.rowValue)
}

class DynamoDBHistoricalClientTests: XCTestCase {

    func testInsertItemSuccessSync() throws {

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = testHistoricalItemProvider(databaseItem)

        let table = InMemoryDynamoDBTable()

        try table.insertItemWithHistoricalRowSync(primaryItem: databaseItem, historicalItem: historicalItem)
        let inserted : DatabaseRowType = try table.getItemSync(forKey: databaseItem.compositePrimaryKey)!
        XCTAssertEqual(inserted.compositePrimaryKey.partitionKey, databaseItem.compositePrimaryKey.partitionKey)
        XCTAssertEqual(inserted.compositePrimaryKey.sortKey, databaseItem.compositePrimaryKey.sortKey)
    }

    func testInsertItemSuccessAsync() throws {

        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = testHistoricalItemProvider(databaseItem)

        let table = InMemoryDynamoDBTable()

        var isCompleted = false
        func completionHandler(error: Error?) {
            if error != nil {
                XCTFail()
            }

            isCompleted = true
        }

        try table.insertItemWithHistoricalRowAsync(primaryItem: databaseItem, historicalItem: historicalItem,
                                                    completion: completionHandler)
        let inserted : DatabaseRowType = try table.getItemSync(forKey: databaseItem.compositePrimaryKey)!
        XCTAssertEqual(inserted.compositePrimaryKey.partitionKey, databaseItem.compositePrimaryKey.partitionKey)
        XCTAssertEqual(inserted.compositePrimaryKey.sortKey, databaseItem.compositePrimaryKey.sortKey)
        XCTAssertTrue(isCompleted)
    }

    func testInsertItemFailureSync() throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = testHistoricalItemProvider(databaseItem)

        let table = InMemoryDynamoDBTable()

        try table.insertItemWithHistoricalRowSync(primaryItem: databaseItem, historicalItem: historicalItem)

        do {
            // Second insert will fail.
            try table.insertItemWithHistoricalRowSync(primaryItem: databaseItem, historicalItem: historicalItem)
        } catch SmokeDynamoDBError.conditionalCheckFailed(_) {
            // Success
        } catch {
             return XCTFail("Unexpected exception")
        }
    }

    func testInsertItemFailureAsync() throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = testHistoricalItemProvider(databaseItem)

        let table = InMemoryDynamoDBTable()

        var isInsert1Completed = false
        func completionHandler(error: Error?) {
            if error != nil {
                XCTFail()
            }

            isInsert1Completed = true
        }

        try table.insertItemWithHistoricalRowAsync(primaryItem: databaseItem, historicalItem: historicalItem,
                                                    completion: completionHandler)

        // Second insert will fail.
        var isInsert2Completed = false
        try table.insertItemWithHistoricalRowAsync(primaryItem: databaseItem, historicalItem: historicalItem) { error in
            guard let theError = error, case SmokeDynamoDBError.conditionalCheckFailed = theError else {
                return XCTFail("Expected error not thrown")
            }

            isInsert2Completed = true
        }

        XCTAssertTrue(isInsert1Completed)
        XCTAssertTrue(isInsert2Completed)
    }

    func testUpdateItemSuccessSync() throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = testHistoricalItemProvider(databaseItem)

        let table = InMemoryDynamoDBTable()

        try table.insertItemWithHistoricalRowSync(primaryItem: databaseItem, historicalItem: historicalItem)

        let updatedPayload = versionedPayload.createUpdatedItem(withValue: versionedPayload.rowValue)
        let updatedItem = databaseItem.createUpdatedItem(withValue: updatedPayload)
        try table.updateItemWithHistoricalRowSync(primaryItem: updatedItem, existingItem: databaseItem, historicalItem: testHistoricalItemProvider(updatedItem))

        let inserted : DatabaseRowType = try table.getItemSync(forKey: key)!
        XCTAssertEqual(inserted.compositePrimaryKey.partitionKey, databaseItem.compositePrimaryKey.partitionKey)
        XCTAssertEqual(inserted.compositePrimaryKey.sortKey, databaseItem.compositePrimaryKey.sortKey)
    }

    func testUpdateItemSuccessAsync() throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = testHistoricalItemProvider(databaseItem)

        let table = InMemoryDynamoDBTable()

        var isInsertCompleted = false
        func insertCompletionHandler(error: Error?) {
            if error != nil {
                XCTFail()
            }

            isInsertCompleted = true
        }

        try table.insertItemWithHistoricalRowAsync(primaryItem: databaseItem, historicalItem: historicalItem,
                                                    completion: insertCompletionHandler)

        var isUpdateCompleted = false
        func updateCompletionHandler(error: Error?) {
            if error != nil {
                XCTFail()
            }

            isUpdateCompleted = true
        }

        let updatedPayload = versionedPayload.createUpdatedItem(withValue: versionedPayload.rowValue)
        let updatedItem = databaseItem.createUpdatedItem(withValue: updatedPayload)
        try table.updateItemWithHistoricalRowAsync(primaryItem: updatedItem, existingItem: databaseItem,
                                                    historicalItem: testHistoricalItemProvider(updatedItem),
                                                    completion: updateCompletionHandler)

        let inserted : DatabaseRowType = try table.getItemSync(forKey: key)!
        XCTAssertEqual(inserted.compositePrimaryKey.partitionKey, databaseItem.compositePrimaryKey.partitionKey)
        XCTAssertEqual(inserted.compositePrimaryKey.sortKey, databaseItem.compositePrimaryKey.sortKey)
        XCTAssertTrue(isInsertCompleted)
        XCTAssertTrue(isUpdateCompleted)
    }

    func testUpdateItemFailureSync() throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = testHistoricalItemProvider(databaseItem)

        let table = InMemoryDynamoDBTable()

        try table.insertItemWithHistoricalRowSync(primaryItem: databaseItem, historicalItem: historicalItem)

        let updatedPayload = versionedPayload.createUpdatedItem(withValue: versionedPayload.rowValue)
        let updatedItem = databaseItem.createUpdatedItem(withValue: updatedPayload)
        try table.updateItemWithHistoricalRowSync(primaryItem: updatedItem, existingItem: databaseItem, historicalItem: testHistoricalItemProvider(updatedItem))

        do {
            // Second update will fail.
            try table.updateItemWithHistoricalRowSync(primaryItem: databaseItem.createUpdatedItem(withValue: versionedPayload), existingItem: databaseItem, historicalItem: historicalItem)
        } catch SmokeDynamoDBError.conditionalCheckFailed(_) {
            // Success
        } catch {
            return XCTFail("Unexpected exception")
        }
    }

    func testUpdateItemFailureAsync() throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        let versionedPayload = RowWithItemVersion.newItem(withValue: payload)

        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: versionedPayload)
        let historicalItem = testHistoricalItemProvider(databaseItem)

        let table = InMemoryDynamoDBTable()

        var isInsertCompleted = false
        func insertCompletionHandler(error: Error?) {
            if error != nil {
                XCTFail()
            }

            isInsertCompleted = true
        }

        try table.insertItemWithHistoricalRowAsync(primaryItem: databaseItem, historicalItem: historicalItem,
                                                    completion: insertCompletionHandler)

        var isUpdate1Completed = false
        func update1CompletionHandler(error: Error?) {
            if error != nil {
                XCTFail()
            }

            isUpdate1Completed = true
        }

        let updatedPayload = versionedPayload.createUpdatedItem(withValue: versionedPayload.rowValue)
        let updatedItem = databaseItem.createUpdatedItem(withValue: updatedPayload)
        try table.updateItemWithHistoricalRowAsync(primaryItem: updatedItem, existingItem: databaseItem,
                                                    historicalItem: testHistoricalItemProvider(updatedItem),
                                                    completion: update1CompletionHandler)

        // Second update will fail.
        var isUpdate2Completed = false
        try table.updateItemWithHistoricalRowAsync(primaryItem: databaseItem.createUpdatedItem(withValue: versionedPayload),
                                               existingItem: databaseItem, historicalItem: historicalItem) { error in
            guard let theError = error, case SmokeDynamoDBError.conditionalCheckFailed = theError else {
                return XCTFail("Expected error not thrown")
            }

            isUpdate2Completed = true
        }

        XCTAssertTrue(isInsertCompleted)
        XCTAssertTrue(isUpdate1Completed)
        XCTAssertTrue(isUpdate2Completed)
    }

    func testClobberItemSuccessSync() throws {
        let table = InMemoryDynamoDBTable()

        let databaseItem = testPrimaryItemProvider(nil)

        try table.clobberItemWithHistoricalRowSync(primaryItemProvider: testPrimaryItemProvider, historicalItemProvider: testHistoricalItemProvider)
        let inserted : DatabaseRowType = (try table.getItemSync(forKey: databaseItem.compositePrimaryKey))!
        XCTAssertEqual(inserted.compositePrimaryKey.partitionKey, databaseItem.compositePrimaryKey.partitionKey)
        XCTAssertEqual(inserted.compositePrimaryKey.sortKey, databaseItem.compositePrimaryKey.sortKey)
    }

    func testClobberItemSuccessAsync() throws {
        let table = InMemoryDynamoDBTable()

        let databaseItem = testPrimaryItemProvider(nil)

        var isCompleted = false
        func completionHandler(error: Error?) {
            if error != nil {
                XCTFail()
            }

            isCompleted = true
        }

        try table.clobberItemWithHistoricalRowAsync(primaryItemProvider: testPrimaryItemProvider,
                                                     historicalItemProvider: testHistoricalItemProvider,
                                                     completion: completionHandler)
        let inserted : DatabaseRowType = (try table.getItemSync(forKey: databaseItem.compositePrimaryKey))!
        XCTAssertEqual(inserted.compositePrimaryKey.partitionKey, databaseItem.compositePrimaryKey.partitionKey)
        XCTAssertEqual(inserted.compositePrimaryKey.sortKey, databaseItem.compositePrimaryKey.sortKey)
        XCTAssertTrue(isCompleted)
    }

    func testClobberItemSuccessAfterRetrySync() throws {

        let databaseItem = testPrimaryItemProvider(nil)

        let wrappedTable = InMemoryDynamoDBTable()
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 5)

        try table.clobberItemWithHistoricalRowSync(primaryItemProvider: testPrimaryItemProvider,
                                                    historicalItemProvider: testHistoricalItemProvider)
        let inserted : DatabaseRowType = try table.getItemSync(forKey: databaseItem.compositePrimaryKey)!
        XCTAssertTrue(inserted.rowStatus.rowVersion > databaseItem.rowStatus.rowVersion)
    }

    func testClobberItemSuccessAfterRetryAsync() throws {

        let databaseItem = testPrimaryItemProvider(nil)

        let wrappedTable = InMemoryDynamoDBTable()
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 5)

        var isCompleted = false
        func completionHandler(error: Error?) {
            if error != nil {
                XCTFail()
            }

            isCompleted = true
        }

        try table.clobberItemWithHistoricalRowAsync(primaryItemProvider: testPrimaryItemProvider,
                                                     historicalItemProvider: testHistoricalItemProvider,
                                                     completion: completionHandler)
        let inserted : DatabaseRowType = try table.getItemSync(forKey: databaseItem.compositePrimaryKey)!
        XCTAssertTrue(inserted.rowStatus.rowVersion > databaseItem.rowStatus.rowVersion)
        XCTAssertTrue(isCompleted)
    }

    func testClobberItemFailureSync() throws {

        let wrappedTable = InMemoryDynamoDBTable()
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 12)

        do {
            try table.clobberItemWithHistoricalRowSync(primaryItemProvider: testPrimaryItemProvider, historicalItemProvider: testHistoricalItemProvider, withRetries: 9)

            XCTFail("Expected error not thrown.")
        } catch SmokeDynamoDBError.concurrencyError(_) {
            // Success
        } catch {
            return XCTFail("Unexpected exception: \(error)")
        }

    }

    func testClobberItemFailureAsync() throws {

        let wrappedTable = InMemoryDynamoDBTable()
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 12)

        var isCompleted = false
        try table.clobberItemWithHistoricalRowAsync(primaryItemProvider: testPrimaryItemProvider, historicalItemProvider: testHistoricalItemProvider, withRetries: 9) { error in
            guard let theError = error, case SmokeDynamoDBError.concurrencyError = theError else {
                return XCTFail("Expected error not thrown")
            }

            isCompleted = true
        }

        XCTAssertTrue(isCompleted)
    }

    private func conditionalUpdatePrimaryItemProvider(existingItem: DatabaseRowType) throws -> DatabaseRowType {
        let rowVersion = existingItem.rowStatus.rowVersion
        let dPayload = TestTypeA(firstly: "firstly_\(rowVersion)", secondly: "secondly_\(rowVersion)")

        return try existingItem.createUpdatedRowWithItemVersion(
            withValue: dPayload,
            conditionalStatusVersion: nil)
    }

    private let historicalCompositePrimaryKey = StandardCompositePrimaryKey(partitionKey: "historicalPartitionKey",
                                                                                   sortKey: "historicalSortKey")
    private func conditionalUpdateHistoricalItemProvider(updatedItem: DatabaseRowType) -> DatabaseRowType {
        // create an item for the history partition
        return TypedDatabaseItem.newItem(withKey: historicalCompositePrimaryKey,
                                         andValue: updatedItem.rowValue)
    }

    func testConditionallyUpdateItemWithHistoricalRowSync() throws {
        let table = InMemoryDynamoDBTable()

        let databaseItem = testPrimaryItemProvider(nil)
        try table.insertItemSync(databaseItem)

        let updated = try table.conditionallyUpdateItemWithHistoricalRowSync(
            compositePrimaryKey: dKey,
            primaryItemProvider: conditionalUpdatePrimaryItemProvider,
            historicalItemProvider: conditionalUpdateHistoricalItemProvider)

        let inserted: DatabaseRowType = (try table.getItemSync(forKey: databaseItem.compositePrimaryKey))!
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly_1")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly_1")
        XCTAssertEqual(inserted.rowStatus.rowVersion, 2)
        XCTAssertEqual(inserted.rowValue.itemVersion, 2)

        XCTAssertEqual(updated.rowValue.rowValue.firstly, inserted.rowValue.rowValue.firstly)
        XCTAssertEqual(updated.rowValue.rowValue.secondly, inserted.rowValue.rowValue.secondly)
        XCTAssertEqual(updated.rowStatus.rowVersion, inserted.rowStatus.rowVersion)
        XCTAssertEqual(updated.rowValue.itemVersion, inserted.rowValue.itemVersion)

        let historicalInserted: DatabaseRowType = (try table.getItemSync(forKey: historicalCompositePrimaryKey))!
        XCTAssertEqual(historicalInserted.rowValue.rowValue.firstly, "firstly_1")
        XCTAssertEqual(historicalInserted.rowValue.rowValue.secondly, "secondly_1")
        XCTAssertEqual(historicalInserted.rowStatus.rowVersion, 1)
        XCTAssertEqual(historicalInserted.rowValue.itemVersion, 2)
    }

    func testConditionallyUpdateItemWithHistoricalRowAsync() throws {
        let table = InMemoryDynamoDBTable()

        let databaseItem = testPrimaryItemProvider(nil)
        try table.insertItemSync(databaseItem)

        var isCompleted = false
        func completionHandler( result: SmokeDynamoDBErrorResult<DatabaseRowType> ) {
            switch result {
            case .success( let result ):
                // the result returned is the updated item
                XCTAssertEqual( result.rowStatus.rowVersion, 2 )
                XCTAssertEqual( result.rowValue.itemVersion, 2 )
            case .failure:
                XCTFail()
            }

            isCompleted = true
        }

        try table.conditionallyUpdateItemWithHistoricalRowAsync(
            forPrimaryKey: dKey,
            primaryItemProvider: conditionalUpdatePrimaryItemProvider,
            historicalItemProvider: conditionalUpdateHistoricalItemProvider, completion: completionHandler)

        let inserted: DatabaseRowType = (try table.getItemSync(forKey: databaseItem.compositePrimaryKey))!
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly_1")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly_1")
        XCTAssertEqual(inserted.rowStatus.rowVersion, 2)
        XCTAssertEqual(inserted.rowValue.itemVersion, 2)

        let historicalInserted: DatabaseRowType = (try table.getItemSync(forKey: historicalCompositePrimaryKey))!
        XCTAssertEqual(historicalInserted.rowValue.rowValue.firstly, "firstly_1")
        XCTAssertEqual(historicalInserted.rowValue.rowValue.secondly, "secondly_1")
        XCTAssertEqual(historicalInserted.rowStatus.rowVersion, 1)
        XCTAssertEqual(historicalInserted.rowValue.itemVersion, 2)

        XCTAssertTrue(isCompleted)
    }

    func testConditionallyUpdateItemWithHistoricalRowAcceptableConcurrencySync() throws {
        let wrappedTable = InMemoryDynamoDBTable()
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)

        let databaseItem = testPrimaryItemProvider(nil)
        try table.insertItemSync(databaseItem)

        let updated = try table.conditionallyUpdateItemWithHistoricalRowSync(
            compositePrimaryKey: dKey,
            primaryItemProvider: conditionalUpdatePrimaryItemProvider,
            historicalItemProvider: conditionalUpdateHistoricalItemProvider)

        let inserted: DatabaseRowType = (try table.getItemSync(forKey: databaseItem.compositePrimaryKey))!
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly_6")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly_6")
        // the row version has been updated by the SimulateConcurrencyDynamoDBTable an
        // additional 5 fives, item updated by conditionallyUpdateItemWithHistoricalRow
        // (which increments itemVersion) only once
        XCTAssertEqual(inserted.rowStatus.rowVersion, 7)
        XCTAssertEqual(inserted.rowValue.itemVersion, 2)

        XCTAssertEqual(updated.rowValue.rowValue.firstly, inserted.rowValue.rowValue.firstly)
        XCTAssertEqual(updated.rowValue.rowValue.secondly, inserted.rowValue.rowValue.secondly)
        XCTAssertEqual(updated.rowStatus.rowVersion, inserted.rowStatus.rowVersion)
        XCTAssertEqual(updated.rowValue.itemVersion, inserted.rowValue.itemVersion)
    }

    func testConditionallyUpdateItemWithHistoricalRowAcceptableConcurrencyAsync() throws {
        let wrappedTable = InMemoryDynamoDBTable()
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)

        let databaseItem = testPrimaryItemProvider(nil)
        try table.insertItemSync(databaseItem)

        var isCompleted = false
        func completionHandler(result: SmokeDynamoDBErrorResult<DatabaseRowType>) {
            switch result {
            case .success( let result ):
                // the result returned is the updated item
                XCTAssertEqual( result.rowStatus.rowVersion, 7 )
                XCTAssertEqual( result.rowValue.itemVersion, 2 )
            case .failure:
                XCTFail()
            }

            isCompleted = true
        }

        try table.conditionallyUpdateItemWithHistoricalRowAsync(
            forPrimaryKey: dKey,
            primaryItemProvider: conditionalUpdatePrimaryItemProvider,
            historicalItemProvider: conditionalUpdateHistoricalItemProvider, completion: completionHandler)

        let inserted: DatabaseRowType = (try table.getItemSync(forKey: databaseItem.compositePrimaryKey))!
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly_6")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly_6")
        // the row version has been updated by the SimulateConcurrencyDynamoDBTable an
        // additional 5 fives, item updated by conditionallyUpdateItemWithHistoricalRow
        // (which increments itemVersion) only once
        XCTAssertEqual(inserted.rowStatus.rowVersion, 7)
        XCTAssertEqual(inserted.rowValue.itemVersion, 2)

        XCTAssertTrue(isCompleted)
    }

    func testConditionallyUpdateItemWithHistoricalRowUnacceptableConcurrencySync() throws {
        let wrappedTable = InMemoryDynamoDBTable()
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 50,
                                                     simulateOnInsertItem: false)

        let databaseItem = testPrimaryItemProvider(nil)
        try table.insertItemSync(databaseItem)

        do {
            _ = try table.conditionallyUpdateItemWithHistoricalRowSync(
                compositePrimaryKey: dKey,
                primaryItemProvider: conditionalUpdatePrimaryItemProvider,
                historicalItemProvider: conditionalUpdateHistoricalItemProvider)

            XCTFail("Expected error not thrown.")
        } catch SmokeDynamoDBError.concurrencyError(_) {
            // Success
        } catch {
            return XCTFail("Unexpected exception: \(error)")
        }

        let inserted: DatabaseRowType = (try table.getItemSync(forKey: databaseItem.compositePrimaryKey))!
        // confirm row has not been updated by conditionallyUpdateItemWithHistoricalRow
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly")
        XCTAssertEqual(inserted.rowStatus.rowVersion, 11)
        XCTAssertEqual(inserted.rowValue.itemVersion, 1)
    }

    func testConditionallyUpdateItemWithHistoricalRowUnacceptableConcurrencyAsync() throws {
        let wrappedTable = InMemoryDynamoDBTable()
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 50,
                                                     simulateOnInsertItem: false)

        let databaseItem = testPrimaryItemProvider(nil)
        try table.insertItemSync(databaseItem)

        var isCompleted = false
        try table.conditionallyUpdateItemWithHistoricalRowAsync(
            forPrimaryKey: dKey,
            primaryItemProvider: conditionalUpdatePrimaryItemProvider,
            historicalItemProvider: conditionalUpdateHistoricalItemProvider) { result in
                guard case let .failure(theError) = result, case SmokeDynamoDBError.concurrencyError = theError else {
                    return XCTFail( "Expected error not thrown" )
                }

                isCompleted = true
            }

        let inserted: DatabaseRowType = (try table.getItemSync(forKey: databaseItem.compositePrimaryKey))!
        // confirm row has not been updated by conditionallyUpdateItemWithHistoricalRow
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly")
        XCTAssertEqual(inserted.rowStatus.rowVersion, 11)
        XCTAssertEqual(inserted.rowValue.itemVersion, 1)

        XCTAssertTrue(isCompleted)
    }

    enum TestError: Error {
        case everythingIsWrong
    }

    func testConditionallyUpdateItemWithHistoricalRowPrimaryItemProviderErrorSync() throws {
        let wrappedTable = InMemoryDynamoDBTable()
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)

        let databaseItem = testPrimaryItemProvider(nil)
        try table.insertItemSync(databaseItem)

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
            _ = try table.conditionallyUpdateItemWithHistoricalRowSync(
                compositePrimaryKey: dKey,
                primaryItemProvider: primaryItemProvider,
                historicalItemProvider: conditionalUpdateHistoricalItemProvider)

            XCTFail("Expected error not thrown.")
        } catch TestError.everythingIsWrong {
            // Success
        } catch {
            return XCTFail("Unexpected exception: \(error)")
        }

        let inserted: DatabaseRowType = (try table.getItemSync(forKey: databaseItem.compositePrimaryKey))!
        // confirm row has not been updated by conditionallyUpdateItemWithHistoricalRow
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly")
        XCTAssertEqual(inserted.rowStatus.rowVersion, 6)
        XCTAssertEqual(inserted.rowValue.itemVersion, 1)
    }

    func testConditionallyUpdateItemWithHistoricalRowPrimaryItemProviderErrorAsync() throws {
        let wrappedTable = InMemoryDynamoDBTable()
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)

        let databaseItem = testPrimaryItemProvider(nil)
        try table.insertItemSync(databaseItem)

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

        var isCompleted = false
        try table.conditionallyUpdateItemWithHistoricalRowAsync(
            forPrimaryKey: dKey,
            primaryItemProvider: primaryItemProvider,
            historicalItemProvider: conditionalUpdateHistoricalItemProvider) { result in
                guard case let .failure(theError) = result, case SmokeDynamoDBError.unrecognizedError(let errorType, let errorDescription) = theError,
                    errorDescription == String(describing: TestError.everythingIsWrong),
                    errorType == String(describing: type(of: TestError.everythingIsWrong)) else {
                    return XCTFail( "Expected error not thrown" )
                }

                isCompleted = true
            }

        let inserted: DatabaseRowType = (try table.getItemSync(forKey: databaseItem.compositePrimaryKey))!
        // confirm row has not been updated by conditionallyUpdateItemWithHistoricalRow
        XCTAssertEqual(inserted.rowValue.rowValue.firstly, "firstly")
        XCTAssertEqual(inserted.rowValue.rowValue.secondly, "secondly")
        XCTAssertEqual(inserted.rowStatus.rowVersion, 6)
        XCTAssertEqual(inserted.rowValue.itemVersion, 1)

        XCTAssertTrue(isCompleted)
    }

    static var allTests = [
        ("testInsertItemSuccessSync", testInsertItemSuccessSync),
        ("testInsertItemSuccessAsync", testInsertItemSuccessAsync),
        ("testInsertItemFailureSync", testInsertItemFailureSync),
        ("testInsertItemFailureAsync", testInsertItemFailureAsync),
        ("testUpdateItemSuccessSync", testUpdateItemSuccessSync),
        ("testUpdateItemSuccessAsync", testUpdateItemSuccessAsync),
        ("testUpdateItemFailureSync", testUpdateItemFailureSync),
        ("testUpdateItemFailureAsync", testUpdateItemFailureAsync),
        ("testClobberItemSuccessSync", testClobberItemSuccessSync),
        ("testClobberItemSuccessAsync", testClobberItemSuccessAsync),
        ("testClobberItemFailureSync", testClobberItemFailureSync),
        ("testClobberItemFailureAsync", testClobberItemFailureAsync),
        ("testClobberItemSuccessAfterRetrySync", testClobberItemSuccessAfterRetrySync),
        ("testClobberItemSuccessAfterRetryAsync", testClobberItemSuccessAfterRetryAsync),
        ("testConditionallyUpdateItemWithHistoricalRowSync", testConditionallyUpdateItemWithHistoricalRowSync),
        ("testConditionallyUpdateItemWithHistoricalRowAsync", testConditionallyUpdateItemWithHistoricalRowAsync),
        ("testConditionallyUpdateItemWithHistoricalRowAcceptableConcurrencySync",
         testConditionallyUpdateItemWithHistoricalRowAcceptableConcurrencySync),
        ("testConditionallyUpdateItemWithHistoricalRowAcceptableConcurrencyAsync",
         testConditionallyUpdateItemWithHistoricalRowAcceptableConcurrencyAsync),
        ("testConditionallyUpdateItemWithHistoricalRowUnacceptableConcurrencySync",
         testConditionallyUpdateItemWithHistoricalRowUnacceptableConcurrencySync),
        ("testConditionallyUpdateItemWithHistoricalRowUnacceptableConcurrencyAsync",
         testConditionallyUpdateItemWithHistoricalRowUnacceptableConcurrencyAsync),
        ("testConditionallyUpdateItemWithHistoricalRowPrimaryItemProviderErrorSync",
         testConditionallyUpdateItemWithHistoricalRowPrimaryItemProviderErrorSync),
        ("testConditionallyUpdateItemWithHistoricalRowPrimaryItemProviderErrorAsync",
         testConditionallyUpdateItemWithHistoricalRowPrimaryItemProviderErrorAsync)
    ]
}
