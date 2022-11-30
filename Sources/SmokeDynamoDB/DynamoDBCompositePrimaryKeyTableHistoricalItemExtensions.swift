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
//  DynamoDBCompositePrimaryKeyTableHistoricalItemExtensions
//      Extensions which enable historical item multi-row update usecases.
//  SmokeDynamoDB
//

import Foundation
import Logging
import SmokeHTTPClient
import DynamoDBModel

public extension DynamoDBCompositePrimaryKeyTable {
    /**
     * Historical items exist across multiple rows. This method provides an interface to record all
     * rows in a single call.
     */
    func insertItemWithHistoricalRow<AttributesType, ItemType>(primaryItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                               historicalItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                               tableOverrides: WritableTableOverrides? = nil) async throws {
        try await insertItem(primaryItem, tableOverrides: tableOverrides)
        try await insertItem(historicalItem, tableOverrides: tableOverrides)
    }

    func updateItemWithHistoricalRow<AttributesType, ItemType>(primaryItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                               existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                               historicalItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                               tableOverrides: WritableTableOverrides? = nil) async throws {
        try await updateItem(newItem: primaryItem, existingItem: existingItem, tableOverrides: tableOverrides)
        try await insertItem(historicalItem, tableOverrides: tableOverrides)
    }
    
    /**
     * This operation will attempt to update the primary item, repeatedly calling the
     * `primaryItemProvider` to retrieve an updated version of the current row (if it
     * exists) until the appropriate `insert` or  `update` operation succeeds. Once this
     * operation has succeeded, the `historicalItemProvider` is called to provide
     * the historical item based on the primary item that was inserted into the
     * database table. The primary item may not exist in the database table to
     * begin with.
     *
     * Clobbering a historical item requires knowledge of existing rows to accurately record
     * historical data.
     */
    func clobberItemWithHistoricalRow<AttributesType, ItemType>(
            readableTableOverrides: ReadableTableOverrides? = nil,
            writableTableOverrides: WritableTableOverrides? = nil,
            primaryItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>?) -> TypedDatabaseItem<AttributesType, ItemType>,
            historicalItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
            withRetries retries: Int = 10) async throws {
        let primaryItem = primaryItemProvider(nil)

        guard retries > 0 else {
            throw SmokeDynamoDBError.concurrencyError(partitionKey: primaryItem.compositePrimaryKey.partitionKey,
                                                      sortKey: primaryItem.compositePrimaryKey.sortKey,
                                                      message: "Unable to complete request to clobber versioned item in specified number of attempts")
            
        }
        
                let existingItemOptional: TypedDatabaseItem<AttributesType, ItemType>? = try await getItem(forKey: primaryItem.compositePrimaryKey,
                                                                                                           tableOverrides: readableTableOverrides)
            
        if let existingItem = existingItemOptional {
            let newItem: TypedDatabaseItem<AttributesType, ItemType> = primaryItemProvider(existingItem)

            do {
                try await updateItemWithHistoricalRow(primaryItem: newItem, existingItem: existingItem,
                                                      historicalItem: historicalItemProvider(newItem),
                                                      tableOverrides: writableTableOverrides)
            } catch {
                try await clobberItemWithHistoricalRow(readableTableOverrides: readableTableOverrides,
                                                       writableTableOverrides: writableTableOverrides,
                                                       primaryItemProvider: primaryItemProvider,
                                                       historicalItemProvider: historicalItemProvider,
                                                       withRetries: retries - 1)
                return
            }
        } else {
            do {
                try await insertItemWithHistoricalRow(primaryItem: primaryItem,
                                                      historicalItem: historicalItemProvider(primaryItem),
                                                      tableOverrides: writableTableOverrides)
            } catch {
                try await clobberItemWithHistoricalRow(readableTableOverrides: readableTableOverrides,
                                                       writableTableOverrides: writableTableOverrides,
                                                       primaryItemProvider: primaryItemProvider,
                                                       historicalItemProvider: historicalItemProvider,
                                                       withRetries: retries - 1)
                return
            }
        }
    }
    
    /**
      Operations will attempt to update the primary item, repeatedly calling the
      `primaryItemProvider` to retrieve an updated version of the current row
      until the appropriate  `update` operation succeeds. The
      `primaryItemProvider` can thrown an exception to indicate that the current
      row is unable to be updated. The `historicalItemProvider` is called to
      provide the historical item based on the primary item that was
      inserted into the database table.

     - Parameters:
        - compositePrimaryKey: The composite key for the version to update.
        - primaryItemProvider: Function to provide the updated item or throw if the current item can't be updated.
        - historicalItemProvider: Function to provide the historical item for the primary item.
     */
    func conditionallyUpdateItemWithHistoricalRow<AttributesType, ItemType>(
            compositePrimaryKey: CompositePrimaryKey<AttributesType>,
            readableTableOverrides: ReadableTableOverrides? = nil,
            writableTableOverrides: WritableTableOverrides? = nil,
            primaryItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) async throws -> TypedDatabaseItem<AttributesType, ItemType>,
            historicalItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
            withRetries retries: Int = 10) async throws -> TypedDatabaseItem<AttributesType, ItemType> {
        return try await conditionallyUpdateItemWithHistoricalRowInternal(
            compositePrimaryKey: compositePrimaryKey,
            readableTableOverrides: readableTableOverrides,
            writableTableOverrides: writableTableOverrides,
            primaryItemProvider: primaryItemProvider,
            historicalItemProvider: historicalItemProvider,
            withRetries: retries)
    }
    
    private func conditionallyUpdateItemWithHistoricalRowInternal<AttributesType, ItemType>(
            compositePrimaryKey: CompositePrimaryKey<AttributesType>,
            readableTableOverrides: ReadableTableOverrides? = nil,
            writableTableOverrides: WritableTableOverrides? = nil,
            primaryItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) async throws -> TypedDatabaseItem<AttributesType, ItemType>,
            historicalItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
            withRetries retries: Int = 10) async throws -> TypedDatabaseItem<AttributesType, ItemType> {
        guard retries > 0 else {
             throw SmokeDynamoDBError.concurrencyError(partitionKey: compositePrimaryKey.partitionKey,
                                                       sortKey: compositePrimaryKey.sortKey,
                                                       message: "Unable to complete request to update versioned item in specified number of attempts")
        }
        
                let existingItemOptional: TypedDatabaseItem<AttributesType, ItemType>? = try await getItem(forKey: compositePrimaryKey,
                                                                                                           tableOverrides: readableTableOverrides)
        
        guard let existingItem = existingItemOptional else {
            throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: compositePrimaryKey.partitionKey,
                                                            sortKey: compositePrimaryKey.sortKey,
                                                            message: "Item not present in database.")
        }
        
        let updatedItem = try await primaryItemProvider(existingItem)
        let historicalItem = historicalItemProvider(updatedItem)

        do {
            try await updateItemWithHistoricalRow(primaryItem: updatedItem,
                                                  existingItem: existingItem,
                                                  historicalItem: historicalItem,
                                                  tableOverrides: writableTableOverrides)
        } catch SmokeDynamoDBError.conditionalCheckFailed {
            // try again
            return try await conditionallyUpdateItemWithHistoricalRow(compositePrimaryKey: compositePrimaryKey,
                                                                      readableTableOverrides: readableTableOverrides,
                                                                      writableTableOverrides: writableTableOverrides,
                                                                      primaryItemProvider: primaryItemProvider,
                                                                      historicalItemProvider: historicalItemProvider, withRetries: retries - 1)
        }
        
        return updatedItem
    }
}
