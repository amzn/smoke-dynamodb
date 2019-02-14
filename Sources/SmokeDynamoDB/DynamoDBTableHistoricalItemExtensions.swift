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
//  DynamoDBTableHistoricalItemExtensions
//      Extensions which enable historical item multi-row update usecases.
//  SmokeDynamoDB
//

import Foundation
import LoggerAPI
import SmokeHTTPClient

public extension DynamoDBTable {

    /**
     * Historical items exist across multiple rows. This method provides an interface to record all
     * rows in a single call.
     */
    public func insertItemWithHistoricalRowSync<AttributesType, ItemType>(primaryItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                                          historicalItem: TypedDatabaseItem<AttributesType, ItemType>) throws {
        try insertItemSync(primaryItem)
        try insertItemSync(historicalItem)
    }

    public func insertItemWithHistoricalRowAsync<AttributesType, ItemType>(primaryItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                                           historicalItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                                           completion: @escaping (Error?) -> ()) throws {
        try insertItemAsync(primaryItem) { error in
            if let error = error {
                return completion(error)
            }

            do {
                try self.insertItemAsync(historicalItem, completion: completion)
            } catch {
                completion(error)
            }
        }
    }

    public func updateItemWithHistoricalRowSync<AttributesType, ItemType>(primaryItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                                          existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                                          historicalItem: TypedDatabaseItem<AttributesType, ItemType>) throws {
        try updateItemSync(newItem: primaryItem, existingItem: existingItem)
        try insertItemSync(historicalItem)
    }

    public func updateItemWithHistoricalRowAsync<AttributesType, ItemType>(primaryItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                                           existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                                           historicalItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                                           completion: @escaping (Error?) -> ()) throws {
        try updateItemAsync(newItem: primaryItem, existingItem: existingItem) { error in
            if let error = error {
                return completion(error)
            }

            do {
                try self.insertItemAsync(historicalItem, completion: completion)
            } catch {
                completion(error)
            }
        }
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
    public func clobberItemWithHistoricalRowSync<AttributesType, ItemType>(
            primaryItemProvider: (TypedDatabaseItem<AttributesType, ItemType>?) -> TypedDatabaseItem<AttributesType, ItemType>,
            historicalItemProvider: (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
            withRetries retries: Int = 10) throws {

        let primaryItem = primaryItemProvider(nil)

        guard retries > 0 else {
            throw SmokeDynamoDBError.concurrencyError(partitionKey: primaryItem.compositePrimaryKey.partitionKey,
                                                    sortKey: primaryItem.compositePrimaryKey.sortKey,
                                                    message: "Unable to complete request to clobber versioned item in specified number of attempts")
        }

        if let existingItem: TypedDatabaseItem<AttributesType, ItemType> = try getItemSync(forKey: primaryItem.compositePrimaryKey) {

            let newItem: TypedDatabaseItem<AttributesType, ItemType> = primaryItemProvider(existingItem)

            do {
                try updateItemWithHistoricalRowSync(primaryItem: newItem, existingItem: existingItem, historicalItem: historicalItemProvider(newItem))
            } catch SmokeDynamoDBError.conditionalCheckFailed(_) {
                try clobberItemWithHistoricalRowSync(primaryItemProvider: primaryItemProvider,
                                                 historicalItemProvider: historicalItemProvider,
                                                 withRetries: retries - 1)
            }
        } else {
            do {
                try insertItemWithHistoricalRowSync(primaryItem: primaryItem,
                                                historicalItem: historicalItemProvider(primaryItem))
            } catch SmokeDynamoDBError.conditionalCheckFailed(_) {
                try clobberItemWithHistoricalRowSync(primaryItemProvider: primaryItemProvider,
                                                 historicalItemProvider: historicalItemProvider,
                                                 withRetries: retries - 1)
            }
        }
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
    public func clobberItemWithHistoricalRowAsync<AttributesType, ItemType>(
            primaryItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>?) -> TypedDatabaseItem<AttributesType, ItemType>,
            historicalItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
            withRetries retries: Int = 10,
            completion: @escaping (Error?) -> ()) throws {

        let primaryItem = primaryItemProvider(nil)

        guard retries > 0 else {
            throw SmokeDynamoDBError.concurrencyError(partitionKey: primaryItem.compositePrimaryKey.partitionKey,
                                                    sortKey: primaryItem.compositePrimaryKey.sortKey,
                                                    message: "Unable to complete request to clobber versioned item in specified number of attempts")
        }

        func insertOrUpdateItemResultHander(error: Error?) {
            if let theError = error, case SmokeDynamoDBError.conditionalCheckFailed = theError {
                do {
                    try clobberItemWithHistoricalRowAsync(primaryItemProvider: primaryItemProvider,
                                                          historicalItemProvider: historicalItemProvider,
                                                          withRetries: retries - 1,
                                                          completion: completion)
                } catch {
                    completion(error)
                }
            } else {
                completion(error)
            }
        }

        func handleGetItemResult(result: HTTPResult<TypedDatabaseItem<AttributesType, ItemType>?>) {
            switch result {
            case .response(let existingItemOptional):
                do {
                    if let existingItem = existingItemOptional {
                        let newItem: TypedDatabaseItem<AttributesType, ItemType> = primaryItemProvider(existingItem)

                        try updateItemWithHistoricalRowAsync(primaryItem: newItem, existingItem: existingItem,
                                                             historicalItem: historicalItemProvider(newItem),
                                                             completion: insertOrUpdateItemResultHander)
                    } else {
                        try insertItemWithHistoricalRowAsync(primaryItem: primaryItem,
                                                             historicalItem: historicalItemProvider(primaryItem),
                                                             completion: insertOrUpdateItemResultHander)
                    }
                } catch {
                    completion(error)
                }
            case .error(let error):
                completion(error)
            }
        }

        try getItemAsync(forKey: primaryItem.compositePrimaryKey, completion: handleGetItemResult)
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
    public func conditionallyUpdateItemWithHistoricalRowSync<AttributesType, ItemType>(
        compositePrimaryKey: CompositePrimaryKey<AttributesType>,
        primaryItemProvider: (TypedDatabaseItem<AttributesType, ItemType>) throws -> TypedDatabaseItem<AttributesType, ItemType>,
        historicalItemProvider: (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
        withRetries retries: Int = 10) throws -> TypedDatabaseItem<AttributesType, ItemType> {

        guard retries > 0 else {
            throw SmokeDynamoDBError.concurrencyError(partitionKey: compositePrimaryKey.partitionKey,
                                                    sortKey: compositePrimaryKey.sortKey,
                                                    message: "Unable to complete request to update versioned item in specified number of attempts")
        }

        // get the existing item
        guard let existingItem: TypedDatabaseItem<AttributesType, ItemType> =
            try getItemSync(forKey: compositePrimaryKey) else {
                throw SmokeDynamoDBError.conditionalCheckFailed(paritionKey: compositePrimaryKey.partitionKey,
                                                              sortKey: compositePrimaryKey.sortKey,
                                                              message: "Item not present in database.")
        }

        let updatedItem = try primaryItemProvider(existingItem)
        let historicalItem = historicalItemProvider(updatedItem)

        do {
            try updateItemWithHistoricalRowSync(primaryItem: updatedItem,
                                            existingItem: existingItem,
                                            historicalItem: historicalItem)

            return updatedItem
        } catch SmokeDynamoDBError.conditionalCheckFailed {
            // try again
            return try conditionallyUpdateItemWithHistoricalRowSync(compositePrimaryKey: compositePrimaryKey,
                                                                    primaryItemProvider: primaryItemProvider,
                                                                    historicalItemProvider: historicalItemProvider,
                                                                    withRetries: retries - 1)
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
    public func conditionallyUpdateItemWithHistoricalRowAsync<AttributesType, ItemType>(
        forPrimaryKey compositePrimaryKey: CompositePrimaryKey<AttributesType>,
        primaryItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) throws -> TypedDatabaseItem<AttributesType, ItemType>,
        historicalItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
        withRetries retries: Int = 10,
        completion: @escaping (HTTPResult<TypedDatabaseItem<AttributesType, ItemType>>) -> ()) throws {

        guard retries > 0 else {
            throw SmokeDynamoDBError.concurrencyError(partitionKey: compositePrimaryKey.partitionKey,
                                                    sortKey: compositePrimaryKey.sortKey,
                                                    message: "Unable to complete request to update versioned item in specified number of attempts")
        }

        func handleUpdateItemResult(error: Error?) {
            if let theError = error, case SmokeDynamoDBError.conditionalCheckFailed = theError {
                do {
                    // try again
                    return try conditionallyUpdateItemWithHistoricalRowAsync(forPrimaryKey: compositePrimaryKey,
                                                                             primaryItemProvider: primaryItemProvider,
                                                                             historicalItemProvider: historicalItemProvider,
                                                                             withRetries: retries - 1,
                                                                             completion: completion)
                } catch {
                    completion( HTTPResult.error( error ) )
                }
            } else if let theError = error {
                completion( HTTPResult.error( theError ) )
            }
        }

        func handleGetItemResult(result: HTTPResult<TypedDatabaseItem<AttributesType, ItemType>?>) {
            switch result {
            case .response(let existingItemOptional):
                guard let existingItem = existingItemOptional else {
                    let error = SmokeDynamoDBError.conditionalCheckFailed(paritionKey: compositePrimaryKey.partitionKey,
                                                              sortKey: compositePrimaryKey.sortKey,
                                                              message: "Item not present in database.")

                    return completion( HTTPResult.error( error ) )
                }

                do {
                    let updatedItem = try primaryItemProvider(existingItem)
                    let historicalItem = historicalItemProvider(updatedItem)

                    try updateItemWithHistoricalRowAsync(primaryItem: updatedItem,
                                                         existingItem: existingItem,
                                                         historicalItem: historicalItem,
                                                         completion: handleUpdateItemResult)
                    completion( HTTPResult.response( updatedItem ) )
                } catch {
                    completion( HTTPResult.error( error ) )
                }
            case .error(let error):
                completion( HTTPResult.error( error ) )
            }
        }

        try getItemAsync(forKey: compositePrimaryKey, completion: handleGetItemResult)
    }
}
