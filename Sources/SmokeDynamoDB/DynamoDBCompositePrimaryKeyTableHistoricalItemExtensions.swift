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
import NIO

public extension DynamoDBCompositePrimaryKeyTable {

    /**
     * Historical items exist across multiple rows. This method provides an interface to record all
     * rows in a single call.
     */
    func insertItemWithHistoricalRow<AttributesType, ItemType>(primaryItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                               historicalItem: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void> {
        return insertItem(primaryItem).flatMap { _ in
            return self.insertItem(historicalItem)
        }
    }

    func updateItemWithHistoricalRow<AttributesType, ItemType>(primaryItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                               existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                               historicalItem: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void> {
        return updateItem(newItem: primaryItem, existingItem: existingItem).flatMap { _ in
            return self.insertItem(historicalItem)
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
    func clobberItemWithHistoricalRow<AttributesType, ItemType>(
            primaryItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>?) -> TypedDatabaseItem<AttributesType, ItemType>,
            historicalItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
            withRetries retries: Int = 10) -> EventLoopFuture<Void> {

        let primaryItem = primaryItemProvider(nil)

        guard retries > 0 else {
            let error = SmokeDynamoDBError.concurrencyError(partitionKey: primaryItem.compositePrimaryKey.partitionKey,
                                                    sortKey: primaryItem.compositePrimaryKey.sortKey,
                                                    message: "Unable to complete request to clobber versioned item in specified number of attempts")
            
            let promise = self.eventLoop.makePromise(of: Void.self)
            promise.fail(error)
            return promise.futureResult
        }
        
        func insertOrUpdateItemErrorHander(error: Error) -> EventLoopFuture<Void> {
            if case SmokeDynamoDBError.conditionalCheckFailed = error {
                return clobberItemWithHistoricalRow(primaryItemProvider: primaryItemProvider,
                                                    historicalItemProvider: historicalItemProvider,
                                                    withRetries: retries - 1)
            } else {
                // propagate the error as it's not an error causing a retry
                let promise = self.eventLoop.makePromise(of: Void.self)
                promise.fail(error)
                return promise.futureResult
            }
        }
        
        return getItem(forKey: primaryItem.compositePrimaryKey).flatMap { (existingItemOptional: TypedDatabaseItem<AttributesType, ItemType>?) in
            if let existingItem = existingItemOptional {
                let newItem: TypedDatabaseItem<AttributesType, ItemType> = primaryItemProvider(existingItem)

                return self.updateItemWithHistoricalRow(primaryItem: newItem, existingItem: existingItem,
                                                   historicalItem: historicalItemProvider(newItem))
                    .flatMapError(insertOrUpdateItemErrorHander)
            } else {
                return self.insertItemWithHistoricalRow(primaryItem: primaryItem,
                                                   historicalItem: historicalItemProvider(primaryItem))
                    .flatMapError(insertOrUpdateItemErrorHander)
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
            primaryItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) throws -> TypedDatabaseItem<AttributesType, ItemType>,
            historicalItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
            withRetries retries: Int = 10) -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>> {
        guard retries > 0 else {
            let error = SmokeDynamoDBError.concurrencyError(partitionKey: compositePrimaryKey.partitionKey,
                                                            sortKey: compositePrimaryKey.sortKey,
                                                            message: "Unable to complete request to update versioned item in specified number of attempts")
            
            let promise = self.eventLoop.makePromise(of: TypedDatabaseItem<AttributesType, ItemType>.self)
            promise.fail(error)
            return promise.futureResult
        }
        
        return getItem(forKey: compositePrimaryKey).flatMap { (existingItemOptional: TypedDatabaseItem<AttributesType, ItemType>?) in
            guard let existingItem = existingItemOptional else {
                let error = SmokeDynamoDBError.conditionalCheckFailed(partitionKey: compositePrimaryKey.partitionKey,
                                                          sortKey: compositePrimaryKey.sortKey,
                                                          message: "Item not present in database.")
                
                let promise = self.eventLoop.makePromise(of: TypedDatabaseItem<AttributesType, ItemType>.self)
                promise.fail(error)
                return promise.futureResult
            }

            do {
                let updatedItem = try primaryItemProvider(existingItem)
                let historicalItem = historicalItemProvider(updatedItem)

                let errorHandler: ((Error) -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>>) = self.getUpdateItemErrorHandler(
                    forPrimaryKey: compositePrimaryKey,
                    primaryItemProvider: primaryItemProvider,
                    historicalItemProvider: historicalItemProvider,
                    withRetries: retries)

                return self.updateItemWithHistoricalRow(primaryItem: updatedItem,
                                                        existingItem: existingItem,
                                                        historicalItem: historicalItem)
                    .map { _ in
                        // return the updated item
                        return updatedItem
                    } .flatMapError(errorHandler)
            } catch {
                let promise = self.eventLoop.makePromise(of: TypedDatabaseItem<AttributesType, ItemType>.self)
                promise.fail(error)
                return promise.futureResult
            }
        }
    }
    
#if (os(Linux) && compiler(>=5.5)) || (!os(Linux) && compiler(>=5.5.2)) && canImport(_Concurrency)
    /**
     * Historical items exist across multiple rows. This method provides an interface to record all
     * rows in a single call.
     */
    func insertItemWithHistoricalRow<AttributesType, ItemType>(primaryItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                               historicalItem: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        try await insertItem(primaryItem)
        try await insertItem(historicalItem)
    }

    func updateItemWithHistoricalRow<AttributesType, ItemType>(primaryItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                               existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                               historicalItem: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        try await updateItem(newItem: primaryItem, existingItem: existingItem)
        try await insertItem(historicalItem)
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
            primaryItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>?) -> TypedDatabaseItem<AttributesType, ItemType>,
            historicalItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
            withRetries retries: Int = 10) async throws {
        try await clobberItemWithHistoricalRow(primaryItemProvider: primaryItemProvider,
                                               historicalItemProvider: historicalItemProvider,
                                               withRetries: retries).get()
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
            primaryItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) async throws -> TypedDatabaseItem<AttributesType, ItemType>,
            historicalItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
            withRetries retries: Int = 10) async throws -> TypedDatabaseItem<AttributesType, ItemType> {
        return try await conditionallyUpdateItemWithHistoricalRowInternal(
            compositePrimaryKey: compositePrimaryKey,
            primaryItemProvider: primaryItemProvider,
            historicalItemProvider: historicalItemProvider,
            withRetries: retries)
    }
    
    // Explicitly specify an overload with sync updatedPayloadProvider
    // to avoid the compiler matching a call site with such a provider with the EventLoopFuture-returning overload.
    func conditionallyUpdateItemWithHistoricalRow<AttributesType, ItemType>(
            compositePrimaryKey: CompositePrimaryKey<AttributesType>,
            primaryItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) throws -> TypedDatabaseItem<AttributesType, ItemType>,
            historicalItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
            withRetries retries: Int = 10) async throws -> TypedDatabaseItem<AttributesType, ItemType> {
        return try await conditionallyUpdateItemWithHistoricalRowInternal(
            compositePrimaryKey: compositePrimaryKey,
            primaryItemProvider: primaryItemProvider,
            historicalItemProvider: historicalItemProvider,
            withRetries: retries)
    }
    
    private func conditionallyUpdateItemWithHistoricalRowInternal<AttributesType, ItemType>(
            compositePrimaryKey: CompositePrimaryKey<AttributesType>,
            primaryItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) async throws -> TypedDatabaseItem<AttributesType, ItemType>,
            historicalItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
            withRetries retries: Int = 10) async throws -> TypedDatabaseItem<AttributesType, ItemType> {
        guard retries > 0 else {
             throw SmokeDynamoDBError.concurrencyError(partitionKey: compositePrimaryKey.partitionKey,
                                                       sortKey: compositePrimaryKey.sortKey,
                                                       message: "Unable to complete request to update versioned item in specified number of attempts")
        }
        
        let existingItemOptional: TypedDatabaseItem<AttributesType, ItemType>? = try await getItem(forKey: compositePrimaryKey)
        
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
                                                  historicalItem: historicalItem)
        } catch SmokeDynamoDBError.conditionalCheckFailed {
            // try again
            return try await conditionallyUpdateItemWithHistoricalRow(compositePrimaryKey: compositePrimaryKey,
                                                                      primaryItemProvider: primaryItemProvider,
                                                                      historicalItemProvider: historicalItemProvider, withRetries: retries - 1)
        }
        
        return updatedItem
    }
#endif
    
    func conditionallyUpdateItemWithHistoricalRow<AttributesType, ItemType>(
            compositePrimaryKey: CompositePrimaryKey<AttributesType>,
            primaryItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>>,
            historicalItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
            withRetries retries: Int = 10) -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>> {
        guard retries > 0 else {
            let error = SmokeDynamoDBError.concurrencyError(partitionKey: compositePrimaryKey.partitionKey,
                                                            sortKey: compositePrimaryKey.sortKey,
                                                            message: "Unable to complete request to update versioned item in specified number of attempts")
            
            let promise = self.eventLoop.makePromise(of: TypedDatabaseItem<AttributesType, ItemType>.self)
            promise.fail(error)
            return promise.futureResult
        }
        
        return getItem(forKey: compositePrimaryKey).flatMap { (existingItemOptional: TypedDatabaseItem<AttributesType, ItemType>?) in
            guard let existingItem = existingItemOptional else {
                let error = SmokeDynamoDBError.conditionalCheckFailed(partitionKey: compositePrimaryKey.partitionKey,
                                                          sortKey: compositePrimaryKey.sortKey,
                                                          message: "Item not present in database.")
                
                let promise = self.eventLoop.makePromise(of: TypedDatabaseItem<AttributesType, ItemType>.self)
                promise.fail(error)
                return promise.futureResult
            }
            
            let updatedItemFuture = primaryItemProvider(existingItem)
            
            return updatedItemFuture.flatMap { updatedItem in
                let historicalItem = historicalItemProvider(updatedItem)
                
                let errorHandler: ((Error) -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>>) = self.getUpdateItemErrorHandler(
                    forPrimaryKey: compositePrimaryKey,
                    primaryItemProvider: primaryItemProvider,
                    historicalItemProvider: historicalItemProvider,
                    withRetries: retries)

                return self.updateItemWithHistoricalRow(primaryItem: updatedItem,
                                                        existingItem: existingItem,
                                                        historicalItem: historicalItem)
                    .map { _ in
                        // return the updated item
                        return updatedItem
                    } .flatMapError(errorHandler)
            }
        }
    }
    
    /**
     Create a error handler to pass to `updateItemWithHistoricalRowAsync` from
     `conditionallyUpdateItemWithHistoricalRowAsync`, capturing the current updatedItem and passing it to the outer
     completion handler when `updateItemWithHistoricalRowAsync` completes with no error.
     */
    private func getUpdateItemErrorHandler<AttributesType, ItemType>(
            forPrimaryKey compositePrimaryKey: CompositePrimaryKey<AttributesType>,
            primaryItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) throws -> TypedDatabaseItem<AttributesType, ItemType>,
            historicalItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
            withRetries retries: Int) -> (Error) -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>> {
        func handleUpdateItemError(error: Error) -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>> {
            // If there was a failure due to conditionalCheckFailed
            if case SmokeDynamoDBError.conditionalCheckFailed = error {
                // try again
                return conditionallyUpdateItemWithHistoricalRow(compositePrimaryKey: compositePrimaryKey,
                                                                primaryItemProvider: primaryItemProvider,
                                                                historicalItemProvider: historicalItemProvider, withRetries: retries - 1)
            } else {
                // propagate the error as it's not an error causing a retry
                let promise = self.eventLoop.makePromise(of: TypedDatabaseItem<AttributesType, ItemType>.self)
                promise.fail(error)
                return promise.futureResult
            }
        }

        return handleUpdateItemError
    }
    
    private func getUpdateItemErrorHandler<AttributesType, ItemType>(
            forPrimaryKey compositePrimaryKey: CompositePrimaryKey<AttributesType>,
            primaryItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>>,
            historicalItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) -> TypedDatabaseItem<AttributesType, ItemType>,
            withRetries retries: Int) -> (Error) -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>> {
        func handleUpdateItemError(error: Error) -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>> {
            // If there was a failure due to conditionalCheckFailed
            if case SmokeDynamoDBError.conditionalCheckFailed = error {
                // try again
                return conditionallyUpdateItemWithHistoricalRow(compositePrimaryKey: compositePrimaryKey,
                                                                primaryItemProvider: primaryItemProvider,
                                                                historicalItemProvider: historicalItemProvider, withRetries: retries - 1)
            } else {
                // propagate the error as it's not an error causing a retry
                let promise = self.eventLoop.makePromise(of: TypedDatabaseItem<AttributesType, ItemType>.self)
                promise.fail(error)
                return promise.futureResult
            }
        }

        return handleUpdateItemError
    }
}
