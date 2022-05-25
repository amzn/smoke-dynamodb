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
//  DynamoDBCompositePrimaryKeyTable+conditionallyUpdateItem.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import Logging
import DynamoDBModel
import NIO

public extension DynamoDBCompositePrimaryKeyTable {
    /**
     Method to conditionally update an item at the specified key for a number of retries.
     This method is useful for database rows that may be updated simultaneously by different clients
     and each client will only attempt to update based on the current row value.
     On each attempt, the updatedPayloadProvider will be passed the current row value. It can either
     generate an updated payload or fail with an error if an updated payload is not valid. If an updated
     payload is returned, this method will attempt to update the row. This update may fail due to
     concurrency, in which case the process will repeat until the retry limit has been reached.
 
     - Parameters:
         _: the key of the item to update
         withRetries: the number of times to attempt to retry the update before failing.
         updatedPayloadProvider: the provider that will return updated payloads.
     */
    func conditionallyUpdateItem<AttributesType, ItemType: Codable>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        updatedPayloadProvider: @escaping (ItemType) async throws -> ItemType) async throws {
            let updatedItemProvider: (TypedDatabaseItem<AttributesType, ItemType>) async throws -> TypedDatabaseItem<AttributesType, ItemType> = { existingItem in
                let updatedPayload = try await updatedPayloadProvider(existingItem.rowValue)
                return existingItem.createUpdatedItem(withValue: updatedPayload)
            }
            try await conditionallyUpdateItemInternal(
                forKey: key,
                withRetries: retries,
                updatedItemProvider: updatedItemProvider)
        }
    
    /**
     Method to conditionally update an item at the specified key for a number of retries.
     This method is useful for database rows that may be updated simultaneously by different clients
     and each client will only attempt to update based on the current row value.
     On each attempt, the updatedItemProvider will be passed the current row. It can either
     generate an updated row or fail with an error if an updated row is not valid. If an updated
     row is returned, this method will attempt to update the row. This update may fail due to
     concurrency, in which case the process will repeat until the retry limit has been reached.
     
     - Parameters:
         _: the key of the item to update
         withRetries: the number of times to attempt to retry the update before failing.
         updatedItemProvider: the provider that will return updated items.
     */
    func conditionallyUpdateItem<AttributesType, ItemType: Codable>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        updatedItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) async throws -> TypedDatabaseItem<AttributesType, ItemType>) async throws {
            try await conditionallyUpdateItemInternal(
                forKey: key,
                withRetries: retries,
                updatedItemProvider: updatedItemProvider)
        }
    
    private func conditionallyUpdateItemInternal<AttributesType, ItemType: Codable>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        updatedItemProvider: @escaping (TypedDatabaseItem<AttributesType, ItemType>) async throws
        -> TypedDatabaseItem<AttributesType, ItemType>) async throws {
            guard retries > 0 else {
                throw SmokeDynamoDBError.concurrencyError(partitionKey: key.partitionKey,
                                                          sortKey: key.sortKey,
                                                          message: "Unable to complete request to update versioned item in specified number of attempts")
            }
            
            let databaseItemOptional: TypedDatabaseItem<AttributesType, ItemType>? = try await getItem(forKey: key)
            
            guard let databaseItem = databaseItemOptional else {
                throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: key.partitionKey,
                                                                sortKey: key.sortKey,
                                                                message: "Item not present in database.")
            }
            
            let updatedDatabaseItem = try await updatedItemProvider(databaseItem)
            
            do {
                try await self.updateItem(newItem: updatedDatabaseItem, existingItem: databaseItem)
            } catch SmokeDynamoDBError.conditionalCheckFailed {
                // try again
                return try await self.conditionallyUpdateItem(forKey: key,
                                                              withRetries: retries - 1,
                                                              updatedItemProvider: updatedItemProvider)
            }
        }
}
