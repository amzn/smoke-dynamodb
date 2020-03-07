// Copyright 2018-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
    func conditionallyUpdateItemSync<AttributesType, ItemType: Codable>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        updatedPayloadProvider: (ItemType) throws -> ItemType) throws {
        
        guard retries > 0 else {
            throw SmokeDynamoDBError.concurrencyError(partitionKey: key.partitionKey,
                                                    sortKey: key.sortKey,
                                                    message: "Unable to complete request to update versioned item in specified number of attempts")
        }
        
        guard let databaseItem: TypedDatabaseItem<AttributesType, ItemType> = try getItemSync(forKey: key) else {
            throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: key.partitionKey,
                                                          sortKey: key.sortKey,
                                                          message: "Item not present in database.")
        }
        
        let updatedPayload = try updatedPayloadProvider(databaseItem.rowValue)
        
        let updatedDatabaseItem = databaseItem.createUpdatedItem(withValue: updatedPayload)
        
        do {
            try updateItemSync(newItem: updatedDatabaseItem, existingItem: databaseItem)
        } catch SmokeDynamoDBError.conditionalCheckFailed(_) {
            return try conditionallyUpdateItemSync(forKey: key,
                                                   withRetries: retries - 1,
                                                   updatedPayloadProvider: updatedPayloadProvider)
        }
    }
    
    func conditionallyUpdateItemAsync<AttributesType, ItemType: Codable>(
        forKey key: CompositePrimaryKey<AttributesType>,
        withRetries retries: Int = 10,
        updatedPayloadProvider: @escaping (ItemType) throws -> ItemType,
        completion: @escaping (Error?) -> ()) throws {
        
        guard retries > 0 else {
            throw SmokeDynamoDBError.concurrencyError(partitionKey: key.partitionKey,
                                                    sortKey: key.sortKey,
                                                    message: "Unable to complete request to update versioned item in specified number of attempts")
        }
        
        func handleGetItemResult(result: SmokeDynamoDBErrorResult<TypedDatabaseItem<AttributesType, ItemType>?>) {
            switch result {
            case .success(let databaseItemOptional):
                guard let databaseItem = databaseItemOptional else {
                    let error = SmokeDynamoDBError.conditionalCheckFailed(partitionKey: key.partitionKey,
                                                                        sortKey: key.sortKey,
                                                                        message: "Item not present in database.")
                    return completion(error)
                }
                
                let updatedPayload: ItemType
                    
                do {
                    updatedPayload = try updatedPayloadProvider(databaseItem.rowValue)
                } catch {
                    return completion(error)
                }
        
                let updatedDatabaseItem = databaseItem.createUpdatedItem(withValue: updatedPayload)
                
                do {
                    try updateItemAsync(newItem: updatedDatabaseItem, existingItem: databaseItem) { error in
                        if let error = error, case SmokeDynamoDBError.conditionalCheckFailed = error {
                            do {
                                try self.conditionallyUpdateItemAsync(forKey: key,
                                                                      withRetries: retries - 1,
                                                                      updatedPayloadProvider: updatedPayloadProvider,
                                                                      completion: completion)
                            } catch {
                                completion(error)
                            }
                        } else {
                            completion(error)
                        }
                    }
                } catch {
                    completion(error)
                }
            case .failure(let error):
                completion(error)
            }
        }
        
        try getItemAsync(forKey: key,
                         completion: handleGetItemResult)
    }
}
