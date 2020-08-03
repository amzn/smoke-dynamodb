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
//  SimulateConcurrencyDynamoDBCompositePrimaryKeyTable.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel

/**
 Implementation of the DynamoDBTable protocol that simulates concurrent access
 to a database by incrementing a row's version every time it is added for
 a specified number of requests.
 */
public class SimulateConcurrencyDynamoDBCompositePrimaryKeyTable: DynamoDBCompositePrimaryKeyTable {
    let wrappedDynamoDBTable: DynamoDBCompositePrimaryKeyTable
    let simulateConcurrencyModifications: Int
    var previousConcurrencyModifications: Int
    let simulateOnInsertItem: Bool
    let simulateOnUpdateItem: Bool
    
    /**
     Initializer.
 
     - Parameters:
        - wrappedDynamoDBTable: The underlying DynamoDBTable used by this implementation.
        - simulateConcurrencyModifications: the number of get requests to simulate concurrency for.
        - simulateOnInsertItem: if this instance should simulate concurrency on insertItem.
        - simulateOnUpdateItem: if this instance should simulate concurrency on updateItem.
     */
    public init(wrappedDynamoDBTable: DynamoDBCompositePrimaryKeyTable, simulateConcurrencyModifications: Int,
                simulateOnInsertItem: Bool = true, simulateOnUpdateItem: Bool = true) {
        self.wrappedDynamoDBTable = wrappedDynamoDBTable
        self.simulateConcurrencyModifications = simulateConcurrencyModifications
        self.previousConcurrencyModifications = 0
        self.simulateOnInsertItem = simulateOnInsertItem
        self.simulateOnUpdateItem = simulateOnUpdateItem
    }
    
    public func insertItemSync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws {
        // if there are still modifications to be made and there is an existing row
        if simulateOnInsertItem && previousConcurrencyModifications < simulateConcurrencyModifications {
            // insert an item so the conditional check will fail
            try wrappedDynamoDBTable.insertItemSync(item)
            previousConcurrencyModifications += 1
        }
        
        // then delegate to the wrapped implementation
        try wrappedDynamoDBTable.insertItemSync(item)
    }
    
    public func insertItemAsync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                          completion: @escaping (Error?) -> ())
        throws where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            do {
                try insertItemSync(item)
                
                completion(nil)
            } catch {
                completion(error)
            }
    }
    
    public func clobberItemSync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws {
        try wrappedDynamoDBTable.clobberItemSync(item)
    }
    
    public func clobberItemAsync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                           completion: @escaping (Error?) -> ())
        throws where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            do {
                try wrappedDynamoDBTable.clobberItemSync(item)
                
                completion(nil)
            } catch {
                completion(error)
            }
    }
    
    public func updateItemSync<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                         existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws {
        
        // if there are still modifications to be made and there is an existing row
        if simulateOnUpdateItem && previousConcurrencyModifications < simulateConcurrencyModifications {
            try wrappedDynamoDBTable.updateItemSync(newItem: existingItem.createUpdatedItem(withValue: existingItem.rowValue),
                                               existingItem: existingItem)
            previousConcurrencyModifications += 1
        }
        
        // then delegate to the wrapped implementation
        try wrappedDynamoDBTable.updateItemSync(newItem: newItem, existingItem: existingItem)
    }
    
    public func updateItemAsync<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                          existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                          completion: @escaping (Error?) -> ())
        throws where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            do {
                try updateItemSync(newItem: newItem, existingItem: existingItem)
                
                completion(nil)
            } catch {
                completion(error)
            }
    }
    
    public func getItemSync<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) throws
        -> TypedDatabaseItem<AttributesType, ItemType>? {
            // simply delegate to the wrapped implementation
            return try wrappedDynamoDBTable.getItemSync(forKey: key)
    }
    
    public func getItemAsync<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>,
                                                       completion: @escaping (SmokeDynamoDBErrorResult<TypedDatabaseItem<AttributesType, ItemType>?>) -> ())
        throws where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            do {
                let item: TypedDatabaseItem<AttributesType, ItemType>? = try getItemSync(forKey: key)
                
                completion(.success(item))
            } catch {
                completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
            }
    }
    
    public func deleteItemSync<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws {
        // simply delegate to the wrapped implementation
        try wrappedDynamoDBTable.deleteItemSync(forKey: key)
    }
    
    public func deleteItemAsync<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>,
                                                completion: @escaping (Error?) -> ())
        throws where AttributesType: PrimaryKeyAttributes {
            do {
                try deleteItemSync(forKey: key)
                
                completion(nil)
            } catch {
                completion(error)
            }
    }
    
    public func deleteItemSync<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        try wrappedDynamoDBTable.deleteItemSync(existingItem: existingItem)
    }
    
    public func deleteItemAsync<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                          completion: @escaping (Error?) -> ()) throws
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        do {
            try deleteItemSync(existingItem: existingItem)
            
            completion(nil)
        } catch {
            completion(error)
        }
    }
    
    public func querySync<AttributesType, PossibleTypes>(forPartitionKey partitionKey: String,
                                                         sortKeyCondition: AttributeCondition?) throws
        -> [PolymorphicDatabaseItem<AttributesType, PossibleTypes>]
        where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            // simply delegate to the wrapped implementation
            return try wrappedDynamoDBTable.querySync(forPartitionKey: partitionKey,
                                                     sortKeyCondition: sortKeyCondition)
    }
    
    public func queryAsync<AttributesType, PossibleTypes>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            completion: @escaping (SmokeDynamoDBErrorResult<[PolymorphicDatabaseItem<AttributesType, PossibleTypes>]>) -> ())
        throws where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            do {
                let items: [PolymorphicDatabaseItem<AttributesType, PossibleTypes>] =
                    try wrappedDynamoDBTable.querySync(forPartitionKey: partitionKey,
                                                      sortKeyCondition: sortKeyCondition)
                
                completion(.success(items))
            } catch {
                completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
            }
    }
    
    public func querySync<AttributesType, PossibleTypes>(forPartitionKey partitionKey: String,
                                                         sortKeyCondition: AttributeCondition?,
                                                         limit: Int?,
                                                         exclusiveStartKey: String?) throws
        -> ([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)
        where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            // simply delegate to the wrapped implementation
            return try wrappedDynamoDBTable.querySync(forPartitionKey: partitionKey,
                                                     sortKeyCondition: sortKeyCondition,
                                                     limit: limit,
                                                     exclusiveStartKey: exclusiveStartKey)
    }
    
    public func querySync<AttributesType, PossibleTypes>(forPartitionKey partitionKey: String,
                                                         sortKeyCondition: AttributeCondition?,
                                                         limit: Int?,
                                                         scanIndexForward: Bool,
                                                         exclusiveStartKey: String?) throws
        -> ([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)
        where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            // simply delegate to the wrapped implementation
            return try wrappedDynamoDBTable.querySync(forPartitionKey: partitionKey,
                                                     sortKeyCondition: sortKeyCondition,
                                                     limit: limit,
                                                     scanIndexForward: scanIndexForward,
                                                     exclusiveStartKey: exclusiveStartKey)
    }
    
    public func queryAsync<AttributesType, PossibleTypes>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?,
            exclusiveStartKey: String?,
            completion: @escaping (SmokeDynamoDBErrorResult<([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)>) -> ())
        throws where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            do {
                let result: ([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?) =
                    try wrappedDynamoDBTable.querySync(forPartitionKey: partitionKey,
                                                      sortKeyCondition: sortKeyCondition,
                                                      limit: limit,
                                                      exclusiveStartKey: exclusiveStartKey)
                
                completion(.success(result))
            } catch {
                completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
            }
    }
    
    public func queryAsync<AttributesType, PossibleTypes>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?,
            scanIndexForward: Bool,
            exclusiveStartKey: String?,
            completion: @escaping (SmokeDynamoDBErrorResult<([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)>) -> ())
        throws where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            do {
                let result: ([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?) =
                    try wrappedDynamoDBTable.querySync(forPartitionKey: partitionKey,
                                                      sortKeyCondition: sortKeyCondition,
                                                      limit: limit,
                                                      scanIndexForward: scanIndexForward,
                                                      exclusiveStartKey: exclusiveStartKey)
                
                completion(.success(result))
            } catch {
                completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
            }
    }
    
    public func monomorphicQuerySync<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                               sortKeyCondition: AttributeCondition?) throws
    -> [TypedDatabaseItem<AttributesType, ItemType>]
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        // simply delegate to the wrapped implementation
        return try wrappedDynamoDBTable.monomorphicQuerySync(forPartitionKey: partitionKey,
                                                             sortKeyCondition: sortKeyCondition)
    }
    
    public func monomorphicQueryAsync<AttributesType, ItemType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            completion: @escaping (SmokeDynamoDBErrorResult<[TypedDatabaseItem<AttributesType, ItemType>]>) -> ()) throws
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        do {
            let items: [TypedDatabaseItem<AttributesType, ItemType>] =
                try wrappedDynamoDBTable.monomorphicQuerySync(forPartitionKey: partitionKey,
                                                              sortKeyCondition: sortKeyCondition)
            
            completion(.success(items))
        } catch {
            completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
        }
    }
    
    public func monomorphicQuerySync<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                               sortKeyCondition: AttributeCondition?,
                                                               limit: Int?,
                                                               scanIndexForward: Bool,
                                                               exclusiveStartKey: String?) throws
    -> ([TypedDatabaseItem<AttributesType, ItemType>], String?)
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        // simply delegate to the wrapped implementation
        return try wrappedDynamoDBTable.monomorphicQuerySync(forPartitionKey: partitionKey,
                                                             sortKeyCondition: sortKeyCondition,
                                                             limit: limit,
                                                             scanIndexForward: scanIndexForward,
                                                             exclusiveStartKey: exclusiveStartKey)
    }
    
    public func monomorphicQueryAsync<AttributesType, ItemType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?,
            scanIndexForward: Bool,
            exclusiveStartKey: String?,
            completion: @escaping (SmokeDynamoDBErrorResult<([TypedDatabaseItem<AttributesType, ItemType>], String?)>) -> ()) throws
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        do {
            let result: ([TypedDatabaseItem<AttributesType, ItemType>], String?) =
                try wrappedDynamoDBTable.monomorphicQuerySync(forPartitionKey: partitionKey,
                                                               sortKeyCondition: sortKeyCondition,
                                                               limit: limit,
                                                               scanIndexForward: scanIndexForward,
                                                               exclusiveStartKey: exclusiveStartKey)
            
            completion(.success(result))
        } catch {
            completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
        }
    }
}
