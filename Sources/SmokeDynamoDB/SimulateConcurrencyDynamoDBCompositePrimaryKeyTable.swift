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
//  SimulateConcurrencyDynamoDBCompositePrimaryKeyTable.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel
import CollectionConcurrencyKit

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
    
    public func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        // if there are still modifications to be made and there is an existing row
        if simulateOnInsertItem && previousConcurrencyModifications < simulateConcurrencyModifications {
            // insert an item so the conditional check will fail
            try await wrappedDynamoDBTable.insertItem(item)
            
            self.previousConcurrencyModifications += 1
            
            // then delegate to the wrapped implementation
            try await self.wrappedDynamoDBTable.insertItem(item)
        }
        
        // otherwise just delegate to the wrapped implementation
        try await wrappedDynamoDBTable.insertItem(item)
    }
    
    public func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        try await wrappedDynamoDBTable.clobberItem(item)
    }
    
    public func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     existingItem: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        
        // if there are still modifications to be made and there is an existing row
        if simulateOnUpdateItem && previousConcurrencyModifications < simulateConcurrencyModifications {
            try await wrappedDynamoDBTable.updateItem(newItem: existingItem.createUpdatedItem(withValue: existingItem.rowValue),
                                                      existingItem: existingItem)
            
            self.previousConcurrencyModifications += 1
            
            // then delegate to the wrapped implementation
            try await self.wrappedDynamoDBTable.updateItem(newItem: newItem, existingItem: existingItem)
        }
        
        // otherwise just delegate to the wrapped implementation
        try await wrappedDynamoDBTable.updateItem(newItem: newItem, existingItem: existingItem)
    }
    
    public func monomorphicBulkWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws {
        try await entries.asyncForEach { entry in
            switch entry {
            case .update(new: let new, existing: let existing):
                return try await updateItem(newItem: new, existingItem: existing)
            case .insert(new: let new):
                return try await insertItem(new)
            case .deleteAtKey(key: let key):
                return try await deleteItem(forKey: key)
            case .deleteItem(existing: let existing):
                return try await deleteItem(existingItem: existing)
            }
        }
    }
    
    public func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) async throws
    -> TypedDatabaseItem<AttributesType, ItemType>? {
        // simply delegate to the wrapped implementation
        return try await wrappedDynamoDBTable.getItem(forKey: key)
    }
    
    public func getItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>]) async throws
    -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType] {
        // simply delegate to the wrapped implementation
        return try await wrappedDynamoDBTable.getItems(forKeys: keys)
    }
    
    public func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) async throws {
        // simply delegate to the wrapped implementation
        return try await wrappedDynamoDBTable.deleteItem(forKey: key)
    }
    
    public func deleteItem<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) async throws
            where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        return try await wrappedDynamoDBTable.deleteItem(existingItem: existingItem)
    }
    
    public func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws {
        return try await wrappedDynamoDBTable.deleteItems(forKeys: keys)
    }
    
    public func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType]) async throws {
        return try await wrappedDynamoDBTable.deleteItems(existingItems: existingItems)
    }
    
    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    consistentRead: Bool) async throws
    -> [ReturnedType] {
        // simply delegate to the wrapped implementation
        return try await wrappedDynamoDBTable.query(forPartitionKey: partitionKey,
                                                    sortKeyCondition: sortKeyCondition,
                                                    consistentRead: consistentRead)
    }
    
    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    limit: Int?,
                                                                    exclusiveStartKey: String?,
                                                                    consistentRead: Bool) async throws
    -> ([ReturnedType], String?) {
        // simply delegate to the wrapped implementation
        return try await wrappedDynamoDBTable.query(forPartitionKey: partitionKey,
                                                    sortKeyCondition: sortKeyCondition,
                                                    limit: limit,
                                                    exclusiveStartKey: exclusiveStartKey,
                                                    consistentRead: consistentRead)
    }
    
    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    limit: Int?,
                                                                    scanIndexForward: Bool,
                                                                    exclusiveStartKey: String?,
                                                                    consistentRead: Bool) async throws
    -> ([ReturnedType], String?) {
        // simply delegate to the wrapped implementation
        return try await wrappedDynamoDBTable.query(forPartitionKey: partitionKey,
                                                    sortKeyCondition: sortKeyCondition,
                                                    limit: limit,
                                                    scanIndexForward: scanIndexForward,
                                                    exclusiveStartKey: exclusiveStartKey,
                                                    consistentRead: consistentRead)
    }
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
    -> [ReturnedType] {
        // simply delegate to the wrapped implementation
        return try await wrappedDynamoDBTable.execute(partitionKeys: partitionKeys,
                                                      attributesFilter: attributesFilter,
                                                      additionalWhereClause: additionalWhereClause)
    }
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
    -> ([ReturnedType], String?) {
        // simply delegate to the wrapped implementation
        return try await wrappedDynamoDBTable.execute(partitionKeys: partitionKeys,
                                                      attributesFilter: attributesFilter,
                                                      additionalWhereClause: additionalWhereClause,
                                                      nextToken: nextToken)
    }
    
    public func monomorphicGetItems<AttributesType, ItemType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws
    -> [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>] {
        // simply delegate to the wrapped implementation
        return try await wrappedDynamoDBTable.monomorphicGetItems(forKeys: keys)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        // simply delegate to the wrapped implementation
        return try await wrappedDynamoDBTable.monomorphicExecute(partitionKeys: partitionKeys,
                                                                 attributesFilter: attributesFilter,
                                                                 additionalWhereClause: additionalWhereClause)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
    -> ([TypedDatabaseItem<AttributesType, ItemType>], String?) {
        // simply delegate to the wrapped implementation
        return try await wrappedDynamoDBTable.monomorphicExecute(partitionKeys: partitionKeys,
                                                                 attributesFilter: attributesFilter,
                                                                 additionalWhereClause: additionalWhereClause,
                                                                 nextToken: nextToken)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                           sortKeyCondition: AttributeCondition?,
                                                           consistentRead: Bool) async throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        // simply delegate to the wrapped implementation
        return try await wrappedDynamoDBTable.monomorphicQuery(forPartitionKey: partitionKey,
                                                               sortKeyCondition: sortKeyCondition,
                                                               consistentRead: consistentRead)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                           sortKeyCondition: AttributeCondition?,
                                                           limit: Int?,
                                                           scanIndexForward: Bool,
                                                           exclusiveStartKey: String?,
                                                           consistentRead: Bool) async throws
    -> ([TypedDatabaseItem<AttributesType, ItemType>], String?) {
        // simply delegate to the wrapped implementation
        return try await wrappedDynamoDBTable.monomorphicQuery(forPartitionKey: partitionKey,
                                                               sortKeyCondition: sortKeyCondition,
                                                               limit: limit,
                                                               scanIndexForward: scanIndexForward,
                                                               exclusiveStartKey: exclusiveStartKey,
                                                               consistentRead: consistentRead)
    }
}
