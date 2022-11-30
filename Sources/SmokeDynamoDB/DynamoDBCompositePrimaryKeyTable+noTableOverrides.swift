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
//  DynamoDBCompositePrimaryKeyTable+noTableOverrides.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel

public extension DynamoDBCompositePrimaryKeyTable {
    
    func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        return try await insertItem(item, tableOverrides: nil)
    }

    func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        return try await clobberItem(item, tableOverrides: nil)
    }

    func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                              existingItem: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        return try await updateItem(newItem: newItem, existingItem: existingItem, tableOverrides: nil)
    }
    
    func monomorphicBulkWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws {
        return try await monomorphicBulkWrite(entries, tableOverrides: nil)
    }

    func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) async throws
    -> TypedDatabaseItem<AttributesType, ItemType>? {
        return try await getItem(forKey: key, tableOverrides: nil)
    }
    
    func getItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>]) async throws
    -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType] {
        return try await getItems(forKeys: keys, tableOverrides: nil)
    }

    func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) async throws {
        return try await deleteItem(forKey: key, tableOverrides: nil)
    }
    
    func deleteItem<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        return try await deleteItem(existingItem: existingItem, tableOverrides: nil)
    }
    
    func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws {
        return try await deleteItems(forKeys: keys, tableOverrides: nil)
    }
    
    func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType]) async throws {
        return try await deleteItems(existingItems: existingItems, tableOverrides: nil)
    }

    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?) async throws
    -> [ReturnedType] {
        return try await query(forPartitionKey: partitionKey,
                               sortKeyCondition: sortKeyCondition,
                               tableOverrides: nil)
    }

    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             limit: Int?,
                                                             exclusiveStartKey: String?) async throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        return try await query(forPartitionKey: partitionKey,
                               sortKeyCondition: sortKeyCondition,
                               limit: limit,
                               exclusiveStartKey: exclusiveStartKey,
                               tableOverrides: nil)
    }
    
    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             limit: Int?,
                                                             scanIndexForward: Bool,
                                                             exclusiveStartKey: String?) async throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        return try await query(forPartitionKey: partitionKey,
                               sortKeyCondition: sortKeyCondition,
                               limit: limit,
                               scanIndexForward: scanIndexForward,
                               exclusiveStartKey: exclusiveStartKey,
                               tableOverrides: nil)
    }
    
    func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
    -> [ReturnedType] {
        return try await execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                 additionalWhereClause: additionalWhereClause, tableOverrides: nil)
    }
    
    func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        return try await execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                 additionalWhereClause: additionalWhereClause, nextToken: nextToken,
                                 tableOverrides: nil)
    }
    
    func monomorphicGetItems<AttributesType, ItemType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws
    -> [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>] {
        return try await monomorphicGetItems(forKeys: keys, tableOverrides: nil)
    }
    
    func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                    sortKeyCondition: AttributeCondition?) async throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        return try await monomorphicQuery(forPartitionKey: partitionKey,
                                          sortKeyCondition: sortKeyCondition,
                                          tableOverrides: nil)
    }

    func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                    sortKeyCondition: AttributeCondition?,
                                                    limit: Int?,
                                                    scanIndexForward: Bool,
                                                    exclusiveStartKey: String?) async throws
    -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?) {
        return try await monomorphicQuery(forPartitionKey: partitionKey,
                                          sortKeyCondition: sortKeyCondition,
                                          limit: limit,
                                          scanIndexForward: scanIndexForward,
                                          exclusiveStartKey: exclusiveStartKey,
                                          tableOverrides: nil)
    }
    
    func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        return try await monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                            additionalWhereClause: additionalWhereClause, tableOverrides: nil)
    }
    
    func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
    -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?) {
        return try await monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                            additionalWhereClause: additionalWhereClause, nextToken: nextToken,
                                            tableOverrides: nil)
    }
}
