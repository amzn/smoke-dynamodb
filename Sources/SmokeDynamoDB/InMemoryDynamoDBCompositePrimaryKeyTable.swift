// swiftlint:disable cyclomatic_complexity
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
//  InMemoryDynamoDBCompositePrimaryKeyTable.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel

public protocol PolymorphicOperationReturnTypeConvertable {
    var createDate: Foundation.Date { get }
    var rowStatus: RowStatus { get }
    
    var rowTypeIdentifier: String { get }
}

extension TypedDatabaseItem: PolymorphicOperationReturnTypeConvertable {
    public var rowTypeIdentifier: String {
        return getTypeRowIdentifier(type: RowType.self)
    }
}

public typealias ExecuteItemFilterType = (String, String, String, PolymorphicOperationReturnTypeConvertable)
    -> Bool

public struct InMemoryDynamoDBCompositePrimaryKeyTable: DynamoDBCompositePrimaryKeyTable {
    internal let storeWrapper: InMemoryDynamoDBCompositePrimaryKeyTableStore
    
    public init(executeItemFilter: ExecuteItemFilterType? = nil) {
        self.storeWrapper = InMemoryDynamoDBCompositePrimaryKeyTableStore(executeItemFilter: executeItemFilter)
    }
    
    internal init(storeWrapper: InMemoryDynamoDBCompositePrimaryKeyTableStore) {
        self.storeWrapper = storeWrapper
    }
    
    public var store: [String: [String: PolymorphicOperationReturnTypeConvertable]] {
        get async {
            return await self.storeWrapper.store
        }
    }

    public func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                     tableOverrides: WritableTableOverrides?) async throws {
        return try await storeWrapper.insertItem(item)
    }

    public func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                      tableOverrides: WritableTableOverrides?) async throws {
        return try await storeWrapper.clobberItem(item)
    }

    public func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     tableOverrides: WritableTableOverrides?) async throws {
        return try await storeWrapper.updateItem(newItem: newItem, existingItem: existingItem)
    }
    
    public func monomorphicBulkWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>],
                                                               tableOverrides: WritableTableOverrides?) async throws {
        try await storeWrapper.monomorphicBulkWrite(entries)
    }

    public func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>,
                                                  tableOverrides: ReadableTableOverrides?) async throws
    -> TypedDatabaseItem<AttributesType, ItemType>? {
        return try await storeWrapper.getItem(forKey: key)
    }
    
    public func getItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>],
        tableOverrides: ReadableTableOverrides?) async throws
    -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType] {
        return try await storeWrapper.getItems(forKeys: keys)
    }

    public func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>,
                                           tableOverrides: WritableTableOverrides?) async throws {
        return try await storeWrapper.deleteItem(forKey: key)
    }
    
    public func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>],
                                            tableOverrides: WritableTableOverrides?) async throws {
        return try await storeWrapper.deleteItems(forKeys: keys)
    }
    
    public func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType],
                                                    tableOverrides: WritableTableOverrides?) async throws {
        return try await storeWrapper.deleteItems(existingItems: existingItems)
    }
    
    public func deleteItem<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     tableOverrides: WritableTableOverrides?) async throws {
        return try await storeWrapper.deleteItem(existingItem: existingItem)
    }

    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    tableOverrides: ReadableTableOverrides?) async throws
    -> [ReturnedType] {
        return try await storeWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition)
    }
    
    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    limit: Int?,
                                                                    exclusiveStartKey: String?,
                                                                    tableOverrides: ReadableTableOverrides?) async throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        return try await storeWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                            limit: limit, exclusiveStartKey: exclusiveStartKey)
    }

    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    limit: Int?,
                                                                    scanIndexForward: Bool,
                                                                    exclusiveStartKey: String?,
                                                                    tableOverrides: ReadableTableOverrides?) async throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        return try await storeWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                            limit: limit, scanIndexForward: scanIndexForward,
                                            exclusiveStartKey: exclusiveStartKey)
    }
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?,
        tableOverrides: ReadableTableOverrides?) async throws
    -> [ReturnedType] {
        return try await storeWrapper.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                              additionalWhereClause: additionalWhereClause)
    }
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?,
        tableOverrides: ReadableTableOverrides?) async throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        return try await storeWrapper.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                              additionalWhereClause: additionalWhereClause, nextToken: nextToken)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?,
        tableOverrides: ReadableTableOverrides?) async throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        return try await storeWrapper.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                         additionalWhereClause: additionalWhereClause)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?,
        tableOverrides: ReadableTableOverrides?) async throws
    -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?) {
        return try await storeWrapper.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                               additionalWhereClause: additionalWhereClause, nextToken: nextToken)
    }
    
    public func monomorphicGetItems<AttributesType, ItemType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>],
        tableOverrides: ReadableTableOverrides?) async throws
    -> [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>] {
        return try await storeWrapper.monomorphicGetItems(forKeys: keys)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                           sortKeyCondition: AttributeCondition?,
                                                           tableOverrides: ReadableTableOverrides?) async throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        return try await storeWrapper.monomorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                           sortKeyCondition: AttributeCondition?,
                                                           limit: Int?,
                                                           scanIndexForward: Bool,
                                                           exclusiveStartKey: String?,
                                                           tableOverrides: ReadableTableOverrides?) async throws
       -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?) {
        return try await storeWrapper.monomorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                       limit: limit, scanIndexForward: scanIndexForward,
                                                       exclusiveStartKey: exclusiveStartKey)
    }
}
