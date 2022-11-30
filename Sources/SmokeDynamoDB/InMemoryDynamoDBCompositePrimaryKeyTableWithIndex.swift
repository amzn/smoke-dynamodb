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
//  InMemoryDynamoDBCompositePrimaryKeyTableWithIndex.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel
import CollectionConcurrencyKit

public enum GSIError: Error {
    case unknownIndex(name: String)
}

public struct InMemoryDynamoDBCompositePrimaryKeyTableWithIndex<GSILogic: DynamoDBCompositePrimaryKeyGSILogic>: DynamoDBCompositePrimaryKeyTable {
    public let primaryTable: InMemoryDynamoDBCompositePrimaryKeyTable
    public let gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable
    
    private let gsiName: String
    private let gsiLogic: GSILogic
    
    public init(gsiName: String,
                gsiLogic: GSILogic,
                executeItemFilter: ExecuteItemFilterType? = nil) {
        self.gsiName = gsiName
        self.gsiLogic = gsiLogic
        self.primaryTable = InMemoryDynamoDBCompositePrimaryKeyTable(executeItemFilter: executeItemFilter)
        self.gsiDataStore = InMemoryDynamoDBCompositePrimaryKeyTable(executeItemFilter: executeItemFilter)
    }
    
    public func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                     tableOverrides: WritableTableOverrides?) async throws {
        try await self.primaryTable.insertItem(item, tableOverrides: tableOverrides)
        try await self.gsiLogic.onInsertItem(item, gsiDataStore: self.gsiDataStore)
    }
    
    public func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                      tableOverrides: WritableTableOverrides?) async throws {
        try await self.primaryTable.clobberItem(item, tableOverrides: tableOverrides)
        try await self.gsiLogic.onClobberItem(item, gsiDataStore: self.gsiDataStore)
    }
    
    public func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     tableOverrides: WritableTableOverrides?) async throws {
        try await self.primaryTable.updateItem(newItem: newItem, existingItem: existingItem, tableOverrides: tableOverrides)
        try await self.gsiLogic.onUpdateItem(newItem: newItem, existingItem: existingItem, gsiDataStore: self.gsiDataStore)
    }
    
    public func monomorphicBulkWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>],
                                                               tableOverrides: WritableTableOverrides?) async throws {
        try await entries.asyncForEach { entry in
            switch entry {
            case .update(new: let new, existing: let existing):
                return try await updateItem(newItem: new, existingItem: existing, tableOverrides: tableOverrides)
            case .insert(new: let new):
                return try await insertItem(new, tableOverrides: tableOverrides)
            case .deleteAtKey(key: let key):
                return try await deleteItem(forKey: key, tableOverrides: tableOverrides)
            case .deleteItem(existing: let existing):
                return try await deleteItem(existingItem: existing, tableOverrides: tableOverrides)
            }
        }
    }
    
    public func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>,
                                                  tableOverrides: ReadableTableOverrides?) async throws
    -> TypedDatabaseItem<AttributesType, ItemType>? {
        return try await self.primaryTable.getItem(forKey: key, tableOverrides: tableOverrides)
    }
    
    public func getItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>],
        tableOverrides: ReadableTableOverrides?) async throws
    -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType] {
        return try await self.primaryTable.getItems(forKeys: keys, tableOverrides: tableOverrides)
    }
    
    public func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>,
                                           tableOverrides: WritableTableOverrides?) async throws {
        try await self.primaryTable.deleteItem(forKey: key, tableOverrides: tableOverrides)
        try await self.gsiLogic.onDeleteItem(forKey: key, gsiDataStore: self.gsiDataStore)
    }
    
    public func deleteItem<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     tableOverrides: WritableTableOverrides?) async throws {
        try await self.primaryTable.deleteItem(existingItem: existingItem, tableOverrides: tableOverrides)
        try await self.gsiLogic.onDeleteItem(forKey: existingItem.compositePrimaryKey, gsiDataStore: self.gsiDataStore)
    }
    
    public func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>],
                                            tableOverrides: WritableTableOverrides?) async throws {
        try await self.primaryTable.deleteItems(forKeys: keys, tableOverrides: tableOverrides)

        try await keys.asyncForEach { key in
            try await self.gsiLogic.onDeleteItem(forKey: key, gsiDataStore: self.gsiDataStore)
        }
    }
    
    public func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType],
                                                    tableOverrides: WritableTableOverrides?) async throws {
        
        try await self.primaryTable.deleteItems(existingItems: existingItems, tableOverrides: tableOverrides)
        
        try await existingItems.asyncForEach { existingItem in
            try await self.gsiLogic.onDeleteItem(forKey: existingItem.compositePrimaryKey, gsiDataStore: self.gsiDataStore)
        }
    }
    
    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    tableOverrides: ReadableTableOverrides?) async throws
    -> [ReturnedType] {
        // if this is querying an index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }
            
            // query on the index
            return try await self.gsiDataStore.query(forPartitionKey: partitionKey,
                                                     sortKeyCondition: sortKeyCondition,
                                                     tableOverrides: tableOverrides)
        }
        
        // query on the main table
        return try await self.primaryTable.query(forPartitionKey: partitionKey,
                                                 sortKeyCondition: sortKeyCondition,
                                                 tableOverrides: tableOverrides)
    }
    
    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    limit: Int?,
                                                                    exclusiveStartKey: String?,
                                                                    tableOverrides: ReadableTableOverrides?) async throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        // if this is querying an index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }
            
            // query on the index
            return try await self.gsiDataStore.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                     limit: limit, exclusiveStartKey: exclusiveStartKey, tableOverrides: tableOverrides)
        }
        
        // query on the main table
        return try await self.primaryTable.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                 limit: limit, exclusiveStartKey: exclusiveStartKey, tableOverrides: tableOverrides)
    }
    
    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    limit: Int?,
                                                                    scanIndexForward: Bool,
                                                                    exclusiveStartKey: String?,
                                                                    tableOverrides: ReadableTableOverrides?) async throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        // if this is querying an index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }
            
            // query on the index
            return try await self.gsiDataStore.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                     limit: limit, scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                                     tableOverrides: tableOverrides)
        }
        
        // query on the main table
        return try await self.primaryTable.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                 limit: limit, scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                                 tableOverrides: tableOverrides)
    }
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?,
        tableOverrides: ReadableTableOverrides?) async throws
    -> [ReturnedType] {
        // if this is executing on index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }
            
            // execute on the index
            return try await self.gsiDataStore.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                       additionalWhereClause: additionalWhereClause,
                                                       tableOverrides: tableOverrides)
        }
        
        // execute on the main table
        return try await self.primaryTable.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                   additionalWhereClause: additionalWhereClause,
                                                   tableOverrides: tableOverrides)
    }
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?,
        tableOverrides: ReadableTableOverrides?) async throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        // if this is executing on index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }
            
            // execute on the index
            return try await self.gsiDataStore.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                       additionalWhereClause: additionalWhereClause, nextToken: nextToken,
                                                       tableOverrides: tableOverrides)
        }
        
        // execute on the main table
        return try await self.primaryTable.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                   additionalWhereClause: additionalWhereClause, nextToken: nextToken,
                                                   tableOverrides: tableOverrides)
    }
    
    public func monomorphicGetItems<AttributesType, ItemType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>],
        tableOverrides: ReadableTableOverrides?) async throws
    -> [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>] {
        return try await self.primaryTable.monomorphicGetItems(forKeys: keys, tableOverrides: tableOverrides)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                           sortKeyCondition: AttributeCondition?,
                                                           tableOverrides: ReadableTableOverrides?) async throws
           -> [TypedDatabaseItem<AttributesType, ItemType>] {
        // if this is querying an index
        if let indexName = AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }
            
            // query on the index
            return try await self.gsiDataStore.monomorphicQuery(forPartitionKey: partitionKey,
                                                                sortKeyCondition: sortKeyCondition,
                                                                tableOverrides: tableOverrides)
        }
        
        // query on the main table
        return try await self.primaryTable.monomorphicQuery(forPartitionKey: partitionKey,
                                                            sortKeyCondition: sortKeyCondition,
                                                            tableOverrides: tableOverrides)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                           sortKeyCondition: AttributeCondition?,
                                                           limit: Int?,
                                                           scanIndexForward: Bool,
                                                           exclusiveStartKey: String?,
                                                           tableOverrides: ReadableTableOverrides?) async throws
       -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?) {
        // if this is querying an index
        if let indexName = AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }
            
            // query on the index
            return try await self.gsiDataStore.monomorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                                limit: limit, scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                                                tableOverrides: tableOverrides)
        }
        
        // query on the main table
        return try await self.primaryTable.monomorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                            limit: limit, scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                                            tableOverrides: tableOverrides)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?,
        tableOverrides: ReadableTableOverrides?) async throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        // if this is executing on index
        if let indexName = AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }
            
            // execute on the index
            return try await self.gsiDataStore.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                                  additionalWhereClause: additionalWhereClause,
                                                                  tableOverrides: tableOverrides)
        }
        
        // execute on the main table
        return try await self.primaryTable.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                              additionalWhereClause: additionalWhereClause,
                                                              tableOverrides: tableOverrides)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?,
        tableOverrides: ReadableTableOverrides?) async throws
    -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?) {
        // if this is executing on index
        if let indexName = AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                throw GSIError.unknownIndex(name: indexName)
            }
            
            // execute on the index
            return try await self.gsiDataStore.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                                  additionalWhereClause: additionalWhereClause, nextToken: nextToken,
                                                                  tableOverrides: tableOverrides)
        }
        
        // execute on the main table
        return try await self.primaryTable.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                              additionalWhereClause: additionalWhereClause, nextToken: nextToken,
                                                              tableOverrides: tableOverrides)
    }
}
