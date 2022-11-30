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
//  InMemoryDynamoDBCompositePrimaryKeyTableStore.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel

internal actor InMemoryDynamoDBCompositePrimaryKeyTableStore {

    internal var store: [String: [String: PolymorphicOperationReturnTypeConvertable]] = [:]
    internal let executeItemFilter: ExecuteItemFilterType?

    init(executeItemFilter: ExecuteItemFilterType? = nil) {
        self.executeItemFilter = executeItemFilter
    }

    func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws {
        let partition = self.store[item.compositePrimaryKey.partitionKey]

        // if there is already a partition
        var updatedPartition: [String: PolymorphicOperationReturnTypeConvertable]
        if let partition = partition {
            updatedPartition = partition

            // if the row already exists
            if partition[item.compositePrimaryKey.sortKey] != nil {
                throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: item.compositePrimaryKey.partitionKey,
                                                                sortKey: item.compositePrimaryKey.sortKey,
                                                                message: "Row already exists.")
            }

            updatedPartition[item.compositePrimaryKey.sortKey] = item
        } else {
            updatedPartition = [item.compositePrimaryKey.sortKey: item]
        }

        self.store[item.compositePrimaryKey.partitionKey] = updatedPartition
    }

    func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws {
        let partition = self.store[item.compositePrimaryKey.partitionKey]

        // if there is already a partition
        var updatedPartition: [String: PolymorphicOperationReturnTypeConvertable]
        if let partition = partition {
            updatedPartition = partition

            updatedPartition[item.compositePrimaryKey.sortKey] = item
        } else {
            updatedPartition = [item.compositePrimaryKey.sortKey: item]
        }

        self.store[item.compositePrimaryKey.partitionKey] = updatedPartition
    }

    func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                              existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws {
        let partition = self.store[newItem.compositePrimaryKey.partitionKey]

        // if there is already a partition
        var updatedPartition: [String: PolymorphicOperationReturnTypeConvertable]
        if let partition = partition {
            updatedPartition = partition

            // if the row already exists
            if let actuallyExistingItem = partition[newItem.compositePrimaryKey.sortKey] {
                if existingItem.rowStatus.rowVersion != actuallyExistingItem.rowStatus.rowVersion ||
                    existingItem.createDate.iso8601 != actuallyExistingItem.createDate.iso8601 {
                    throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: newItem.compositePrimaryKey.partitionKey,
                                                                    sortKey: newItem.compositePrimaryKey.sortKey,
                                                                    message: "Trying to overwrite incorrect version.")
                }
            } else {
                throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: newItem.compositePrimaryKey.partitionKey,
                                                                sortKey: newItem.compositePrimaryKey.sortKey,
                                                                message: "Existing item does not exist.")
            }

            updatedPartition[newItem.compositePrimaryKey.sortKey] = newItem
        } else {
            throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: newItem.compositePrimaryKey.partitionKey,
                                                            sortKey: newItem.compositePrimaryKey.sortKey,
                                                            message: "Existing item does not exist.")
        }

        self.store[newItem.compositePrimaryKey.partitionKey] = updatedPartition
    }
    
    func monomorphicBulkWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) throws {
        try entries.forEach { entry in
            switch entry {
            case .update(new: let new, existing: let existing):
                return try self.updateItem(newItem: new, existingItem: existing)
            case .insert(new: let new):
                return try self.insertItem(new)
            case .deleteAtKey(key: let key):
                return try deleteItem(forKey: key)
            case .deleteItem(existing: let existing):
                return try deleteItem(existingItem: existing)
            }
        }
    }

    func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) throws
    -> TypedDatabaseItem<AttributesType, ItemType>? {
        if let partition = self.store[key.partitionKey] {

            guard let value = partition[key.sortKey] else {
                return nil
            }

            guard let item = value as? TypedDatabaseItem<AttributesType, ItemType> else {
                let foundType = type(of: value)
                let description = "Expected to decode \(TypedDatabaseItem<AttributesType, ItemType>.self). Instead found \(foundType)."
                let context = DecodingError.Context(codingPath: [], debugDescription: description)
                
                throw DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
            }

            return item
        }

        return nil
    }
    
    func getItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>]) throws
    -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType] {
        var map: [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType] = [:]
        
        try keys.forEach { key in
            if let partition = self.store[key.partitionKey] {

                guard let value = partition[key.sortKey] else {
                    return
                }
                
                let itemAsReturnedType: ReturnedType = try self.convertToQueryableType(input: value)
                
                map[key] = itemAsReturnedType
            }
        }
        
        return map
    }

    func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws {
        self.store[key.partitionKey]?[key.sortKey] = nil
    }
    
    func deleteItem<ItemType: DatabaseItem>(existingItem: ItemType) throws {
        let partition = self.store[existingItem.compositePrimaryKey.partitionKey]

        // if there is already a partition
        var updatedPartition: [String: PolymorphicOperationReturnTypeConvertable]
        if let partition = partition {
            updatedPartition = partition

            // if the row already exists
            if let actuallyExistingItem = partition[existingItem.compositePrimaryKey.sortKey] {
                if existingItem.rowStatus.rowVersion != actuallyExistingItem.rowStatus.rowVersion ||
                existingItem.createDate.iso8601 != actuallyExistingItem.createDate.iso8601 {
                    throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: existingItem.compositePrimaryKey.partitionKey,
                                                                    sortKey: existingItem.compositePrimaryKey.sortKey,
                                                                    message: "Trying to delete incorrect version.")
                }
            } else {
                throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: existingItem.compositePrimaryKey.partitionKey,
                                                                sortKey: existingItem.compositePrimaryKey.sortKey,
                                                                message: "Existing item does not exist.")
            }

            updatedPartition[existingItem.compositePrimaryKey.sortKey] = nil
        } else {
            throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: existingItem.compositePrimaryKey.partitionKey,
                                                            sortKey: existingItem.compositePrimaryKey.sortKey,
                                                            message: "Existing item does not exist.")
        }

        self.store[existingItem.compositePrimaryKey.partitionKey] = updatedPartition
    }
    
    func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) throws {
        try keys.forEach { key in
            try deleteItem(forKey: key)
        }
    }
    
    func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType]) throws {
        try existingItems.forEach { (existingItem: ItemType) in
            try deleteItem(existingItem: existingItem)
        }
    }

    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?) throws
    -> [ReturnedType] {
        var items: [ReturnedType] = []

        if let partition = self.store[partitionKey] {
            let sortedPartition = partition.sorted(by: { (left, right) -> Bool in
                return left.key < right.key
            })
            
            sortKeyIteration: for (sortKey, value) in sortedPartition {

                if let currentSortKeyCondition = sortKeyCondition {
                    switch currentSortKeyCondition {
                    case .equals(let value):
                        if !(value == sortKey) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .lessThan(let value):
                        if !(sortKey < value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .lessThanOrEqual(let value):
                        if !(sortKey <= value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .greaterThan(let value):
                        if !(sortKey > value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .greaterThanOrEqual(let value):
                        if !(sortKey >= value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .between(let value1, let value2):
                        if !(sortKey > value1 && sortKey < value2) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .beginsWith(let value):
                        if !(sortKey.hasPrefix(value)) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    }
                }

                items.append(try self.convertToQueryableType(input: value))
            }
        }

        return items
    }
    
    internal func convertToQueryableType<ReturnedType: PolymorphicOperationReturnType>(input: PolymorphicOperationReturnTypeConvertable) throws -> ReturnedType {
        let storedRowTypeName = input.rowTypeIdentifier
        
        var queryableTypeProviders: [String: PolymorphicOperationReturnOption<ReturnedType.AttributesType, ReturnedType>] = [:]
        ReturnedType.types.forEach { (type, provider) in
            queryableTypeProviders[getTypeRowIdentifier(type: type)] = provider
        }

        if let provider = queryableTypeProviders[storedRowTypeName] {
            return try provider.getReturnType(input: input)
        } else {
            // throw an exception, we don't know what this type is
            throw SmokeDynamoDBError.unexpectedType(provided: storedRowTypeName)
        }
    }
    
    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             limit: Int?,
                                                             exclusiveStartKey: String?) throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        return try query(forPartitionKey: partitionKey,
                         sortKeyCondition: sortKeyCondition,
                         limit: limit,
                         scanIndexForward: true,
                         exclusiveStartKey: exclusiveStartKey)
    }

    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             limit: Int?,
                                                             scanIndexForward: Bool,
                                                             exclusiveStartKey: String?) throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        // get all the results
        let rawItems: [ReturnedType] = try query(forPartitionKey: partitionKey,
                                                 sortKeyCondition: sortKeyCondition)
        
        let items: [ReturnedType]
        if !scanIndexForward {
            items = rawItems.reversed()
        } else {
            items = rawItems
        }

        let startIndex: Int
        // if there is an exclusiveStartKey
        if let exclusiveStartKey = exclusiveStartKey {
            guard let storedStartIndex = Int(exclusiveStartKey) else {
                fatalError("Unexpectedly encoded exclusiveStartKey '\(exclusiveStartKey)'")
            }

            startIndex = storedStartIndex
        } else {
            startIndex = 0
        }

        let endIndex: Int
        let lastEvaluatedKey: String?
        if let limit = limit, startIndex + limit < items.count {
            endIndex = startIndex + limit
            lastEvaluatedKey = String(endIndex)
        } else {
            endIndex = items.count
            lastEvaluatedKey = nil
        }

        return (Array(items[startIndex..<endIndex]), lastEvaluatedKey)
    }
}
