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
import NIO

internal class InMemoryDynamoDBCompositePrimaryKeyTableStore {

    internal var store: [String: [String: PolymorphicOperationReturnTypeConvertable]] = [:]
    internal let accessQueue = DispatchQueue(
        label: "com.amazon.SmokeDynamoDB.InMemoryDynamoDBCompositePrimaryKeyTable.accessQueue",
        target: DispatchQueue.global())
    
    internal let executeItemFilter: ExecuteItemFilterType?

    init(executeItemFilter: ExecuteItemFilterType? = nil) {
        self.executeItemFilter = executeItemFilter
    }
    
    func getStore(eventLoop: EventLoop) -> EventLoopFuture<[String: [String: PolymorphicOperationReturnTypeConvertable]]> {
        let promise = eventLoop.makePromise(of: [String: [String: PolymorphicOperationReturnTypeConvertable]].self)
        
        accessQueue.async {
            promise.succeed(self.store)
        }
        
        return promise.futureResult
    }

    func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                              eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let promise = eventLoop.makePromise(of: Void.self)
        
        accessQueue.async {
            let partition = self.store[item.compositePrimaryKey.partitionKey]

            // if there is already a partition
            var updatedPartition: [String: PolymorphicOperationReturnTypeConvertable]
            if let partition = partition {
                updatedPartition = partition

                // if the row already exists
                if partition[item.compositePrimaryKey.sortKey] != nil {
                    let error = SmokeDynamoDBError.conditionalCheckFailed(partitionKey: item.compositePrimaryKey.partitionKey,
                                                                          sortKey: item.compositePrimaryKey.sortKey,
                                                                          message: "Row already exists.")
                    
                    promise.fail(error)
                    return
                }

                updatedPartition[item.compositePrimaryKey.sortKey] = item
            } else {
                updatedPartition = [item.compositePrimaryKey.sortKey: item]
            }

            self.store[item.compositePrimaryKey.partitionKey] = updatedPartition
            promise.succeed(())
        }
        
        return promise.futureResult
    }

    func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                               eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let promise = eventLoop.makePromise(of: Void.self)
        
        accessQueue.async {
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
            promise.succeed(())
        }
        
        return promise.futureResult
    }

    func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                              existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                              eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let promise = eventLoop.makePromise(of: Void.self)
        
        accessQueue.async {
            let partition = self.store[newItem.compositePrimaryKey.partitionKey]

            // if there is already a partition
            var updatedPartition: [String: PolymorphicOperationReturnTypeConvertable]
            if let partition = partition {
                updatedPartition = partition

                // if the row already exists
                if let actuallyExistingItem = partition[newItem.compositePrimaryKey.sortKey] {
                    if existingItem.rowStatus.rowVersion != actuallyExistingItem.rowStatus.rowVersion ||
                        existingItem.createDate.iso8601 != actuallyExistingItem.createDate.iso8601 {
                        let error = SmokeDynamoDBError.conditionalCheckFailed(partitionKey: newItem.compositePrimaryKey.partitionKey,
                                                                              sortKey: newItem.compositePrimaryKey.sortKey,
                                                                              message: "Trying to overwrite incorrect version.")
                        promise.fail(error)
                        return
                    }
                } else {
                    let error = SmokeDynamoDBError.conditionalCheckFailed(partitionKey: newItem.compositePrimaryKey.partitionKey,
                                                                  sortKey: newItem.compositePrimaryKey.sortKey,
                                                                  message: "Existing item does not exist.")
                    promise.fail(error)
                    return
                }

                updatedPartition[newItem.compositePrimaryKey.sortKey] = newItem
            } else {
                let error = SmokeDynamoDBError.conditionalCheckFailed(partitionKey: newItem.compositePrimaryKey.partitionKey,
                                                                      sortKey: newItem.compositePrimaryKey.sortKey,
                                                                      message: "Existing item does not exist.")
                promise.fail(error)
                return
            }

            self.store[newItem.compositePrimaryKey.partitionKey] = updatedPartition
            promise.succeed(())
        }
        
        return promise.futureResult
    }
    
    public func updateItems<AttributesType, ItemType>(_ items: [(new: TypedDatabaseItem<AttributesType, ItemType>,
                                                      existing: TypedDatabaseItem<AttributesType, ItemType>)],
                                                      eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let futures = items.map { (new, existing) in
            return updateItem(newItem: new, existingItem: existing, eventLoop: eventLoop)
        }
        
        return EventLoopFuture.andAllComplete(futures, on: eventLoop)
    }

    func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>,
                                           eventLoop: EventLoop)
        -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>?> {
        let promise = eventLoop.makePromise(of: TypedDatabaseItem<AttributesType, ItemType>?.self)
        
        accessQueue.async {
            if let partition = self.store[key.partitionKey] {

                guard let value = partition[key.sortKey] else {
                    promise.succeed(nil)
                    return
                }

                guard let item = value as? TypedDatabaseItem<AttributesType, ItemType> else {
                    let foundType = type(of: value)
                    let description = "Expected to decode \(TypedDatabaseItem<AttributesType, ItemType>.self). Instead found \(foundType)."
                    let context = DecodingError.Context(codingPath: [], debugDescription: description)
                    let error = DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
                    
                    promise.fail(error)
                    return
                }

                promise.succeed(item)
                return
            }

            promise.succeed(nil)
        }
        
        return promise.futureResult
    }
    
    func getItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>],
        eventLoop: EventLoop)
    -> EventLoopFuture<[CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]> {
        let promise = eventLoop.makePromise(of: [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType].self)
        
        accessQueue.async {
            var map: [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType] = [:]
            
            keys.forEach { key in
                if let partition = self.store[key.partitionKey] {

                    guard let value = partition[key.sortKey] else {
                        return
                    }
                    
                    let itemAsReturnedType: ReturnedType
                        
                    do {
                        itemAsReturnedType = try self.convertToQueryableType(input: value)
                    } catch {
                        promise.fail(error)
                        return
                    }
                    
                    map[key] = itemAsReturnedType
                }
            }
            
            promise.succeed(map)
        }
        
        return promise.futureResult
    }

    func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>,
                                    eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let promise = eventLoop.makePromise(of: Void.self)
        
        accessQueue.async {
            self.store[key.partitionKey]?[key.sortKey] = nil
            promise.succeed(())
        }
        
        return promise.futureResult
    }
    
    func deleteItem<ItemType: DatabaseItem>(existingItem: ItemType,
                                            eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let promise = eventLoop.makePromise(of: Void.self)
        
        accessQueue.async {
            let partition = self.store[existingItem.compositePrimaryKey.partitionKey]

            // if there is already a partition
            var updatedPartition: [String: PolymorphicOperationReturnTypeConvertable]
            if let partition = partition {
                updatedPartition = partition

                // if the row already exists
                if let actuallyExistingItem = partition[existingItem.compositePrimaryKey.sortKey] {
                    if existingItem.rowStatus.rowVersion != actuallyExistingItem.rowStatus.rowVersion ||
                    existingItem.createDate.iso8601 != actuallyExistingItem.createDate.iso8601 {
                        let error = SmokeDynamoDBError.conditionalCheckFailed(partitionKey: existingItem.compositePrimaryKey.partitionKey,
                                                                              sortKey: existingItem.compositePrimaryKey.sortKey,
                                                                              message: "Trying to delete incorrect version.")
                        
                        promise.fail(error)
                        return
                    }
                } else {
                    let error =  SmokeDynamoDBError.conditionalCheckFailed(partitionKey: existingItem.compositePrimaryKey.partitionKey,
                                                                           sortKey: existingItem.compositePrimaryKey.sortKey,
                                                                           message: "Existing item does not exist.")
                    
                    promise.fail(error)
                    return
                }

                updatedPartition[existingItem.compositePrimaryKey.sortKey] = nil
            } else {
                let error =  SmokeDynamoDBError.conditionalCheckFailed(partitionKey: existingItem.compositePrimaryKey.partitionKey,
                                                                       sortKey: existingItem.compositePrimaryKey.sortKey,
                                                                       message: "Existing item does not exist.")
                
                promise.fail(error)
                return
            }

            self.store[existingItem.compositePrimaryKey.partitionKey] = updatedPartition
            promise.succeed(())
        }
        
        return promise.futureResult
    }
    
    func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>],
                                     eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let futures = keys.map { key in
            return deleteItem(forKey: key, eventLoop: eventLoop)
        }
        
        return EventLoopFuture.andAllComplete(futures, on: eventLoop)
    }
    
    func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType],
                                             eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let futures = existingItems.map { existingItem in
            return deleteItem(existingItem: existingItem, eventLoop: eventLoop)
        }
        
        return EventLoopFuture.andAllComplete(futures, on: eventLoop)
    }

    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             eventLoop: EventLoop)
        -> EventLoopFuture<[ReturnedType]> {
        let promise = eventLoop.makePromise(of: [ReturnedType].self)
        
        accessQueue.async {
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

                    do {
                        items.append(try self.convertToQueryableType(input: value))
                    } catch {
                        promise.fail(error)
                        return
                    }
                }
            }

            promise.succeed(items)
        }
        
        return promise.futureResult
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
            // throw an exception, we don't what this type is
            throw SmokeDynamoDBError.unexpectedType(provided: storedRowTypeName)
        }
    }
    
    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             limit: Int?,
                                                             exclusiveStartKey: String?,
                                                             eventLoop: EventLoop)
            -> EventLoopFuture<([ReturnedType], String?)> {
        return query(forPartitionKey: partitionKey,
                     sortKeyCondition: sortKeyCondition,
                     limit: limit,
                     scanIndexForward: true,
                     exclusiveStartKey: exclusiveStartKey,
                     eventLoop: eventLoop)
    }

    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             limit: Int?,
                                                             scanIndexForward: Bool,
                                                             exclusiveStartKey: String?,
                                                             eventLoop: EventLoop)
            -> EventLoopFuture<([ReturnedType], String?)> {
        // get all the results
        return query(forPartitionKey: partitionKey,
                     sortKeyCondition: sortKeyCondition,
                     eventLoop: eventLoop)
            .map { (rawItems: [ReturnedType]) in
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
}
