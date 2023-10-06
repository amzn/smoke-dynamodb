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

private let itemAlreadyExistsMessage = "Row already exists."

private struct InMemoryPolymorphicWriteEntryTransform: PolymorphicWriteEntryTransform {
    typealias TableType = InMemoryDynamoDBCompositePrimaryKeyTableStore

    let operation: () throws -> ()

    init<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: WriteEntry<AttributesType, ItemType>, table: TableType) throws {
        switch entry {
        case .update(new: let new, existing: let existing):
            operation = {
                try table.updateItemInternal(newItem: new, existingItem: existing)
            }
        case .insert(new: let new):
            operation = {
                try table.insertItemInternal(new)
            }
        case .deleteAtKey(key: let key):
            operation = {
                table.deleteItemInternal(forKey: key)
            }
        case .deleteItem(existing: let existing):
            operation = {
                try table.deleteItemInternal(existingItem: existing)
            }
        }
    }
}

private struct InMemoryPolymorphicTransactionConstraintTransform: PolymorphicTransactionConstraintTransform {
    typealias TableType = InMemoryDynamoDBCompositePrimaryKeyTableStore
    
    let partitionKey: String
    let sortKey: String
    let rowVersion: Int
    
    init<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: TransactionConstraintEntry<AttributesType, ItemType>,
                                                                  table: TableType) throws {
        switch entry {
        case .required(existing: let existing):
            self.partitionKey = existing.compositePrimaryKey.partitionKey
            self.sortKey = existing.compositePrimaryKey.sortKey
            self.rowVersion = existing.rowStatus.rowVersion
        }
    }
}

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
}

extension InMemoryDynamoDBCompositePrimaryKeyTableStore {
    func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                              eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let promise = eventLoop.makePromise(of: Void.self)
        
        accessQueue.async {
            do {
                try self.insertItemInternal(item)
                promise.succeed(())
            } catch {
                promise.fail(error)
            }
        }
        
        return promise.futureResult
    }
    
    fileprivate func insertItemInternal<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws {
        let partition = self.store[item.compositePrimaryKey.partitionKey]
        
        // if there is already a partition
        var updatedPartition: [String: PolymorphicOperationReturnTypeConvertable]
        if let partition = partition {
            updatedPartition = partition
            
            // if the row already exists
            if partition[item.compositePrimaryKey.sortKey] != nil {
                throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: item.compositePrimaryKey.partitionKey,
                                                                sortKey: item.compositePrimaryKey.sortKey,
                                                                message: itemAlreadyExistsMessage)
            }
            
            updatedPartition[item.compositePrimaryKey.sortKey] = item
        } else {
            updatedPartition = [item.compositePrimaryKey.sortKey: item]
        }
        
        self.store[item.compositePrimaryKey.partitionKey] = updatedPartition
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
            do {
                try self.updateItemInternal(newItem: newItem, existingItem: existingItem)
                promise.succeed(())
            } catch {
                promise.fail(error)
            }
        }
        
        return promise.futureResult
    }
    
    fileprivate func updateItemInternal<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
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
    
    private func handleConstraints<TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
        constraints: [TransactionConstraintEntryType], isTransaction: Bool,
        context: StandardPolymorphicWriteEntryContext<InMemoryPolymorphicWriteEntryTransform,
                                                      InMemoryPolymorphicTransactionConstraintTransform>)
    -> SmokeDynamoDBError? {
        let errors = constraints.compactMap { entry -> SmokeDynamoDBError? in
            let transform: InMemoryPolymorphicTransactionConstraintTransform
            do {
                transform = try entry.handle(context: context)
            } catch {
                return SmokeDynamoDBError.unexpectedError(cause: error)
            }
            
            guard let partition = store[transform.partitionKey],
                    let item = partition[transform.sortKey],
                        item.rowStatus.rowVersion == transform.rowVersion else {
                if isTransaction {
                    return SmokeDynamoDBError.transactionConditionalCheckFailed(partitionKey: transform.partitionKey,
                                                                                sortKey: transform.sortKey,
                                                                                message: "Item doesn't exist or doesn't have correct version")
                } else {
                    return SmokeDynamoDBError.conditionalCheckFailed(partitionKey: transform.partitionKey,
                                                                     sortKey: transform.sortKey,
                                                                     message: "Item doesn't exist or doesn't have correct version")
                }
            }
            
            return nil
        }
        
        if !errors.isEmpty {
            return SmokeDynamoDBError.transactionCanceled(reasons: errors)
        }
        
        return nil
    }
    
    private func handleEntries<WriteEntryType: PolymorphicWriteEntry>(
        entries: [WriteEntryType], isTransaction: Bool,
        context: StandardPolymorphicWriteEntryContext<InMemoryPolymorphicWriteEntryTransform,
                                                      InMemoryPolymorphicTransactionConstraintTransform>)
    -> SmokeDynamoDBError? {
        let writeErrors = entries.compactMap { entry -> SmokeDynamoDBError? in
            let transform: InMemoryPolymorphicWriteEntryTransform
            do {
                transform = try entry.handle(context: context)
            } catch {
                return SmokeDynamoDBError.unexpectedError(cause: error)
            }
            
            do {
                try transform.operation()
            } catch let error {
                if let typedError = error as? SmokeDynamoDBError {
                    if case .conditionalCheckFailed(let partitionKey, let sortKey, let message) = typedError, isTransaction {
                        if message == itemAlreadyExistsMessage {
                            return .duplicateItem(partitionKey: partitionKey, sortKey: sortKey, message: message)
                        } else {
                            return .transactionConditionalCheckFailed(partitionKey: partitionKey,
                                                                      sortKey: sortKey, message: message)
                        }
                    }
                    return typedError
                }
                
                // return unexpected error
                return SmokeDynamoDBError.unexpectedError(cause: error)
            }
            
            return nil
        }
                                    
        if writeErrors.count > 0 {
            if isTransaction {
                return SmokeDynamoDBError.transactionCanceled(reasons: writeErrors)
            } else {
                return SmokeDynamoDBError.batchErrorsReturned(errorCount: writeErrors.count, messageMap: [:])
            }
        }
        
        return nil
    }
    
    func bulkWrite<WriteEntryType: PolymorphicWriteEntry,
                       TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
                        _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType],
                        isTransaction: Bool, eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let promise = eventLoop.makePromise(of: Void.self)
        
        accessQueue.async {
            let entryCount = entries.count + constraints.count
            let context = StandardPolymorphicWriteEntryContext<InMemoryPolymorphicWriteEntryTransform,
                                                               InMemoryPolymorphicTransactionConstraintTransform>(table: self)
                
            if entryCount > AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement {
                let error = SmokeDynamoDBError.transactionSizeExceeded(attemptedSize: entryCount,
                                                                       maximumSize: AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement)
                promise.fail(error)
                return
            }
            
            let store = self.store
            
            if let error = self.handleConstraints(constraints: constraints, isTransaction: isTransaction, context: context) {
                promise.fail(error)
                return
            }
                                        
            if let error = self.handleEntries(entries: entries, isTransaction: isTransaction, context: context) {
                if isTransaction {
                    // restore the state prior to the transaction
                    self.store = store
                }
                
                promise.fail(error)
                return
            }
            
            promise.succeed()
        }
        
        return promise.futureResult
    }
    
    public func monomorphicBulkWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>],
                                                               eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let futures = entries.map { entry -> EventLoopFuture<Void> in
            switch entry {
            case .update(new: let new, existing: let existing):
                return updateItem(newItem: new, existingItem: existing, eventLoop: eventLoop)
            case .insert(new: let new):
                return insertItem(new, eventLoop: eventLoop)
            case .deleteAtKey(key: let key):
                return deleteItem(forKey: key, eventLoop: eventLoop)
            case .deleteItem(existing: let existing):
                return deleteItem(existingItem: existing, eventLoop: eventLoop)
            }
        }
        
        return EventLoopFuture.andAllSucceed(futures, on: eventLoop)
    }

    public func monomorphicBulkWriteWithoutThrowing<AttributesType, ItemType>(
        _ entries: [WriteEntry<AttributesType, ItemType>],
        eventLoop: EventLoop) -> EventLoopFuture<Set<BatchStatementErrorCodeEnum>> {
            
            let futures = entries.map { entry -> EventLoopFuture<BatchStatementErrorCodeEnum?> in
                switch entry {
                case .update(new: let new, existing: let existing):
                    return updateItem(newItem: new, existingItem: existing, eventLoop: eventLoop)
                        .map { () -> BatchStatementErrorCodeEnum? in
                            return nil
                        }.flatMapError { error -> EventLoopFuture<BatchStatementErrorCodeEnum?> in
                            let promise = eventLoop.makePromise(of: BatchStatementErrorCodeEnum?.self)
                            promise.succeed(BatchStatementErrorCodeEnum.duplicateitem)
                            return promise.futureResult
                        }
                case .insert(new: let new):
                    return insertItem(new, eventLoop: eventLoop)
                        .map { () -> BatchStatementErrorCodeEnum? in
                            return nil
                        }.flatMapError { error -> EventLoopFuture<BatchStatementErrorCodeEnum?> in
                            let promise = eventLoop.makePromise(of: BatchStatementErrorCodeEnum?.self)
                            promise.succeed(BatchStatementErrorCodeEnum.duplicateitem)
                            return promise.futureResult
                        }
                case .deleteAtKey(key: let key):
                    return deleteItem(forKey: key, eventLoop: eventLoop)
                        .map { () -> BatchStatementErrorCodeEnum? in
                            return nil
                        }.flatMapError { error -> EventLoopFuture<BatchStatementErrorCodeEnum?> in
                            let promise = eventLoop.makePromise(of: BatchStatementErrorCodeEnum?.self)
                            promise.succeed(BatchStatementErrorCodeEnum.duplicateitem)
                            return promise.futureResult
                        }
                case .deleteItem(existing: let existing):
                    return deleteItem(existingItem: existing, eventLoop: eventLoop)
                        .map { () -> BatchStatementErrorCodeEnum? in
                            return nil
                        }.flatMapError { error -> EventLoopFuture<BatchStatementErrorCodeEnum?> in
                            let promise = eventLoop.makePromise(of: BatchStatementErrorCodeEnum?.self)
                            promise.succeed(BatchStatementErrorCodeEnum.duplicateitem)
                            return promise.futureResult
                        }
                }
            }
            
            return EventLoopFuture.whenAllComplete(futures, on: eventLoop)
                .flatMapThrowing { results in
                    var errors: Set<BatchStatementErrorCodeEnum> = Set()
                    try results.forEach { result in
                        if let error = try result.get() {
                            errors.insert(error)
                        }
                    }
                    return errors
                }
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
            self.deleteItemInternal(forKey: key)
            promise.succeed(())
        }
        
        return promise.futureResult
    }
    
    fileprivate func deleteItemInternal<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) {
        self.store[key.partitionKey]?[key.sortKey] = nil
    }
    
    func deleteItem<ItemType: DatabaseItem>(existingItem: ItemType,
                                            eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let promise = eventLoop.makePromise(of: Void.self)
        
        accessQueue.async {
            do {
                try self.deleteItemInternal(existingItem: existingItem)
                promise.succeed(())
            } catch {
                promise.fail(error)
            }
        }
        
        return promise.futureResult
    }
    
    fileprivate func deleteItemInternal<ItemType: DatabaseItem>(existingItem: ItemType) throws {
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
    
    func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>],
                                     eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let futures = keys.map { key in
            return deleteItem(forKey: key, eventLoop: eventLoop)
        }
        
        return EventLoopFuture.andAllSucceed(futures, on: eventLoop)
    }
    
    func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType],
                                             eventLoop: EventLoop) -> EventLoopFuture<Void> {
        let futures = existingItems.map { existingItem in
            return deleteItem(existingItem: existingItem, eventLoop: eventLoop)
        }
        
        return EventLoopFuture.andAllSucceed(futures, on: eventLoop)
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
            // throw an exception, we don't know what this type is
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
