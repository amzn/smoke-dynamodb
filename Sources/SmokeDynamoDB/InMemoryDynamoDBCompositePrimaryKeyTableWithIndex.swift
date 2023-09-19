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
import NIO

private let maxStatementLength = 8192

public enum GSIError: Error {
    case unknownIndex(name: String)
}

public struct InMemoryDynamoDBCompositePrimaryKeyTableWithIndex<GSILogic: DynamoDBCompositePrimaryKeyGSILogic>: DynamoDBCompositePrimaryKeyTable {
    public var eventLoop: EventLoop
    
    public let primaryTable: InMemoryDynamoDBCompositePrimaryKeyTable
    public let gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable
    
    private let gsiName: String
    private let gsiLogic: GSILogic
    
    public init(eventLoop: EventLoop,
                gsiName: String,
                gsiLogic: GSILogic,
                executeItemFilter: ExecuteItemFilterType? = nil) {
        self.eventLoop = eventLoop
        self.gsiName = gsiName
        self.gsiLogic = gsiLogic
        self.primaryTable = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop, executeItemFilter: executeItemFilter)
        self.gsiDataStore = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop, executeItemFilter: executeItemFilter)
    }
    
    public func validateEntry<AttributesType, ItemType>(entry: WriteEntry<AttributesType, ItemType>) throws {
        let entryString = "\(entry)"
        if entryString.count > maxStatementLength {
            throw SmokeDynamoDBError.statementLengthExceeded(
                reason: "failed to satisfy constraint: Member must have length less than or equal to \(maxStatementLength). Actual length \(entryString.count)")
        }
    }
    
    public func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>)
    -> EventLoopFuture<Void> where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        return self.primaryTable.insertItem(item) .flatMap { _ in
            return self.gsiLogic.onInsertItem(item, gsiDataStore: self.gsiDataStore)
        }
    }
    
    public func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>)
    -> EventLoopFuture<Void> where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        return self.primaryTable.clobberItem(item) .flatMap { _ in
            return self.gsiLogic.onClobberItem(item, gsiDataStore: self.gsiDataStore)
        }
    }
    
    public func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>, existingItem: TypedDatabaseItem<AttributesType, ItemType>)
    -> EventLoopFuture<Void> where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        return self.primaryTable.updateItem(newItem: newItem, existingItem: existingItem) .flatMap { _ in
            return self.gsiLogic.onUpdateItem(newItem: newItem, existingItem: existingItem, gsiDataStore: self.gsiDataStore)
        }
    }
    
    public func transactWrite<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws {
        return try await self.primaryTable.transactWrite(entries)
    }
    
    public func transactWrite<WriteEntryType: PolymorphicWriteEntry,
                              TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
                                _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType]) async throws {
        return try await self.primaryTable.transactWrite(entries, constraints: constraints)
    }
    
    public func bulkWrite<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws {
        return try await self.primaryTable.bulkWrite(entries)
    }
    
    public func monomorphicBulkWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>])
    -> EventLoopFuture<Void> {
        let futures = entries.map { entry -> EventLoopFuture<Void> in
            switch entry {
            case .update(new: let new, existing: let existing):
                return updateItem(newItem: new, existingItem: existing)
            case .insert(new: let new):
                return insertItem(new)
            case .deleteAtKey(key: let key):
                return deleteItem(forKey: key)
            case .deleteItem(existing: let existing):
                return deleteItem(existingItem: existing)
            }
        }
        
        return EventLoopFuture.andAllSucceed(futures, on: self.eventLoop)
    }
    
    public func monomorphicBulkWriteWithFallback<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) -> EventLoopFuture<Void> {
        // fall back to singel operation if the write entry exceeds the statement length limitation
        var nonBulkWriteEntries: [WriteEntry<AttributesType, ItemType>] = []
        var bulkWriteEntries: [WriteEntry<AttributesType, ItemType>] = []
        do {
            bulkWriteEntries = try entries.compactMap { entry in
                do {
                    try self.validateEntry(entry: entry)
                    return entry
                } catch SmokeDynamoDBError.statementLengthExceeded {
                    nonBulkWriteEntries.append(entry)
                    return nil
                }
            }
        } catch {
            let promise = self.eventLoop.makePromise(of: Void.self)
            promise.fail(error)
            return promise.futureResult
        }
            
        var futures = nonBulkWriteEntries.map { nonBulkWriteEntry -> EventLoopFuture<Void> in
            switch nonBulkWriteEntry {
            case .update(new: let new, existing: let existing):
                return self.updateItem(newItem: new, existingItem: existing)
            case .insert(new: let new):
                return self.insertItem(new)
            case .deleteAtKey(key: let key):
                return self.deleteItem(forKey: key)
            case .deleteItem(existing: let existing):
                return self.deleteItem(existingItem: existing)
            }
        }
        
        futures.append(self.monomorphicBulkWrite(bulkWriteEntries))
        
        return EventLoopFuture.andAllSucceed(futures, on: self.eventLoop)
    }
    
    public func monomorphicBulkWriteWithoutThrowing<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>])
    -> EventLoopFuture<Set<BatchStatementErrorCodeEnum>> {
        let futures = entries.map { entry -> EventLoopFuture<BatchStatementErrorCodeEnum?> in
            switch entry {
            case .update(new: let new, existing: let existing):
                return updateItem(newItem: new, existingItem: existing)
                    .map { () -> BatchStatementErrorCodeEnum? in
                        return nil
                    }.flatMapError { error -> EventLoopFuture<BatchStatementErrorCodeEnum?> in
                        let promise = eventLoop.makePromise(of: BatchStatementErrorCodeEnum?.self)
                        promise.succeed(BatchStatementErrorCodeEnum.duplicateitem)
                        return promise.futureResult
                    }
            case .insert(new: let new):
                return insertItem(new)
                    .map { () -> BatchStatementErrorCodeEnum? in
                        return nil
                    }.flatMapError { error -> EventLoopFuture<BatchStatementErrorCodeEnum?> in
                        let promise = eventLoop.makePromise(of: BatchStatementErrorCodeEnum?.self)
                        promise.succeed(BatchStatementErrorCodeEnum.duplicateitem)
                        return promise.futureResult
                    }
            case .deleteAtKey(key: let key):
                return deleteItem(forKey: key)
                    .map { () -> BatchStatementErrorCodeEnum? in
                        return nil
                    }.flatMapError { error -> EventLoopFuture<BatchStatementErrorCodeEnum?> in
                        let promise = eventLoop.makePromise(of: BatchStatementErrorCodeEnum?.self)
                        promise.succeed(BatchStatementErrorCodeEnum.duplicateitem)
                        return promise.futureResult
                    }
            case .deleteItem(existing: let existing):
                return deleteItem(existingItem: existing)
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
    
    public func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>)
    -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>?> where AttributesType : PrimaryKeyAttributes,
                                                                           ItemType : Decodable, ItemType : Encodable {
        return self.primaryTable.getItem(forKey: key)
    }
    
    public func getItems<ReturnedType>(forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>])
    -> EventLoopFuture<[CompositePrimaryKey<ReturnedType.AttributesType> : ReturnedType]>
    where ReturnedType : BatchCapableReturnType, ReturnedType : PolymorphicOperationReturnType {
        return self.primaryTable.getItems(forKeys: keys)
    }
    
    public func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>)
    -> EventLoopFuture<Void> where AttributesType : PrimaryKeyAttributes {
        return self.primaryTable.deleteItem(forKey: key) .flatMap { _ in
            return self.gsiLogic.onDeleteItem(forKey: key, gsiDataStore: self.gsiDataStore)
        }
    }
    
    public func deleteItem<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>)
    -> EventLoopFuture<Void> where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        return self.primaryTable.deleteItem(existingItem: existingItem) .flatMap { _ in
            return self.gsiLogic.onDeleteItem(forKey: existingItem.compositePrimaryKey, gsiDataStore: self.gsiDataStore)
        }
    }
    
    public func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) -> EventLoopFuture<Void> {
        return self.primaryTable.deleteItems(forKeys: keys).flatMap {
            let futures = keys.map { key in
                return self.gsiLogic.onDeleteItem(forKey: key, gsiDataStore: self.gsiDataStore)
            }
            
            return EventLoopFuture.andAllSucceed(futures, on: self.eventLoop)
        }
    }
    
    public func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType]) -> EventLoopFuture<Void> {
        
        return self.primaryTable.deleteItems(existingItems: existingItems).flatMap {
            let futures = existingItems.map { existingItem in
                return self.gsiLogic.onDeleteItem(forKey: existingItem.compositePrimaryKey, gsiDataStore: self.gsiDataStore)
            }
            
            return EventLoopFuture.andAllSucceed(futures, on: self.eventLoop)
        }
    }
    
    public func query<ReturnedType>(forPartitionKey partitionKey: String,
                                    sortKeyCondition: AttributeCondition?,
                                    consistentRead: Bool)
    -> EventLoopFuture<[ReturnedType]> where ReturnedType : PolymorphicOperationReturnType {
        // if this is querying an index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                let promise = self.eventLoop.makePromise(of: [ReturnedType].self)
                promise.fail(GSIError.unknownIndex(name: indexName))
                return promise.futureResult
            }
            
            // query on the index
            return self.gsiDataStore.query(forPartitionKey: partitionKey,
                                           sortKeyCondition: sortKeyCondition,
                                           consistentRead: consistentRead)
        }
        
        // query on the main table
        return self.primaryTable.query(forPartitionKey: partitionKey,
                                       sortKeyCondition: sortKeyCondition,
                                       consistentRead: consistentRead)
    }
    
    public func query<ReturnedType>(forPartitionKey partitionKey: String, sortKeyCondition: AttributeCondition?,
                                    limit: Int?, exclusiveStartKey: String?, consistentRead: Bool)
    -> EventLoopFuture<([ReturnedType], String?)> where ReturnedType : PolymorphicOperationReturnType {
        // if this is querying an index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                let promise = self.eventLoop.makePromise(of: ([ReturnedType], String?).self)
                promise.fail(GSIError.unknownIndex(name: indexName))
                return promise.futureResult
            }
            
            // query on the index
            return self.gsiDataStore.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                           limit: limit, exclusiveStartKey: exclusiveStartKey, consistentRead: consistentRead)
        }
        
        // query on the main table
        return self.primaryTable.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                       limit: limit, exclusiveStartKey: exclusiveStartKey, consistentRead: consistentRead)
    }
    
    public func query<ReturnedType>(forPartitionKey partitionKey: String,
                                    sortKeyCondition: AttributeCondition?,
                                    limit: Int?, scanIndexForward: Bool,
                                    exclusiveStartKey: String?,
                                    consistentRead: Bool)
    -> EventLoopFuture<([ReturnedType], String?)> where ReturnedType : PolymorphicOperationReturnType {
        // if this is querying an index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                let promise = self.eventLoop.makePromise(of: ([ReturnedType], String?).self)
                promise.fail(GSIError.unknownIndex(name: indexName))
                return promise.futureResult
            }
            
            // query on the index
            return self.gsiDataStore.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                           limit: limit, scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                           consistentRead: consistentRead)
        }
        
        // query on the main table
        return self.primaryTable.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                       limit: limit, scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                       consistentRead: consistentRead)
    }
    
    public func execute<ReturnedType>(partitionKeys: [String], attributesFilter: [String]?, additionalWhereClause: String?)
    -> EventLoopFuture<[ReturnedType]> where ReturnedType : PolymorphicOperationReturnType {
        // if this is executing on index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                let promise = self.eventLoop.makePromise(of: [ReturnedType].self)
                promise.fail(GSIError.unknownIndex(name: indexName))
                return promise.futureResult
            }
            
            // execute on the index
            return self.gsiDataStore.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                             additionalWhereClause: additionalWhereClause)
        }
        
        // execute on the main table
        return self.primaryTable.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                         additionalWhereClause: additionalWhereClause)
    }
    
    public func execute<ReturnedType>(partitionKeys: [String], attributesFilter: [String]?, additionalWhereClause: String?, nextToken: String?)
    -> EventLoopFuture<([ReturnedType], String?)> where ReturnedType : PolymorphicOperationReturnType {
        // if this is executing on index
        if let indexName = ReturnedType.AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                let promise = self.eventLoop.makePromise(of: ([ReturnedType], String?).self)
                promise.fail(GSIError.unknownIndex(name: indexName))
                return promise.futureResult
            }
            
            // execute on the index
            return self.gsiDataStore.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                             additionalWhereClause: additionalWhereClause, nextToken: nextToken)
        }
        
        // execute on the main table
        return self.primaryTable.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                         additionalWhereClause: additionalWhereClause, nextToken: nextToken)
    }
    
    public func monomorphicGetItems<AttributesType, ItemType>(forKeys keys: [CompositePrimaryKey<AttributesType>])
    -> EventLoopFuture<[CompositePrimaryKey<AttributesType> : TypedDatabaseItem<AttributesType, ItemType>]>
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        return self.primaryTable.monomorphicGetItems(forKeys: keys)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                           sortKeyCondition: AttributeCondition?,
                                                           consistentRead: Bool)
    -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]> where AttributesType : PrimaryKeyAttributes, ItemType : Decodable,
                                                                            ItemType : Encodable {
        // if this is querying an index
        if let indexName = AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                let promise = self.eventLoop.makePromise(of: [TypedDatabaseItem<AttributesType, ItemType>].self)
                promise.fail(GSIError.unknownIndex(name: indexName))
                return promise.futureResult
            }
            
            // query on the index
            return self.gsiDataStore.monomorphicQuery(forPartitionKey: partitionKey,
                                                      sortKeyCondition: sortKeyCondition,
                                                      consistentRead: consistentRead)
        }
        
        // query on the main table
        return self.primaryTable.monomorphicQuery(forPartitionKey: partitionKey,
                                                  sortKeyCondition: sortKeyCondition,
                                                  consistentRead: consistentRead)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String, sortKeyCondition: AttributeCondition?,
                                                           limit: Int?, scanIndexForward: Bool, exclusiveStartKey: String?,
                                                           consistentRead: Bool)
    -> EventLoopFuture<([TypedDatabaseItem<AttributesType, ItemType>], String?)> where AttributesType : PrimaryKeyAttributes,
                                                                                       ItemType : Decodable, ItemType : Encodable {
        // if this is querying an index
        if let indexName = AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                let promise = self.eventLoop.makePromise(of: ([TypedDatabaseItem<AttributesType, ItemType>], String?).self)
                promise.fail(GSIError.unknownIndex(name: indexName))
                return promise.futureResult
            }
            
            // query on the index
            return self.gsiDataStore.monomorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                      limit: limit, scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                                      consistentRead: consistentRead)
        }
        
        // query on the main table
        return self.primaryTable.monomorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                  limit: limit, scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                                  consistentRead: consistentRead)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(partitionKeys: [String], attributesFilter: [String]?, additionalWhereClause: String?)
    -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]> where AttributesType : PrimaryKeyAttributes, ItemType : Decodable,
                                                                            ItemType : Encodable {
        // if this is executing on index
        if let indexName = AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                let promise = self.eventLoop.makePromise(of: [TypedDatabaseItem<AttributesType, ItemType>].self)
                promise.fail(GSIError.unknownIndex(name: indexName))
                return promise.futureResult
            }
            
            // execute on the index
            return self.gsiDataStore.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                        additionalWhereClause: additionalWhereClause)
        }
        
        // execute on the main table
        return self.primaryTable.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                    additionalWhereClause: additionalWhereClause)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(partitionKeys: [String], attributesFilter: [String]?,
                                                             additionalWhereClause: String?, nextToken: String?)
    -> EventLoopFuture<([TypedDatabaseItem<AttributesType, ItemType>], String?)> where AttributesType : PrimaryKeyAttributes,
                                                                                       ItemType : Decodable, ItemType : Encodable {
        // if this is executing on index
        if let indexName = AttributesType.indexName {
            // fail if it isn't the index we know about
            guard indexName == gsiName else {
                let promise = self.eventLoop.makePromise(of: ([TypedDatabaseItem<AttributesType, ItemType>], String?).self)
                promise.fail(GSIError.unknownIndex(name: indexName))
                return promise.futureResult
            }
            
            // execute on the index
            return self.gsiDataStore.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                        additionalWhereClause: additionalWhereClause, nextToken: nextToken)
        }
        
        // execute on the main table
        return self.primaryTable.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                    additionalWhereClause: additionalWhereClause, nextToken: nextToken)
    }
}
