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
import NIO

private let maxStatementLength = 8192

/**
 Implementation of the DynamoDBTable protocol that simulates concurrent access
 to a database by incrementing a row's version every time it is added for
 a specified number of requests.
 */
public class SimulateConcurrencyDynamoDBCompositePrimaryKeyTable: DynamoDBCompositePrimaryKeyTable {
    public var eventLoop: EventLoop
    
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
    public init(wrappedDynamoDBTable: DynamoDBCompositePrimaryKeyTable, eventLoop: EventLoop, simulateConcurrencyModifications: Int,
                simulateOnInsertItem: Bool = true, simulateOnUpdateItem: Bool = true) {
        self.wrappedDynamoDBTable = wrappedDynamoDBTable
        self.eventLoop = eventLoop
        self.simulateConcurrencyModifications = simulateConcurrencyModifications
        self.previousConcurrencyModifications = 0
        self.simulateOnInsertItem = simulateOnInsertItem
        self.simulateOnUpdateItem = simulateOnUpdateItem
    }
    
    public func validateEntry<AttributesType, ItemType>(entry: WriteEntry<AttributesType, ItemType>) throws {
        let entryString = "\(entry)"
        if entryString.count > maxStatementLength {
            throw SmokeDynamoDBError.statementLengthExceeded(
                reason: "failed to satisfy constraint: Member must have length less than or equal to \(maxStatementLength). Actual length \(entryString.count)")
        }
    }
    
    public func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void> {
        // if there are still modifications to be made and there is an existing row
        if simulateOnInsertItem && previousConcurrencyModifications < simulateConcurrencyModifications {
            // insert an item so the conditional check will fail
            return wrappedDynamoDBTable.insertItem(item).flatMap { _ in
                self.previousConcurrencyModifications += 1
                
                // then delegate to the wrapped implementation
                return self.wrappedDynamoDBTable.insertItem(item)
            }
        }
        
        // otherwise just delegate to the wrapped implementation
        return wrappedDynamoDBTable.insertItem(item)
    }
    
    public func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void> {
        return wrappedDynamoDBTable.clobberItem(item)
    }
    
    public func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     existingItem: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void> {
        
        // if there are still modifications to be made and there is an existing row
        if simulateOnUpdateItem && previousConcurrencyModifications < simulateConcurrencyModifications {
            return wrappedDynamoDBTable.updateItem(newItem: existingItem.createUpdatedItem(withValue: existingItem.rowValue),
                                                   existingItem: existingItem).flatMap { _ in
                self.previousConcurrencyModifications += 1
                
                // then delegate to the wrapped implementation
                return self.wrappedDynamoDBTable.updateItem(newItem: newItem, existingItem: existingItem)
            }
        }
        
        // otherwise just delegate to the wrapped implementation
        return wrappedDynamoDBTable.updateItem(newItem: newItem, existingItem: existingItem)
    }
    
    public func transactWrite<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws {
        return try await self.wrappedDynamoDBTable.transactWrite(entries)
    }
    
    public func transactWrite<WriteEntryType: PolymorphicWriteEntry,
                              TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
                                _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType]) async throws {
        return try await self.wrappedDynamoDBTable.transactWrite(entries, constraints: constraints)
    }
    
    public func bulkWrite<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws {
        return try await self.wrappedDynamoDBTable.bulkWrite(entries)
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
    
    public func monomorphicBulkWriteWithFallback<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>])
    -> EventLoopFuture<Void> {
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
            let promise = eventLoop.makePromise(of: Void.self)
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
    -> EventLoopFuture<Set<BatchStatementErrorCodeEnum>>
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        return self.wrappedDynamoDBTable.monomorphicBulkWriteWithoutThrowing(entries)
    }
    
    public func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>)
            -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>?> {
        // simply delegate to the wrapped implementation
        return wrappedDynamoDBTable.getItem(forKey: key)
    }
    
    public func getItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>])
    -> EventLoopFuture<[CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]> {
        // simply delegate to the wrapped implementation
        return wrappedDynamoDBTable.getItems(forKeys: keys)
    }
    
    public func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) -> EventLoopFuture<Void> {
        // simply delegate to the wrapped implementation
        return wrappedDynamoDBTable.deleteItem(forKey: key)
    }
    
    public func deleteItem<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void>
            where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        return wrappedDynamoDBTable.deleteItem(existingItem: existingItem)
    }
    
    public func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) -> EventLoopFuture<Void> {
        return wrappedDynamoDBTable.deleteItems(forKeys: keys)
    }
    
    public func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType]) -> EventLoopFuture<Void> {
        return wrappedDynamoDBTable.deleteItems(existingItems: existingItems)
    }
    
    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    consistentRead: Bool)
    -> EventLoopFuture<[ReturnedType]> {
        // simply delegate to the wrapped implementation
        return wrappedDynamoDBTable.query(forPartitionKey: partitionKey,
                                          sortKeyCondition: sortKeyCondition,
                                          consistentRead: consistentRead)
    }
    
    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    limit: Int?,
                                                                    exclusiveStartKey: String?,
                                                                    consistentRead: Bool)
    -> EventLoopFuture<([ReturnedType], String?)> {
        // simply delegate to the wrapped implementation
        return wrappedDynamoDBTable.query(forPartitionKey: partitionKey,
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
                                                                    consistentRead: Bool)
    -> EventLoopFuture<([ReturnedType], String?)> {
        // simply delegate to the wrapped implementation
        return wrappedDynamoDBTable.query(forPartitionKey: partitionKey,
                                          sortKeyCondition: sortKeyCondition,
                                          limit: limit,
                                          scanIndexForward: scanIndexForward,
                                          exclusiveStartKey: exclusiveStartKey,
                                          consistentRead: consistentRead)
    }
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?) -> EventLoopFuture<[ReturnedType]> {
        // simply delegate to the wrapped implementation
        return wrappedDynamoDBTable.execute(partitionKeys: partitionKeys,
                                            attributesFilter: attributesFilter,
                                            additionalWhereClause: additionalWhereClause)
    }
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?, nextToken: String?) -> EventLoopFuture<([ReturnedType], String?)> {
        // simply delegate to the wrapped implementation
        return wrappedDynamoDBTable.execute(partitionKeys: partitionKeys,
                                            attributesFilter: attributesFilter,
                                            additionalWhereClause: additionalWhereClause,
                                            nextToken: nextToken)
    }
    
    public func monomorphicGetItems<AttributesType, ItemType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>])
    -> EventLoopFuture<[CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>]> {
        // simply delegate to the wrapped implementation
        return wrappedDynamoDBTable.monomorphicGetItems(forKeys: keys)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?) -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]> {
        // simply delegate to the wrapped implementation
        return wrappedDynamoDBTable.monomorphicExecute(partitionKeys: partitionKeys,
                                                       attributesFilter: attributesFilter,
                                                       additionalWhereClause: additionalWhereClause)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?, nextToken: String?) -> EventLoopFuture<([TypedDatabaseItem<AttributesType, ItemType>], String?)> {
        // simply delegate to the wrapped implementation
        return wrappedDynamoDBTable.monomorphicExecute(partitionKeys: partitionKeys,
                                                       attributesFilter: attributesFilter,
                                                       additionalWhereClause: additionalWhereClause,
                                                       nextToken: nextToken)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                           sortKeyCondition: AttributeCondition?,
                                                           consistentRead: Bool)
    -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]>
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        // simply delegate to the wrapped implementation
        return wrappedDynamoDBTable.monomorphicQuery(forPartitionKey: partitionKey,
                                                     sortKeyCondition: sortKeyCondition,
                                                     consistentRead: consistentRead)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                           sortKeyCondition: AttributeCondition?,
                                                           limit: Int?,
                                                           scanIndexForward: Bool,
                                                           exclusiveStartKey: String?,
                                                           consistentRead: Bool)
    -> EventLoopFuture<([TypedDatabaseItem<AttributesType, ItemType>], String?)>
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        // simply delegate to the wrapped implementation
        return wrappedDynamoDBTable.monomorphicQuery(forPartitionKey: partitionKey,
                                                     sortKeyCondition: sortKeyCondition,
                                                     limit: limit,
                                                     scanIndexForward: scanIndexForward,
                                                     exclusiveStartKey: exclusiveStartKey,
                                                     consistentRead: consistentRead)
    }
}
