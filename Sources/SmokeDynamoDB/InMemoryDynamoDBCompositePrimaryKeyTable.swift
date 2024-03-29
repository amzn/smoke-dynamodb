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
import NIO
import CollectionConcurrencyKit

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

public protocol InMemoryTransactionDelegate {
    func injectErrors<WriteEntryType: PolymorphicWriteEntry,
                      TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
                        _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType],
                        table: InMemoryDynamoDBCompositePrimaryKeyTable) async throws -> [SmokeDynamoDBError]
}

public class InMemoryDynamoDBCompositePrimaryKeyTable: DynamoDBCompositePrimaryKeyTable {

    public let eventLoop: EventLoop
    public let escapeSingleQuoteInPartiQL: Bool
    public let transactionDelegate: InMemoryTransactionDelegate?
    internal let storeWrapper: InMemoryDynamoDBCompositePrimaryKeyTableStore
    
    public var store: [String: [String: PolymorphicOperationReturnTypeConvertable]] {
        do {
            return try storeWrapper.getStore(eventLoop: self.eventLoop).wait()
        } catch {
            fatalError("Unable to retrieve InMemoryDynamoDBCompositePrimaryKeyTable store.")
        }
    }
    
    public init(eventLoop: EventLoop,
                executeItemFilter: ExecuteItemFilterType? = nil,
                escapeSingleQuoteInPartiQL: Bool = false,
                transactionDelegate: InMemoryTransactionDelegate? = nil) {
        self.eventLoop = eventLoop
        self.storeWrapper = InMemoryDynamoDBCompositePrimaryKeyTableStore(executeItemFilter: executeItemFilter)
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.transactionDelegate = transactionDelegate
    }
    
    internal init(eventLoop: EventLoop,
                  storeWrapper: InMemoryDynamoDBCompositePrimaryKeyTableStore,
                  escapeSingleQuoteInPartiQL: Bool = false,
                  transactionDelegate: InMemoryTransactionDelegate? = nil) {
        self.eventLoop = eventLoop
        self.storeWrapper = storeWrapper
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.transactionDelegate = transactionDelegate
    }
    
    public func on(eventLoop: EventLoop) -> InMemoryDynamoDBCompositePrimaryKeyTable {
        return InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop,
                                                        storeWrapper: self.storeWrapper)
    }

    public func validateEntry<AttributesType, ItemType>(entry: WriteEntry<AttributesType, ItemType>) throws {
        let entryString = "\(entry)"
        if entryString.count > AWSDynamoDBLimits.maxStatementLength {
            throw SmokeDynamoDBError.statementLengthExceeded(
                reason: "failed to satisfy constraint: Member must have length less than or equal to "
                    + "\(AWSDynamoDBLimits.maxStatementLength). Actual length \(entryString.count)")
        }
    }

    public func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void> {
        return storeWrapper.insertItem(item, eventLoop: self.eventLoop)
    }

    public func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void> {
        return storeWrapper.clobberItem(item, eventLoop: self.eventLoop)
    }

    public func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     existingItem: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void> {
        return storeWrapper.updateItem(newItem: newItem, existingItem: existingItem, eventLoop: self.eventLoop)
    }
    
    public func transactWrite<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws {
        let noConstraints: [EmptyPolymorphicTransactionConstraintEntry] = []
        return try await transactWrite(entries, constraints: noConstraints)
    }
    
    public func transactWrite<WriteEntryType: PolymorphicWriteEntry,
                              TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
                                _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType]) async throws {
        // if there is a transaction delegate and it wants to inject errors
        if let errors = try await transactionDelegate?.injectErrors(entries, constraints: constraints, table: self), !errors.isEmpty {
            throw SmokeDynamoDBError.transactionCanceled(reasons: errors)
        }
                                    
        return try await storeWrapper.bulkWrite(entries, constraints: constraints, isTransaction: true, eventLoop: self.eventLoop).get()
    }
    
    public func bulkWrite<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws {
        let noConstraints: [EmptyPolymorphicTransactionConstraintEntry] = []
        return try await storeWrapper.bulkWrite(entries, constraints: noConstraints, isTransaction: false, eventLoop: self.eventLoop).get()
    }
    
    public func monomorphicBulkWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>])
    -> EventLoopFuture<Void> {
        return storeWrapper.monomorphicBulkWrite(entries, eventLoop: self.eventLoop)
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
        
        return EventLoopFuture.andAllSucceed(futures, on: eventLoop)
    }

    public func monomorphicBulkWriteWithoutThrowing<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) -> EventLoopFuture<Set<BatchStatementErrorCodeEnum>> {
        return storeWrapper.monomorphicBulkWriteWithoutThrowing(entries, eventLoop: eventLoop)
    }
    
    public func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>)
    -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>?> {
        return storeWrapper.getItem(forKey: key, eventLoop: self.eventLoop)
    }
    
    public func getItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>])
    -> EventLoopFuture<[CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]> {
        return storeWrapper.getItems(forKeys: keys, eventLoop: self.eventLoop)
    }

    public func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) -> EventLoopFuture<Void> {
        return storeWrapper.deleteItem(forKey: key, eventLoop: self.eventLoop)
    }
    
    public func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) -> EventLoopFuture<Void> {
        return storeWrapper.deleteItems(forKeys: keys, eventLoop: self.eventLoop)
    }
    
    public func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType]) -> EventLoopFuture<Void> {
        return storeWrapper.deleteItems(existingItems: existingItems, eventLoop: self.eventLoop)
    }
    
    public func deleteItem<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void>
            where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        return storeWrapper.deleteItem(existingItem: existingItem, eventLoop: self.eventLoop)
    }

    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    consistentRead: Bool)
    -> EventLoopFuture<[ReturnedType]> {
        return storeWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition, eventLoop: self.eventLoop)
    }
    
    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    limit: Int?,
                                                                    exclusiveStartKey: String?,
                                                                    consistentRead: Bool)
    -> EventLoopFuture<([ReturnedType], String?)> {
        return storeWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                  limit: limit, exclusiveStartKey: exclusiveStartKey, eventLoop: self.eventLoop)
    }

    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    limit: Int?,
                                                                    scanIndexForward: Bool,
                                                                    exclusiveStartKey: String?,
                                                                    consistentRead: Bool)
    -> EventLoopFuture<([ReturnedType], String?)> {
        return storeWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                  limit: limit, scanIndexForward: scanIndexForward,
                                  exclusiveStartKey: exclusiveStartKey, eventLoop: eventLoop)
    }
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?) -> EventLoopFuture<[ReturnedType]> {
        return storeWrapper.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                    additionalWhereClause: additionalWhereClause, eventLoop: self.eventLoop)
    }
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?,
            nextToken: String?) -> EventLoopFuture<([ReturnedType], String?)> {
        return storeWrapper.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                    additionalWhereClause: additionalWhereClause, nextToken: nextToken, eventLoop: self.eventLoop)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?)
    -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]> {
        return storeWrapper.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                               additionalWhereClause: additionalWhereClause, eventLoop: self.eventLoop)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?,
            nextToken: String?)
    -> EventLoopFuture<([TypedDatabaseItem<AttributesType, ItemType>], String?)> {
        return storeWrapper.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                               additionalWhereClause: additionalWhereClause, nextToken: nextToken, eventLoop: self.eventLoop)
    }
    
    public func monomorphicGetItems<AttributesType, ItemType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>])
    -> EventLoopFuture<[CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>]> {
        return storeWrapper.monomorphicGetItems(forKeys: keys, eventLoop: self.eventLoop)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                           sortKeyCondition: AttributeCondition?,
                                                           consistentRead: Bool)
    -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]>
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        return storeWrapper.monomorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                             eventLoop: self.eventLoop)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?,
            scanIndexForward: Bool,
            exclusiveStartKey: String?,
            consistentRead: Bool)
    -> EventLoopFuture<([TypedDatabaseItem<AttributesType, ItemType>], String?)>
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        return storeWrapper.monomorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                             limit: limit, scanIndexForward: scanIndexForward,
                                             exclusiveStartKey: exclusiveStartKey, eventLoop: self.eventLoop)
    }
}
