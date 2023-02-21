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

private struct InMemoryPolymorphicWriteEntryTransform: PolymorphicWriteEntryTransform {
    typealias TableType = InMemoryDynamoDBCompositePrimaryKeyTable

    let task: Task<Void, Swift.Error>

    init<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: WriteEntry<AttributesType, ItemType>, table: TableType) throws {
        switch entry {
        case .update(new: let new, existing: let existing):
            task = Task { try await table.updateItem(newItem: new, existingItem: existing) }
        case .insert(new: let new):
            task = Task { try await table.insertItem(new) }
        case .deleteAtKey(key: let key):
            task = Task { try await table.deleteItem(forKey: key) }
        case .deleteItem(existing: let existing):
            task = Task { try await table.deleteItem(existingItem: existing) }
        }
    }
}

private struct InMemoryPolymorphicTransactionConstraintTransform: PolymorphicTransactionConstraintTransform {
    typealias TableType = InMemoryDynamoDBCompositePrimaryKeyTable
    
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

public typealias ExecuteItemFilterType = (String, String, String, PolymorphicOperationReturnTypeConvertable)
    -> Bool

public protocol InMemoryTransactionDelegate {
    func injectErrors<WriteEntryType: PolymorphicWriteEntry,
                      TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
                        _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType],
                        table: InMemoryDynamoDBCompositePrimaryKeyTable) async throws -> [SmokeDynamoDBError]
}

public struct InMemoryDynamoDBCompositePrimaryKeyTable: DynamoDBCompositePrimaryKeyTable {

    public let escapeSingleQuoteInPartiQL: Bool
    public let transactionDelegate: InMemoryTransactionDelegate?
    internal let storeWrapper: InMemoryDynamoDBCompositePrimaryKeyTableStore
    
    public var store: [String: [String: PolymorphicOperationReturnTypeConvertable]] {
        get async {
            return await self.storeWrapper.store
        }
    }
    
    public init(executeItemFilter: ExecuteItemFilterType? = nil,
                escapeSingleQuoteInPartiQL: Bool = false,
                transactionDelegate: InMemoryTransactionDelegate? = nil) {
        self.storeWrapper = InMemoryDynamoDBCompositePrimaryKeyTableStore(executeItemFilter: executeItemFilter)
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.transactionDelegate = transactionDelegate
    }
    
    @available(*, deprecated, message: "eventLoop no longer needs to be provided")
    public init<EventLoopType>(eventLoop: EventLoopType,
                               executeItemFilter: ExecuteItemFilterType? = nil,
                               escapeSingleQuoteInPartiQL: Bool = false,
                               transactionDelegate: InMemoryTransactionDelegate? = nil) {
        self.storeWrapper = InMemoryDynamoDBCompositePrimaryKeyTableStore(executeItemFilter: executeItemFilter)
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.transactionDelegate = transactionDelegate
    }
    
    internal init(storeWrapper: InMemoryDynamoDBCompositePrimaryKeyTableStore,
                  escapeSingleQuoteInPartiQL: Bool = false,
                  transactionDelegate: InMemoryTransactionDelegate? = nil) {
        self.storeWrapper = storeWrapper
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.transactionDelegate = transactionDelegate
    }

    public func validateEntry<AttributesType, ItemType>(entry: WriteEntry<AttributesType, ItemType>) throws {
        let entryString = "\(entry)"
        if entryString.count > AWSDynamoDBLimits.maxStatementLength {
            throw SmokeDynamoDBError.statementLengthExceeded(
                reason: "failed to satisfy constraint: Member must have length less than or equal to "
                    + "\(AWSDynamoDBLimits.maxStatementLength). Actual length \(entryString.count)")
        }
    }

    public func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        return try await storeWrapper.insertItem(item)
    }

    public func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        return try await storeWrapper.clobberItem(item)
    }

    public func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     existingItem: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        return try await storeWrapper.updateItem(newItem: newItem, existingItem: existingItem)
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
        
        let entryCount = entries.count + constraints.count
        let context = StandardPolymorphicWriteEntryContext<InMemoryPolymorphicWriteEntryTransform,
                                                           InMemoryPolymorphicTransactionConstraintTransform>(table: self)
            
        if entryCount > AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement {
            throw SmokeDynamoDBError.transactionSizeExceeded(attemptedSize: entryCount,
                                                             maximumSize: AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement)
        }
        
        let store = await self.store
        let errors = try constraints.compactMap { entry -> SmokeDynamoDBError? in
            let transform: InMemoryPolymorphicTransactionConstraintTransform = try entry.handle(context: context)
            
            guard let partition = store[transform.partitionKey],
                    let item = partition[transform.sortKey],
                        item.rowStatus.rowVersion == transform.rowVersion else {
                return SmokeDynamoDBError.conditionalCheckFailed(partitionKey: transform.partitionKey,
                                                                 sortKey: transform.sortKey,
                                                                 message: "Item doesn't exist or doesn't have correct version")
            }
            
            return nil
        }
        
        if !errors.isEmpty {
            throw SmokeDynamoDBError.transactionCanceled(reasons: errors)
        }
        
        let writeErrors = try await entries.asyncCompactMap { entry -> SmokeDynamoDBError? in
            let transform: InMemoryPolymorphicWriteEntryTransform = try entry.handle(context: context)
            
            do {
                try await transform.task.value
            } catch let error {
                if let typedError = error as? SmokeDynamoDBError {
                    return typedError
                }
                
                // rethrow unexpected error
                throw error
            }
            
            return nil
        }
                                    
        if writeErrors.count > 0 {
            throw SmokeDynamoDBError.transactionCanceled(reasons: writeErrors)
        }
    }
    
    public func bulkWrite<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws {
        let context = StandardPolymorphicWriteEntryContext<InMemoryPolymorphicWriteEntryTransform,
                                                           InMemoryPolymorphicTransactionConstraintTransform>(table: self)
        
        let writeErrors = try await entries.asyncCompactMap { entry -> SmokeDynamoDBError? in
            let transform: InMemoryPolymorphicWriteEntryTransform = try entry.handle(context: context)
            
            do {
                try await transform.task.value
            } catch let error {
                if let typedError = error as? SmokeDynamoDBError {
                    return typedError
                }
                
                // rethrow unexpected error
                throw error
            }
            
            return nil
        }
                                    
        if writeErrors.count > 0 {
            throw SmokeDynamoDBError.batchErrorsReturned(errorCount: writeErrors.count, messageMap: [:])
        }
    }
    
    public func monomorphicBulkWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws {
        try await storeWrapper.monomorphicBulkWrite(entries)
    }
    
    public func monomorphicBulkWriteWithoutThrowing<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws
    -> Set<BatchStatementErrorCodeEnum> {
        return try await storeWrapper.monomorphicBulkWriteWithoutThrowing(entries)
    }

    public func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) async throws
    -> TypedDatabaseItem<AttributesType, ItemType>? {
        return try await storeWrapper.getItem(forKey: key)
    }
    
    public func getItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>]) async throws
    -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType] {
        return try await storeWrapper.getItems(forKeys: keys)
    }

    public func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) async throws {
        return try await storeWrapper.deleteItem(forKey: key)
    }
    
    public func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws {
        return try await storeWrapper.deleteItems(forKeys: keys)
    }
    
    public func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType]) async throws {
        return try await storeWrapper.deleteItems(existingItems: existingItems)
    }
    
    public func deleteItem<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) async throws {
        return try await storeWrapper.deleteItem(existingItem: existingItem)
    }

    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    consistentRead: Bool) async throws
    -> [ReturnedType] {
        return try await storeWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                            consistentRead: consistentRead)
    }
    
    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    limit: Int?,
                                                                    exclusiveStartKey: String?,
                                                                    consistentRead: Bool) async throws
    -> ([ReturnedType], String?) {
        return try await storeWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                            limit: limit, exclusiveStartKey: exclusiveStartKey, consistentRead: consistentRead)
    }

    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    limit: Int?,
                                                                    scanIndexForward: Bool,
                                                                    exclusiveStartKey: String?,
                                                                    consistentRead: Bool) async throws
    -> ([ReturnedType], String?) {
        return try await storeWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                            limit: limit, scanIndexForward: scanIndexForward,
                                            exclusiveStartKey: exclusiveStartKey, consistentRead: consistentRead)
    }
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
    -> [ReturnedType] {
        return try await storeWrapper.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                              additionalWhereClause: additionalWhereClause)
    }
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
    -> ([ReturnedType], String?) {
        return try await storeWrapper.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                              additionalWhereClause: additionalWhereClause, nextToken: nextToken)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        return try await storeWrapper.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                                         additionalWhereClause: additionalWhereClause)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
    -> ([TypedDatabaseItem<AttributesType, ItemType>], String?) {
        return try await storeWrapper.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                               additionalWhereClause: additionalWhereClause, nextToken: nextToken)
    }
    
    public func monomorphicGetItems<AttributesType, ItemType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws
    -> [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>] {
        return try await storeWrapper.monomorphicGetItems(forKeys: keys)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                           sortKeyCondition: AttributeCondition?,
                                                           consistentRead: Bool) async throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        return try await storeWrapper.monomorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                       consistentRead: consistentRead)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                           sortKeyCondition: AttributeCondition?,
                                                           limit: Int?,
                                                           scanIndexForward: Bool,
                                                           exclusiveStartKey: String?,
                                                           consistentRead: Bool) async throws
       -> ([TypedDatabaseItem<AttributesType, ItemType>], String?) {
        return try await storeWrapper.monomorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                                       limit: limit, scanIndexForward: scanIndexForward,
                                                       exclusiveStartKey: exclusiveStartKey, consistentRead: consistentRead)
    }
}
