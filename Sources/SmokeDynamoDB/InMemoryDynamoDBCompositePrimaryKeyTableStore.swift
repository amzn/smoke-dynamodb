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

private let itemAlreadyExistsMessage = "Row already exists."

internal struct InMemoryPolymorphicWriteEntryTransform: PolymorphicWriteEntryTransform {
    typealias TableType = InMemoryDynamoDBCompositePrimaryKeyTableStore

    let operation: (inout TableType.StoreType) throws -> ()

    init<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: WriteEntry<AttributesType, ItemType>, table: TableType) throws {
        switch entry {
        case .update(new: let new, existing: let existing):
            operation = { store in
                try updateItem(newItem: new, existingItem: existing, store: &store)
            }
        case .insert(new: let new):
            operation = { store in
                try insertItem(new, store: &store)
            }
        case .deleteAtKey(key: let key):
            operation = { store in
                try deleteItem(forKey: key, store: &store)
            }
        case .deleteItem(existing: let existing):
            operation = { store in
                try deleteItem(existingItem: existing, store: &store)
            }
        }
    }
}

private func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                  existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                  store: inout [String: [String: PolymorphicOperationReturnTypeConvertable]]) throws {
    let partition = store[newItem.compositePrimaryKey.partitionKey]

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

    store[newItem.compositePrimaryKey.partitionKey] = updatedPartition
}

private func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                  store: inout [String: [String: PolymorphicOperationReturnTypeConvertable]]) throws {
    let partition = store[item.compositePrimaryKey.partitionKey]

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

    store[item.compositePrimaryKey.partitionKey] = updatedPartition
}

private func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>,
                                        store: inout [String: [String: PolymorphicOperationReturnTypeConvertable]]) throws {
    store[key.partitionKey]?[key.sortKey] = nil
}

private func deleteItem<ItemType: DatabaseItem>(existingItem: ItemType,
                                                store: inout [String: [String: PolymorphicOperationReturnTypeConvertable]]) throws {
    let partition = store[existingItem.compositePrimaryKey.partitionKey]

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

    store[existingItem.compositePrimaryKey.partitionKey] = updatedPartition
}

internal struct InMemoryPolymorphicTransactionConstraintTransform: PolymorphicTransactionConstraintTransform {
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

internal actor InMemoryDynamoDBCompositePrimaryKeyTableStore {
    typealias StoreType = [String: [String: PolymorphicOperationReturnTypeConvertable]]
    
    internal var store: StoreType = [:]
    internal let executeItemFilter: ExecuteItemFilterType?

    init(executeItemFilter: ExecuteItemFilterType? = nil) {
        self.executeItemFilter = executeItemFilter
    }
    
    nonisolated func validateEntry<AttributesType, ItemType>(entry: WriteEntry<AttributesType, ItemType>) throws {
        let entryString = "\(entry)"
        if entryString.count > AWSDynamoDBLimits.maxStatementLength {
            throw SmokeDynamoDBError.statementLengthExceeded(
                reason: "failed to satisfy constraint: Member must have length less than or equal to "
                    + "\(AWSDynamoDBLimits.maxStatementLength). Actual length \(entryString.count)")
        }
    }

    func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws {
        try SmokeDynamoDB.insertItem(item, store: &self.store)
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
        try SmokeDynamoDB.updateItem(newItem: newItem, existingItem: existingItem, store: &self.store)
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
                try transform.operation(&self.store)
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
                    isTransaction: Bool) throws
    {
        let entryCount = entries.count + constraints.count
        let context = StandardPolymorphicWriteEntryContext<InMemoryPolymorphicWriteEntryTransform,
                                                           InMemoryPolymorphicTransactionConstraintTransform>(table: self)
            
        if entryCount > AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement {
            throw SmokeDynamoDBError.transactionSizeExceeded(attemptedSize: entryCount,
                                                             maximumSize: AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement)
        }
        
        let store = self.store
        
        if let error = self.handleConstraints(constraints: constraints, isTransaction: isTransaction, context: context) {
            throw error
        }
                                    
        if let error = self.handleEntries(entries: entries, isTransaction: isTransaction, context: context) {
            if isTransaction {
                // restore the state prior to the transaction
                self.store = store
            }
            
            throw error
        }
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
    
    func monomorphicBulkWriteWithFallback<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) throws {
        // fall back to single operation if the write entry exceeds the statement length limitation
        var nonBulkWriteEntries: [WriteEntry<AttributesType, ItemType>] = []
        
        let bulkWriteEntries = try entries.compactMap { entry in
            do {
                try self.validateEntry(entry: entry)
                return entry
            } catch SmokeDynamoDBError.statementLengthExceeded {
                nonBulkWriteEntries.append(entry)
                return nil
            }
        }
        
        try self.monomorphicBulkWrite(bulkWriteEntries)
            
        try nonBulkWriteEntries.forEach { nonBulkWriteEntry in
            switch nonBulkWriteEntry {
            case .update(new: let new, existing: let existing):
                try self.updateItem(newItem: new, existingItem: existing)
            case .insert(new: let new):
                try self.insertItem(new)
            case .deleteAtKey(key: let key):
                try self.deleteItem(forKey: key)
            case .deleteItem(existing: let existing):
                try self.deleteItem(existingItem: existing)
            }
        }
    }
    
    func monomorphicBulkWriteWithoutThrowing<AttributesType, ItemType>(
        _ entries: [WriteEntry<AttributesType, ItemType>]) throws
    -> Set<BatchStatementErrorCodeEnum> {
        let results = entries.map { entry -> BatchStatementErrorCodeEnum? in
            switch entry {
            case .update(new: let new, existing: let existing):
                do {
                    try updateItem(newItem: new, existingItem: existing)
                    
                    return nil
                } catch {
                    return BatchStatementErrorCodeEnum.duplicateitem
                }
            case .insert(new: let new):
                do {
                    try insertItem(new)
                    
                    return nil
                } catch {
                    return BatchStatementErrorCodeEnum.duplicateitem
                }
            case .deleteAtKey(key: let key):
                do {
                    try deleteItem(forKey: key)
                    
                    return nil
                } catch {
                    return BatchStatementErrorCodeEnum.duplicateitem
                }
            case .deleteItem(existing: let existing):
                do {
                    try deleteItem(existingItem: existing)
                    
                    return nil
                } catch {
                    return BatchStatementErrorCodeEnum.duplicateitem
                }
            }
        }
        
        var errors: Set<BatchStatementErrorCodeEnum> = Set()
        results.forEach { result in
            if let result {
                errors.insert(result)
            }
        }
        return errors
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
        try SmokeDynamoDB.deleteItem(forKey: key, store: &self.store)
    }
    
    func deleteItem<ItemType: DatabaseItem>(existingItem: ItemType) throws {
        try SmokeDynamoDB.deleteItem(existingItem: existingItem, store: &self.store)
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
                                                             sortKeyCondition: AttributeCondition?,
                                                             consistentRead: Bool) throws
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
                                                             exclusiveStartKey: String?,
                                                             consistentRead: Bool) throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        return try query(forPartitionKey: partitionKey,
                         sortKeyCondition: sortKeyCondition,
                         limit: limit,
                         scanIndexForward: true,
                         exclusiveStartKey: exclusiveStartKey,
                         consistentRead: consistentRead)
    }

    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             limit: Int?,
                                                             scanIndexForward: Bool,
                                                             exclusiveStartKey: String?,
                                                             consistentRead: Bool) throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?) {
        // get all the results
        let rawItems: [ReturnedType] = try query(forPartitionKey: partitionKey,
                                                 sortKeyCondition: sortKeyCondition,
                                                 consistentRead: consistentRead)
        
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
