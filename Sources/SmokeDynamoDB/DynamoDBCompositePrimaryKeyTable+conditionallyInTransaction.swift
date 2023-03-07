// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
//  DynamoDBCompositePrimaryKeyTable+conditionallyInTransaction.swift
//  SmokeDynamoDB
//

import Foundation

private enum ConditionalTransactionFailureState {
    case unknown
    case primaryItemFailureOnly
    case additionalFailures
}

public typealias StandardConditionalTransactWriteError<PrimaryItemType: Codable> =
    ConditionalTransactWriteError<StandardPrimaryKeyAttributes, PrimaryItemType>

public enum ConditionalTransactWriteError<PrimaryAttributesType: PrimaryKeyAttributes, PrimaryItemType: Codable>: Error {
    case transactionCanceled(primaryItem: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>?,
                             reasons: [SmokeDynamoDBError])
    
}

public extension DynamoDBCompositePrimaryKeyTable {
#if (os(Linux) && compiler(>=5.5)) || (!os(Linux) && compiler(>=5.5.2)) && canImport(_Concurrency)
    
    /**
     Method to conditionally update an item at the specified key for a number of retries.
     This method is useful for database rows that may be updated simultaneously by different clients
     and each client will only attempt to update based on the current row value.
     Similar to the `conditionalUpdateItem` family of APIs, this method is useful for database
     rows that may be updated simultaneously by different clients and each client will only attempt to
     update based on the current row value. Unlike `conditionalUpdateItem`, this API can be used
     to add additional conditionality based on successfully updating other rows.
     On each attempt, the updatedPayloadProvider will be passed the current row value. It can either
     generate an updated payload or fail with an error if an updated payload is not valid. If an updated
     payload is returned, this method will attempt to update the row. This update may fail due to
     concurrency, in which case the process will repeat until the retry limit has been reached.
 
     - Parameters:
        - key: the key of the item to update
        - withRetries: the number of times to attempt to retry the update before failing.
        - context: Instance of a custom type that can be passed between the providers. The context can be modified and the
                   modified context will be passed to the next iteration even if there was a transaction failure.
        - updatedPayloadProvider: the provider that will return updated payloads.
        - primaryWriteEntryProvider: provides the `WriteEntryType` entry for the primary item
        - additionalEntriesProvider: provides the additional entries to be part of the transaction based on the primaryItem
     - Returns: the version of the primary item that was successfully written to the table
     */
    @discardableResult
    func conditionallyUpdateItemInTransaction<PrimaryAttributesType, PrimaryItemType,
                                    ContextType, WriteEntryType: PolymorphicWriteEntry>(
        forKey key: CompositePrimaryKey<PrimaryAttributesType>,
        withRetries retries: Int = 10, context: ContextType? = nil,
        updatedItemProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, ContextType?) async throws
            -> (item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: ContextType),
        primaryWriteEntryProvider: @escaping (WriteEntry<PrimaryAttributesType, PrimaryItemType>) -> WriteEntryType,
        additionalEntriesProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, ContextType) async throws
            -> [WriteEntryType]) async throws
    -> (item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: ContextType) {
        func constraintsProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: ContextType)
        -> [EmptyPolymorphicTransactionConstraintEntry] {
            return []
        }
        
        return try await conditionallyUpdateItemInTransaction(
            forKey: key,
            withRetries: retries, context: context,
            updatedItemProvider: updatedItemProvider,
            primaryWriteEntryProvider: primaryWriteEntryProvider,
            additionalEntriesProvider: additionalEntriesProvider,
            constraintsProvider: constraintsProvider)
    }
    
    /**
     Method to conditionally update an item at the specified key for a number of retries.
     This method is useful for database rows that may be updated simultaneously by different clients
     and each client will only attempt to update based on the current row value.
     Similar to the `conditionalUpdateItem` family of APIs, this method is useful for database
     rows that may be updated simultaneously by different clients and each client will only attempt to
     update based on the current row value. Unlike `conditionalUpdateItem`, this API can be used
     to add additional conditionality based on successfully updating other rows.
     On each attempt, the updatedPayloadProvider will be passed the current row value. It can either
     generate an updated payload or fail with an error if an updated payload is not valid. If an updated
     payload is returned, this method will attempt to update the row. This update may fail due to
     concurrency, in which case the process will repeat until the retry limit has been reached.
 
     - Parameters:
        - key: the key of the item to update
        - withRetries: the number of times to attempt to retry the update before failing.
        - updatedPayloadProvider: the provider that will return updated payloads.
        - primaryWriteEntryProvider: provides the `WriteEntryType` entry for the primary item
        - additionalEntriesProvider: provides the additional entries to be part of the transaction based on the primaryItem
     - Returns: the version of the primary item that was successfully written to the table
     */
    @discardableResult
    func conditionallyUpdateItemInTransaction<PrimaryAttributesType, PrimaryItemType,
                                    WriteEntryType: PolymorphicWriteEntry>(
        forKey key: CompositePrimaryKey<PrimaryAttributesType>,
        withRetries retries: Int = 10,
        updatedItemProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>)
            async throws -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>,
        primaryWriteEntryProvider: @escaping (WriteEntry<PrimaryAttributesType, PrimaryItemType>) -> WriteEntryType,
        additionalEntriesProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>) async throws -> [WriteEntryType]) async throws
    -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType> {
        func innerUpdatedItemProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void?) async throws
        -> (item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) {
            let returnedItem = try await updatedItemProvider(item)
            
            return (returnedItem, ())
        }
        
        func innerAdditionalEntriesProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) async throws
        -> [WriteEntryType] {
            return try await additionalEntriesProvider(item)
        }
        
        func constraintsProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void)
        -> [EmptyPolymorphicTransactionConstraintEntry] {
            return []
        }
        
        return try await conditionallyUpdateItemInTransaction(
            forKey: key,
            withRetries: retries,
            updatedItemProvider: innerUpdatedItemProvider,
            primaryWriteEntryProvider: primaryWriteEntryProvider,
            additionalEntriesProvider: innerAdditionalEntriesProvider,
            constraintsProvider: constraintsProvider).item
    }
    
    /**
     Method to conditionally update an item at the specified key for a number of retries.
     This method is useful for database rows that may be updated simultaneously by different clients
     and each client will only attempt to update based on the current row value.
     Similar to the `conditionalUpdateItem` family of APIs, this method is useful for database
     rows that may be updated simultaneously by different clients and each client will only attempt to
     update based on the current row value. Unlike `conditionalUpdateItem`, this API can be used
     to add additional conditionality based on successfully updating other rows.
     On each attempt, the updatedPayloadProvider will be passed the current row value. It can either
     generate an updated payload or fail with an error if an updated payload is not valid. If an updated
     payload is returned, this method will attempt to update the row. This update may fail due to
     concurrency, in which case the process will repeat until the retry limit has been reached.
 
     - Parameters:
        - key: the key of the item to update
        - withRetries: the number of times to attempt to retry the update before failing.
        - additionalEntries: the additional entries to be part of the transaction
        - updatedPayloadProvider: the provider that will return updated payloads.
        - primaryWriteEntryProvider: provides the `WriteEntryType` entry for the primary item
     - Returns: the version of the primary item that was successfully written to the table
     */
    @discardableResult
    func conditionallyUpdateItemInTransaction<PrimaryAttributesType, PrimaryItemType,
                                    WriteEntryType: PolymorphicWriteEntry>(
        forKey key: CompositePrimaryKey<PrimaryAttributesType>,
        withRetries retries: Int = 10, additionalEntries: [WriteEntryType],
        updatedItemProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>)
            async throws -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>,
        primaryWriteEntryProvider: @escaping (WriteEntry<PrimaryAttributesType, PrimaryItemType>) -> WriteEntryType) async throws
    -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType> {
        func innerUpdatedItemProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void?) async throws
        -> (item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) {
            let returnedItem = try await updatedItemProvider(item)
            
            return (returnedItem, ())
        }
        
        func additionalEntriesProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) async throws
        -> [WriteEntryType] {
            return additionalEntries
        }
        
        func constraintsProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void)
        -> [EmptyPolymorphicTransactionConstraintEntry] {
            return []
        }
        
        return try await conditionallyUpdateItemInTransaction(
            forKey: key,
            withRetries: retries,
            updatedItemProvider: innerUpdatedItemProvider,
            primaryWriteEntryProvider: primaryWriteEntryProvider,
            additionalEntriesProvider: additionalEntriesProvider,
            constraintsProvider: constraintsProvider).item
    }
    
    /**
     Method to conditionally update an item at the specified key for a number of retries.
     This method is useful for database rows that may be updated simultaneously by different clients
     and each client will only attempt to update based on the current row value.
     Similar to the `conditionalUpdateItem` family of APIs, this method is useful for database
     rows that may be updated simultaneously by different clients and each client will only attempt to
     update based on the current row value. Unlike `conditionalUpdateItem`, this API can be used
     to add additional conditionality based on successfully updating or the presence of other rows.
     On each attempt, the updatedPayloadProvider will be passed the current row value. It can either
     generate an updated payload or fail with an error if an updated payload is not valid. If an updated
     payload is returned, this method will attempt to update the row. This update may fail due to
     concurrency, in which case the process will repeat until the retry limit has been reached.
 
     - Parameters:
        - key: the key of the item to update
        - withRetries: the number of times to attempt to retry the update before failing.
        - context: Instance of a custom type that can be passed between the providers. The context can be modified and the
                   modified context will be passed to the next iteration even if there was a transaction failure.
        - updatedPayloadProvider: the provider that will return updated payloads.
        - primaryWriteEntryProvider: provides the `WriteEntryType` entry for the primary item
        - additionalEntriesProvider: provides the additional entries to be part of the transaction based on the primaryItem
        - constraintsProvider: provides the constraints to include as part of the transaction based on the primaryItem
     - Returns: the version of the primary item that was successfully written to the table
     */
    @discardableResult
    func conditionallyUpdateItemInTransaction<PrimaryAttributesType, PrimaryItemType,
                                    ContextType, WriteEntryType: PolymorphicWriteEntry,
                                    TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
        forKey key: CompositePrimaryKey<PrimaryAttributesType>,
        withRetries retries: Int = 10, context: ContextType? = nil,
        updatedItemProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, ContextType?) async throws
            -> (item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: ContextType),
        primaryWriteEntryProvider: @escaping (WriteEntry<PrimaryAttributesType, PrimaryItemType>) -> WriteEntryType,
        additionalEntriesProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, ContextType) async throws
            -> [WriteEntryType],
        constraintsProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, ContextType) async throws
            -> [TransactionConstraintEntryType]) async throws
    -> (item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: ContextType) {
        guard retries > 0 else {
            throw SmokeDynamoDBError.concurrencyError(partitionKey: key.partitionKey,
                                                    sortKey: key.sortKey,
                                                    message: "Unable to complete conditional transact write in specified number of attempts")
        }
                                        
        let databaseItemOptional: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>? = try await getItem(forKey: key)
        
        guard let databaseItem = databaseItemOptional else {
            throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: key.partitionKey,
                                                            sortKey: key.sortKey,
                                                            message: "Item not present in database.")
        }
        
        let (updatedDatabaseItem, updatedContext) = try await updatedItemProvider(databaseItem, context)
            
        let entries: [WriteEntryType]
        let primaryWriteEntry: WriteEntry<PrimaryAttributesType, PrimaryItemType> = .update(new: updatedDatabaseItem, existing: databaseItem)
        let typedPrimaryWriteEntry = primaryWriteEntryProvider(primaryWriteEntry)
        
        let additionalEntries = try await additionalEntriesProvider(updatedDatabaseItem, updatedContext)
        let constraints = try await constraintsProvider(updatedDatabaseItem, updatedContext)
        
        entries = [typedPrimaryWriteEntry] + additionalEntries
            
        do {
            try await transactWrite(entries, constraints: constraints)
            
            return (updatedDatabaseItem, updatedContext)
        } catch SmokeDynamoDBError.transactionCanceled(let reasons) {
            let failureState: ConditionalTransactionFailureState = reasons.reduce(.unknown) { partialResult, error in
                switch error {
                case .duplicateItem(partitionKey: let partitionKey, sortKey: let sortKey, message: _),
                     .transactionConditionalCheckFailed(partitionKey: let partitionKey, sortKey: let sortKey, message: _):
                    if partitionKey == key.partitionKey && sortKey == key.sortKey {
                        switch partialResult {
                        case .unknown, .primaryItemFailureOnly:
                            return .primaryItemFailureOnly
                        case .additionalFailures:
                            return .additionalFailures
                        }
                    }
                    
                    return .additionalFailures
                default:
                    return partialResult
                }
            }
            
            switch failureState {
            case .primaryItemFailureOnly:
                // try again
                return try await conditionallyUpdateItemInTransaction(
                    forKey: key, withRetries: retries - 1, context: updatedContext,
                    updatedItemProvider: updatedItemProvider,
                    primaryWriteEntryProvider: primaryWriteEntryProvider,
                    additionalEntriesProvider: additionalEntriesProvider,
                    constraintsProvider: constraintsProvider)
            case .unknown, .additionalFailures:
                // the transaction is going to fail anyway regardless of what happens with the primary item
                throw ConditionalTransactWriteError.transactionCanceled(primaryItem: databaseItem, reasons: reasons)
            }
        }
    }

    /**
     Method to conditionally update an item at the specified key for a number of retries.
     This method is useful for database rows that may be updated simultaneously by different clients
     and each client will only attempt to update based on the current row value.
     Similar to the `conditionalUpdateItem` family of APIs, this method is useful for database
     rows that may be updated simultaneously by different clients and each client will only attempt to
     update based on the current row value. Unlike `conditionalUpdateItem`, this API can be used
     to add additional conditionality based on successfully updating or the presence of other rows.
     On each attempt, the updatedPayloadProvider will be passed the current row value. It can either
     generate an updated payload or fail with an error if an updated payload is not valid. If an updated
     payload is returned, this method will attempt to update the row. This update may fail due to
     concurrency, in which case the process will repeat until the retry limit has been reached.
 
     - Parameters:
        - key: the key of the item to update
        - withRetries: the number of times to attempt to retry the update before failing.
        - updatedPayloadProvider: the provider that will return updated payloads.
        - primaryWriteEntryProvider: provides the `WriteEntryType` entry for the primary item
        - additionalEntriesProvider: provides the additional entries to be part of the transaction based on the primaryItem
        - constraintsProvider: provides the constraints to include as part of the transaction based on the primaryItem
     - Returns: the version of the primary item that was successfully written to the table
     */
    @discardableResult
    func conditionallyUpdateItemInTransaction<PrimaryAttributesType, PrimaryItemType,
                                    WriteEntryType: PolymorphicWriteEntry,
                                    TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
        forKey key: CompositePrimaryKey<PrimaryAttributesType>,
        withRetries retries: Int = 10,
        updatedItemProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>) async throws
            -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>,
        primaryWriteEntryProvider: @escaping (WriteEntry<PrimaryAttributesType, PrimaryItemType>) -> WriteEntryType,
        additionalEntriesProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>) async throws
            -> [WriteEntryType],
        constraintsProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>) async throws
            -> [TransactionConstraintEntryType]) async throws
    -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType> {
        func innerUpdatedItemProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void?) async throws
        -> (item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) {
            let returnedItem = try await updatedItemProvider(item)
            
            return (returnedItem, ())
        }
        
        func innerAdditionalEntriesProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) async throws
        -> [WriteEntryType] {
            return try await additionalEntriesProvider(item)
        }
        
        func innerConstraintsProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) async throws
        -> [TransactionConstraintEntryType] {
            return try await constraintsProvider(item)
        }
        
        return try await conditionallyUpdateItemInTransaction(
            forKey: key,
            withRetries: retries,
            updatedItemProvider: innerUpdatedItemProvider,
            primaryWriteEntryProvider: primaryWriteEntryProvider,
            additionalEntriesProvider: innerAdditionalEntriesProvider,
            constraintsProvider: innerConstraintsProvider).item
    }

    /**
     Method to conditionally update an item at the specified key for a number of retries.
     This method is useful for database rows that may be updated simultaneously by different clients
     and each client will only attempt to update based on the current row value.
     Similar to the `conditionalUpdateItem` family of APIs, this method is useful for database
     rows that may be updated simultaneously by different clients and each client will only attempt to
     update based on the current row value. Unlike `conditionalUpdateItem`, this API can be used
     to add additional conditionality based on successfully updating or the presence of other rows.
     On each attempt, the updatedPayloadProvider will be passed the current row value. It can either
     generate an updated payload or fail with an error if an updated payload is not valid. If an updated
     payload is returned, this method will attempt to update the row. This update may fail due to
     concurrency, in which case the process will repeat until the retry limit has been reached.
 
     - Parameters:
        - key: the key of the item to update
        - withRetries: the number of times to attempt to retry the update before failing.
        - additionalEntries: the additional entries to be part of the transaction
        - constraints: the constraints to include as part of the transaction
        - updatedPayloadProvider: the provider that will return updated payloads.
        - primaryWriteEntryProvider: provides the `WriteEntryType` entry for the primary item
     - Returns: the version of the primary item that was successfully written to the table
     */
    @discardableResult
    func conditionallyUpdateItemInTransaction<PrimaryAttributesType, PrimaryItemType,
                                    WriteEntryType: PolymorphicWriteEntry,
                                    TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
        forKey key: CompositePrimaryKey<PrimaryAttributesType>,
        withRetries retries: Int = 10, additionalEntries: [WriteEntryType],
        constraints: [TransactionConstraintEntryType],
        updatedItemProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>)
            async throws -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>,
        primaryWriteEntryProvider: @escaping (WriteEntry<PrimaryAttributesType, PrimaryItemType>) -> WriteEntryType) async throws
    -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType> {
        func innerUpdatedItemProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void?) async throws
        -> (item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) {
            let returnedItem = try await updatedItemProvider(item)
            
            return (returnedItem, ())
        }
        
        func additionalEntriesProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) async throws
        -> [WriteEntryType] {
            return additionalEntries
        }
        
        func constraintsProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void)
        -> [TransactionConstraintEntryType] {
            return constraints
        }
        
        return try await conditionallyUpdateItemInTransaction(
            forKey: key,
            withRetries: retries,
            updatedItemProvider: innerUpdatedItemProvider,
            primaryWriteEntryProvider: primaryWriteEntryProvider,
            additionalEntriesProvider: additionalEntriesProvider,
            constraintsProvider: constraintsProvider).item
    }
    
    /**
     Method to conditionally insert or update an item at the specified key for a number of retries.
     This method is useful for database rows that may be updated simultaneously by different clients and
     each client will only attempt to update based on the current row value while adding additional
     conditionality based on successfully updating other rows.
     This operation will attempt to update the primary item, repeatedly calling the
     `primaryItemProvider` to retrieve an updated version of the current row (if it
     exists) until a transaction with the appropriate `insert` or  `update` write entry succeeds.
     This update may fail due to concurrency, in which case the process will repeat until the retry
     limit has been reached.
     
     - Parameters:
        - withRetries: the number of times to attempt to retry the update before failing.
        - context: Instance of a custom type that can be passed between the providers. The context can be modified and the
                   modified context will be passed to the next iteration even if there was a transaction failure.
        - primaryItemProvider: provides the primary item
        - primaryWriteEntryProvider: transforms the provided `WriteEntry` for the primary item into the appropriate `WriteEntryType`
        - additionalEntriesProvider: provides the additional entries to be part of the transaction based on the primaryItem
     - Returns: the version of the primary item that was successfully written to the table
     */
    @discardableResult
    func conditionallyInsertOrUpdateItemInTransaction<PrimaryAttributesType, PrimaryItemType,
                                                      ContextType, WriteEntryType: PolymorphicWriteEntry>(
        forKey key: CompositePrimaryKey<PrimaryAttributesType>,
        withRetries retries: Int = 10, context: ContextType? = nil,
        primaryItemProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>?, ContextType?) async throws
            -> (item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: ContextType),
        primaryWriteEntryProvider: @escaping (WriteEntry<PrimaryAttributesType, PrimaryItemType>) -> WriteEntryType,
        additionalEntriesProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, ContextType) async throws -> [WriteEntryType]) async throws
    -> (item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: ContextType) {
        func constraintsProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: ContextType)
        -> [EmptyPolymorphicTransactionConstraintEntry] {
            return []
        }
        
        return try await conditionallyInsertOrUpdateItemInTransaction(
            forKey: key,
            withRetries: retries, context: context,
            primaryItemProvider: primaryItemProvider,
            primaryWriteEntryProvider: primaryWriteEntryProvider,
            additionalEntriesProvider: additionalEntriesProvider,
            constraintsProvider: constraintsProvider)
    }
    
    /**
     Method to conditionally insert or update an item at the specified key for a number of retries.
     This method is useful for database rows that may be updated simultaneously by different clients and
     each client will only attempt to update based on the current row value while adding additional
     conditionality based on successfully updating other rows.
     This operation will attempt to update the primary item, repeatedly calling the
     `primaryItemProvider` to retrieve an updated version of the current row (if it
     exists) until a transaction with the appropriate `insert` or  `update` write entry succeeds.
     This update may fail due to concurrency, in which case the process will repeat until the retry
     limit has been reached.
     
     - Parameters:
        - withRetries: the number of times to attempt to retry the update before failing.
        - primaryItemProvider: provides the primary item
        - primaryWriteEntryProvider: transforms the provided `WriteEntry` for the primary item into the appropriate `WriteEntryType`
        - additionalEntriesProvider: provides the additional entries to be part of the transaction based on the primaryItem
     - Returns: the version of the primary item that was successfully written to the table
     */
    @discardableResult
    func conditionallyInsertOrUpdateItemInTransaction<PrimaryAttributesType, PrimaryItemType,
                                                      WriteEntryType: PolymorphicWriteEntry>(
        forKey key: CompositePrimaryKey<PrimaryAttributesType>,
        withRetries retries: Int = 10,
        primaryItemProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>?)
            async throws -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>,
        primaryWriteEntryProvider: @escaping (WriteEntry<PrimaryAttributesType, PrimaryItemType>) -> WriteEntryType,
        additionalEntriesProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>) async throws -> [WriteEntryType]) async throws
    -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType> {
        func innerPrimaryItemProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>?, context: Void?) async throws
        -> (item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) {
            let returnedItem = try await primaryItemProvider(item)
            
            return (returnedItem, ())
        }
        
        func innerAdditionalEntriesProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) async throws
        -> [WriteEntryType] {
            return try await additionalEntriesProvider(item)
        }
        
        func constraintsProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void)
        -> [EmptyPolymorphicTransactionConstraintEntry] {
            return []
        }
        
        return try await conditionallyInsertOrUpdateItemInTransaction(
            forKey: key,
            withRetries: retries,
            primaryItemProvider: innerPrimaryItemProvider,
            primaryWriteEntryProvider: primaryWriteEntryProvider,
            additionalEntriesProvider: innerAdditionalEntriesProvider,
            constraintsProvider: constraintsProvider).item
    }
    
    /**
     Method to conditionally insert or update an item at the specified key for a number of retries.
     This method is useful for database rows that may be updated simultaneously by different clients and
     each client will only attempt to update based on the current row value while adding additional
     conditionality based on successfully updating other rows.
     This operation will attempt to update the primary item, repeatedly calling the
     `primaryItemProvider` to retrieve an updated version of the current row (if it
     exists) until a transaction with the appropriate `insert` or  `update` write entry succeeds.
     This update may fail due to concurrency, in which case the process will repeat until the retry
     limit has been reached.
     
     - Parameters:
        - withRetries: the number of times to attempt to retry the update before failing.
        - additionalEntries: the additional entries to be part of the transaction
        - primaryItemProvider: provides the primary item
        - primaryWriteEntryProvider: transforms the provided `WriteEntry` for the primary item into the appropriate `WriteEntryType`
     - Returns: the version of the primary item that was successfully written to the table
     */
    @discardableResult
    func conditionallyInsertOrUpdateItemInTransaction<PrimaryAttributesType, PrimaryItemType,
                                                      WriteEntryType: PolymorphicWriteEntry>(
        forKey key: CompositePrimaryKey<PrimaryAttributesType>,
        withRetries retries: Int = 10, additionalEntries: [WriteEntryType],
        primaryItemProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>?)
            async throws -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>,
        primaryWriteEntryProvider: @escaping (WriteEntry<PrimaryAttributesType, PrimaryItemType>) -> WriteEntryType) async throws
    -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType> {
        func innerPrimaryItemProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>?, context: Void?) async throws
        -> (item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) {
            let returnedItem = try await primaryItemProvider(item)
            
            return (returnedItem, ())
        }
        
        func additionalEntriesProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) async throws
        -> [WriteEntryType] {
            return additionalEntries
        }
        
        func constraintsProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void)
        -> [EmptyPolymorphicTransactionConstraintEntry] {
            return []
        }
        
        return try await conditionallyInsertOrUpdateItemInTransaction(
            forKey: key,
            withRetries: retries,
            primaryItemProvider: innerPrimaryItemProvider,
            primaryWriteEntryProvider: primaryWriteEntryProvider,
            additionalEntriesProvider: additionalEntriesProvider,
            constraintsProvider: constraintsProvider).item
    }
    
    /**
     Method to conditionally insert or update an item at the specified key for a number of retries.
     This method is useful for database rows that may be updated simultaneously by different clients and
     each client will only attempt to update based on the current row value while adding additional
     conditionality based on successfully updating or the presence of other rows.
     This operation will attempt to update the primary item, repeatedly calling the
     `primaryItemProvider` to retrieve an updated version of the current row (if it
     exists) until a transaction with the appropriate `insert` or  `update` write entry succeeds.
     This update may fail due to concurrency, in which case the process will repeat until the retry
     limit has been reached.
     
     - Parameters:
        - withRetries: the number of times to attempt to retry the update before failing.
        - context: Instance of a custom type that can be passed between the providers. The context can be modified and the
                                    modified context will be passed to the next iteration even if there was a transaction failure.
        - primaryItemProvider: provides the primary item
        - primaryWriteEntryProvider: transforms the provided `WriteEntry` for the primary item into the appropriate `WriteEntryType`
        - additionalEntriesProvider: provides the additional entries to be part of the transaction based on the primaryItem
        - constraintsProvider: provides the constraints to include as part of the transaction based on the primaryItem
     - Returns: the version of the primary item that was successfully written to the table
     */
    @discardableResult
    func conditionallyInsertOrUpdateItemInTransaction<PrimaryAttributesType, PrimaryItemType,
                                    ContextType, WriteEntryType: PolymorphicWriteEntry,
                                    TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
        forKey key: CompositePrimaryKey<PrimaryAttributesType>,
        withRetries retries: Int = 10, context: ContextType? = nil,
        primaryItemProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>?, ContextType?) async throws
            -> (item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: ContextType),
        primaryWriteEntryProvider: @escaping (WriteEntry<PrimaryAttributesType, PrimaryItemType>) -> WriteEntryType,
        additionalEntriesProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, ContextType) async throws -> [WriteEntryType],
        constraintsProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, ContextType) async throws
            -> [TransactionConstraintEntryType]) async throws
    -> (item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: ContextType) {
        guard retries > 0 else {
            throw SmokeDynamoDBError.concurrencyError(partitionKey: key.partitionKey,
                                                      sortKey: key.sortKey,
                                                      message: "Unable to complete conditional transact write in specified number of attempts")
        }
                                        
        let existingItemOptional: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>? = try await getItem(forKey: key)
            
        let updatedContext: ContextType
        let entries: [WriteEntryType]
        let updatedPrimaryItem: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>
        let constraints: [TransactionConstraintEntryType]
        if let existingItem = existingItemOptional {
            let (newItem, newContext) = try await primaryItemProvider(existingItem, context)
            
            let primaryWriteEntry: WriteEntry<PrimaryAttributesType, PrimaryItemType> = .update(new: newItem, existing: existingItem)
            let typedPrimaryWriteEntry = primaryWriteEntryProvider(primaryWriteEntry)
            
            let additionalEntries = try await additionalEntriesProvider(newItem, newContext)
            constraints = try await constraintsProvider(newItem, newContext)
            updatedContext = newContext
            
            entries = [typedPrimaryWriteEntry] + additionalEntries
            updatedPrimaryItem = newItem
        } else {
            let (primaryItem, newContext) = try await primaryItemProvider(nil, context)
            
            let primaryWriteEntry: WriteEntry<PrimaryAttributesType, PrimaryItemType> = .insert(new: primaryItem)
            let typedPrimaryWriteEntry = primaryWriteEntryProvider(primaryWriteEntry)
            
            let additionalEntries = try await additionalEntriesProvider(primaryItem, newContext)
            constraints = try await constraintsProvider(primaryItem, newContext)
            
            entries = [typedPrimaryWriteEntry] + additionalEntries
            updatedPrimaryItem = primaryItem
            updatedContext = newContext
        }
            
        do {
            try await transactWrite(entries, constraints: constraints)
            
            return (updatedPrimaryItem, updatedContext)
        } catch SmokeDynamoDBError.transactionCanceled(let reasons) {
            let failureState: ConditionalTransactionFailureState = reasons.reduce(.unknown) { partialResult, error in
                switch error {
                case .duplicateItem(partitionKey: let partitionKey, sortKey: let sortKey, message: _),
                     .transactionConditionalCheckFailed(partitionKey: let partitionKey, sortKey: let sortKey, message: _):
                    if partitionKey == key.partitionKey && sortKey == key.sortKey {
                        switch partialResult {
                        case .unknown, .primaryItemFailureOnly:
                            return .primaryItemFailureOnly
                        case .additionalFailures:
                            return .additionalFailures
                        }
                    }
                    
                    return .additionalFailures
                default:
                    return partialResult
                }
            }
            
            switch failureState {
            case .primaryItemFailureOnly:
                // try again
                return try await conditionallyInsertOrUpdateItemInTransaction(
                    forKey: key, withRetries: retries - 1, context: updatedContext,
                    primaryItemProvider: primaryItemProvider,
                    primaryWriteEntryProvider: primaryWriteEntryProvider,
                    additionalEntriesProvider: additionalEntriesProvider,
                    constraintsProvider: constraintsProvider)
            case .unknown, .additionalFailures:
                // the transaction is going to fail anyway regardless of what happens with the primary item
                throw ConditionalTransactWriteError.transactionCanceled(primaryItem: existingItemOptional, reasons: reasons)
            }
        }
    }
    
    /**
     Method to conditionally insert or update an item at the specified key for a number of retries.
     This method is useful for database rows that may be updated simultaneously by different clients and
     each client will only attempt to update based on the current row value while adding additional
     conditionality based on successfully updating or the presence of other rows.
     This operation will attempt to update the primary item, repeatedly calling the
     `primaryItemProvider` to retrieve an updated version of the current row (if it
     exists) until a transaction with the appropriate `insert` or  `update` write entry succeeds.
     This update may fail due to concurrency, in which case the process will repeat until the retry
     limit has been reached.
     
     - Parameters:
        - withRetries: the number of times to attempt to retry the update before failing.
        - primaryItemProvider: provides the primary item
        - primaryWriteEntryProvider: transforms the provided `WriteEntry` for the primary item into the appropriate `WriteEntryType`
        - additionalEntriesProvider: provides the additional entries to be part of the transaction based on the primaryItem
        - constraintsProvider: provides the constraints to include as part of the transaction based on the primaryItem
     - Returns: the version of the primary item that was successfully written to the table
     */
    @discardableResult
    func conditionallyInsertOrUpdateItemInTransaction<PrimaryAttributesType, PrimaryItemType,
                                    WriteEntryType: PolymorphicWriteEntry,
                                    TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
        forKey key: CompositePrimaryKey<PrimaryAttributesType>,
        withRetries retries: Int = 10,
        primaryItemProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>?) async throws
            -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>,
        primaryWriteEntryProvider: @escaping (WriteEntry<PrimaryAttributesType, PrimaryItemType>) -> WriteEntryType,
        additionalEntriesProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>) async throws -> [WriteEntryType],
        constraintsProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>) async throws
            -> [TransactionConstraintEntryType]) async throws
    -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType> {
        func innerPrimaryItemProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>?, context: Void?) async throws
        -> (item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) {
            let returnedItem = try await primaryItemProvider(item)
            
            return (returnedItem, ())
        }
        
        func innerAdditionalEntriesProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) async throws
        -> [WriteEntryType] {
            return try await additionalEntriesProvider(item)
        }
        
        func innerConstraintsProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) async throws
        -> [TransactionConstraintEntryType] {
            return try await constraintsProvider(item)
        }
        
        return try await conditionallyInsertOrUpdateItemInTransaction(
            forKey: key,
            withRetries: retries,
            primaryItemProvider: innerPrimaryItemProvider,
            primaryWriteEntryProvider: primaryWriteEntryProvider,
            additionalEntriesProvider: innerAdditionalEntriesProvider,
            constraintsProvider: innerConstraintsProvider).item
    }
    
    /**
     Method to conditionally insert or update an item at the specified key for a number of retries.
     This method is useful for database rows that may be updated simultaneously by different clients and
     each client will only attempt to update based on the current row value while adding additional
     conditionality based on successfully updating or the presence of other rows.
     This operation will attempt to update the primary item, repeatedly calling the
     `primaryItemProvider` to retrieve an updated version of the current row (if it
     exists) until a transaction with the appropriate `insert` or  `update` write entry succeeds.
     This update may fail due to concurrency, in which case the process will repeat until the retry
     limit has been reached.
     
     - Parameters:
        - withRetries: the number of times to attempt to retry the update before failing.
        - additionalEntries: the additional entries to be part of the transaction
        - constraints: the constraints to include as part of the transaction
        - primaryItemProvider: provides the primary item
        - primaryWriteEntryProvider: transforms the provided `WriteEntry` for the primary item into the appropriate `WriteEntryType`
     - Returns: the version of the primary item that was successfully written to the table
     */
    @discardableResult
    func conditionallyInsertOrUpdateItemInTransaction<PrimaryAttributesType, PrimaryItemType,
                                    WriteEntryType: PolymorphicWriteEntry,
                                    TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
        forKey key: CompositePrimaryKey<PrimaryAttributesType>,
        withRetries retries: Int = 10, additionalEntries: [WriteEntryType],
        constraints: [TransactionConstraintEntryType],
        primaryItemProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>?)
            async throws -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>,
        primaryWriteEntryProvider: @escaping (WriteEntry<PrimaryAttributesType, PrimaryItemType>) -> WriteEntryType) async throws
    -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType> {
        func innerPrimaryItemProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>?, context: Void?) async throws
        -> (item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) {
            let returnedItem = try await primaryItemProvider(item)
            
            return (returnedItem, ())
        }
        
        func additionalEntriesProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void) async throws
        -> [WriteEntryType] {
            return additionalEntries
        }
        
        func constraintsProvider(item: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>, context: Void)
        -> [TransactionConstraintEntryType] {
            return constraints
        }
        
        return try await conditionallyInsertOrUpdateItemInTransaction(
            forKey: key,
            withRetries: retries,
            primaryItemProvider: innerPrimaryItemProvider,
            primaryWriteEntryProvider: primaryWriteEntryProvider,
            additionalEntriesProvider: additionalEntriesProvider,
            constraintsProvider: constraintsProvider).item
    }
#endif
}
