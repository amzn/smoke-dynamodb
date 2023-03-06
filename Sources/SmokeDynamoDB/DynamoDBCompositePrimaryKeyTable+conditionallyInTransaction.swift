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
        - additionalEntries: the additional entries to be part of the transaction
        - updatedPayloadProvider: the provider that will return updated payloads.
     - Returns: the version of the primary item that was successfully written to the table
     */
    @discardableResult
    func conditionallyUpdateItemInTransaction<PrimaryAttributesType, PrimaryItemType,
                                    WriteEntryType: PolymorphicWriteEntry>(
        forKey key: CompositePrimaryKey<PrimaryAttributesType>,
        withRetries retries: Int = 10,
        primaryWriteEntryProvider: @escaping (WriteEntry<PrimaryAttributesType, PrimaryItemType>) -> WriteEntryType,
        additionalEntries: [WriteEntryType],
        updatedItemProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>)
            async throws -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>) async throws
    -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType> {
        let noConstraints: [EmptyPolymorphicTransactionConstraintEntry] = []
        return try await conditionallyUpdateItemInTransaction(
            forKey: key,
            withRetries: retries,
            primaryWriteEntryProvider: primaryWriteEntryProvider,
            additionalEntries: additionalEntries, constraints: noConstraints,
            updatedItemProvider: updatedItemProvider)
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
        - constraints: the contraints to include as part of the transaction
        - updatedPayloadProvider: the provider that will return updated payloads.
     - Returns: the version of the primary item that was successfully written to the table
     */
    @discardableResult
    func conditionallyUpdateItemInTransaction<PrimaryAttributesType, PrimaryItemType,
                                    WriteEntryType: PolymorphicWriteEntry,
                                    TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
        forKey key: CompositePrimaryKey<PrimaryAttributesType>,
        withRetries retries: Int = 10,
        primaryWriteEntryProvider: @escaping (WriteEntry<PrimaryAttributesType, PrimaryItemType>) -> WriteEntryType,
        additionalEntries: [WriteEntryType], constraints: [TransactionConstraintEntryType],
        updatedItemProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>)
            async throws -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>) async throws
    -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType> {                                        
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
        
        let updatedDatabaseItem = try await updatedItemProvider(databaseItem)
            
        let entries: [WriteEntryType]
        let primaryWriteEntry: WriteEntry<PrimaryAttributesType, PrimaryItemType> = .update(new: updatedDatabaseItem, existing: databaseItem)
        let typedPrimaryWriteEntry = primaryWriteEntryProvider(primaryWriteEntry)
        
        entries = [typedPrimaryWriteEntry] + additionalEntries
            
        do {
            try await transactWrite(entries, constraints: constraints)
            
            return updatedDatabaseItem
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
                    forKey: key, withRetries: retries - 1,
                    primaryWriteEntryProvider: primaryWriteEntryProvider,
                    additionalEntries: additionalEntries, constraints: constraints,
                    updatedItemProvider: updatedItemProvider)
            case .unknown, .additionalFailures:
                // the transaction is going to fail anyway regardless of what happens with the primary item
                throw ConditionalTransactWriteError.transactionCanceled(primaryItem: databaseItem, reasons: reasons)
            }
        }
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
        - primaryWriteEntryProvider: transforms the provided `WriteEntry` for the primary item into the appropriate `WriteEntryType`
        - additionalEntries: the additional entries to be part of the transaction
        - primaryItemProvider: provides the primary item
     - Returns: the version of the primary item that was successfully written to the table
     */
    @discardableResult
    func conditionallyInsertOrUpdateItemInTransaction<PrimaryAttributesType, PrimaryItemType,
                                                      WriteEntryType: PolymorphicWriteEntry>(
        forKey key: CompositePrimaryKey<PrimaryAttributesType>,
        withRetries retries: Int = 10,
        primaryWriteEntryProvider: @escaping (WriteEntry<PrimaryAttributesType, PrimaryItemType>) -> WriteEntryType,
        additionalEntries: [WriteEntryType],
        primaryItemProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>?)
            async throws -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>) async throws
    -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType> {
        let noConstraints: [EmptyPolymorphicTransactionConstraintEntry] = []
        return try await conditionallyInsertOrUpdateItemInTransaction(
            forKey: key,
            withRetries: retries,
            primaryWriteEntryProvider: primaryWriteEntryProvider,
            additionalEntries: additionalEntries, constraints: noConstraints,
            primaryItemProvider: primaryItemProvider)
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
        - primaryWriteEntryProvider: transforms the provided `WriteEntry` for the primary item into the appropriate `WriteEntryType`
        - additionalEntries: the additional entries to be part of the transaction
        - constraints: the contraints to include as part of the transaction
        - primaryItemProvider: provides the primary item
     - Returns: the version of the primary item that was successfully written to the table
     */
    @discardableResult
    func conditionallyInsertOrUpdateItemInTransaction<PrimaryAttributesType, PrimaryItemType,
                                    WriteEntryType: PolymorphicWriteEntry,
                                    TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
        forKey key: CompositePrimaryKey<PrimaryAttributesType>,
        withRetries retries: Int = 10,
        primaryWriteEntryProvider: @escaping (WriteEntry<PrimaryAttributesType, PrimaryItemType>) -> WriteEntryType,
        additionalEntries: [WriteEntryType], constraints: [TransactionConstraintEntryType],
        primaryItemProvider: @escaping (TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>?)
            async throws -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>) async throws
    -> TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType> {
        guard retries > 0 else {
            throw SmokeDynamoDBError.concurrencyError(partitionKey: key.partitionKey,
                                                      sortKey: key.sortKey,
                                                      message: "Unable to complete conditional transact write in specified number of attempts")
        }
                                        
        let existingItemOptional: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>? = try await getItem(forKey: key)
            
        let entries: [WriteEntryType]
        let updatedPrimaryItem: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType>
        if let existingItem = existingItemOptional {
            let newItem: TypedDatabaseItem<PrimaryAttributesType, PrimaryItemType> = try await primaryItemProvider(existingItem)
            
            let primaryWriteEntry: WriteEntry<PrimaryAttributesType, PrimaryItemType> = .update(new: newItem, existing: existingItem)
            let typedPrimaryWriteEntry = primaryWriteEntryProvider(primaryWriteEntry)
            
            entries = [typedPrimaryWriteEntry] + additionalEntries
            updatedPrimaryItem = newItem
        } else {
            let primaryItem = try await primaryItemProvider(nil)
            
            let primaryWriteEntry: WriteEntry<PrimaryAttributesType, PrimaryItemType> = .insert(new: primaryItem)
            let typedPrimaryWriteEntry = primaryWriteEntryProvider(primaryWriteEntry)
            
            entries = [typedPrimaryWriteEntry] + additionalEntries
            updatedPrimaryItem = primaryItem
        }
            
        do {
            try await transactWrite(entries, constraints: constraints)
            
            return updatedPrimaryItem
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
                    forKey: key, withRetries: retries - 1,
                    primaryWriteEntryProvider: primaryWriteEntryProvider,
                    additionalEntries: additionalEntries, constraints: constraints,
                    primaryItemProvider: primaryItemProvider)
            case .unknown, .additionalFailures:
                // the transaction is going to fail anyway regardless of what happens with the primary item
                throw ConditionalTransactWriteError.transactionCanceled(primaryItem: existingItemOptional, reasons: reasons)
            }
        }
    }
#endif
}
