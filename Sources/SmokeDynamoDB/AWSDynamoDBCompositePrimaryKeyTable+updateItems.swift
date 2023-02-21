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
//  AWSDynamoDBCompositePrimaryKeyTable+updateItems.swift
//  SmokeDynamoDB
//

import Foundation
import AWSCore
import DynamoDBModel
import SmokeHTTPClient
import Logging
import CollectionConcurrencyKit
import AWSMiddleware

public struct AWSDynamoDBLimits {
    // BatchExecuteStatement has a maximum of 25 statements
    public static let maximumUpdatesPerExecuteStatement = 25
    public static let maximumUpdatesPerTransactionStatement = 100
    public static let maxStatementLength = 8192
}

private struct AWSDynamoDBPolymorphicWriteEntryTransform<MiddlewareStackType: AWSHTTPMiddlewareStackProtocol>: PolymorphicWriteEntryTransform {
    typealias TableType = GenericAWSDynamoDBCompositePrimaryKeyTable<MiddlewareStackType>

    let statement: String

    init<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: WriteEntry<AttributesType, ItemType>, table: TableType) throws {
        self.statement = try table.entryToStatement(entry)
    }
}

private struct AWSDynamoDBPolymorphicTransactionConstraintTransform<MiddlewareStackType: AWSHTTPMiddlewareStackProtocol>:
PolymorphicTransactionConstraintTransform {
    typealias TableType = GenericAWSDynamoDBCompositePrimaryKeyTable<MiddlewareStackType>

    let statement: String
    
    init<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: TransactionConstraintEntry<AttributesType, ItemType>,
                                                                  table: TableType) throws {
        self.statement = try table.entryToStatement(entry)
    }
}

/// DynamoDBTable conformance updateItems function
public extension GenericAWSDynamoDBCompositePrimaryKeyTable {
    
    func validateEntry<AttributesType, ItemType>(entry: WriteEntry<AttributesType, ItemType>) throws {
        
        let statement: String = try entryToStatement(entry)
        
        if statement.count > AWSDynamoDBLimits.maxStatementLength {
            throw SmokeDynamoDBError.statementLengthExceeded(
                reason: "failed to satisfy constraint: Member must have length less than or equal "
                    + "to \(AWSDynamoDBLimits.maxStatementLength). Actual length \(statement.count)")
        }
    }
    
    internal func entryToStatement<AttributesType, ItemType>(
        _ entry: WriteEntry<AttributesType, ItemType>) throws -> String {
        
        let statement: String
        switch entry {
        case .update(new: let new, existing: let existing):
            statement = try getUpdateExpression(tableName: self.targetTableName,
                                                newItem: new,
                                                existingItem: existing)
        case .insert(new: let new):
            statement = try getInsertExpression(tableName: self.targetTableName,
                                                newItem: new)
        case .deleteAtKey(key: let key):
            statement = try getDeleteExpression(tableName: self.targetTableName,
                                                existingKey: key)
        case .deleteItem(existing: let existing):
            statement = try getDeleteExpression(tableName: self.targetTableName,
                                                existingItem: existing)
        }
        
        return statement
    }
    
    internal func entryToStatement<AttributesType, ItemType>(
        _ entry: TransactionConstraintEntry<AttributesType, ItemType>) throws -> String {
        
        let statement: String
        switch entry {
        case .required(existing: let existing):
            statement = getExistsExpression(tableName: self.targetTableName,
                                            existingItem: existing)
        }
        
        return statement
    }

    private func entryToBatchStatementRequest<AttributesType, ItemType>(
        _ entry: WriteEntry<AttributesType, ItemType>) throws -> BatchStatementRequest {
        
        let statement: String = try entryToStatement(entry)
        
        // doesn't require read consistency as no items are being read
        return BatchStatementRequest(consistentRead: false, statement: statement)
    }
    
    func throwOnBatchExecuteStatementErrors(response: DynamoDBModel.BatchExecuteStatementOutput) throws {
        var errorMap: [String: Int] = [:]
        var errorCount = 0
        response.responses?.forEach { response in
            if let error = response.error {
                errorCount += 1
                
                var messageElements: [String] = []
                if let code = error.code {
                    messageElements.append(code.rawValue)
                }
                
                if let message = error.message {
                    messageElements.append(message)
                }
                
                if !messageElements.isEmpty {
                    let message = messageElements.joined(separator: ":")
                    var updatedErrorCount = errorMap[message] ?? 0
                    updatedErrorCount += 1
                    errorMap[message] = updatedErrorCount
                }
            }
        }
        
        guard errorCount > 0 else {
            // no errors
            return
        }
        
        throw SmokeDynamoDBError.batchErrorsReturned(errorCount: errorCount, messageMap: errorMap)
    }
    
#if (os(Linux) && compiler(>=5.5)) || (!os(Linux) && compiler(>=5.5.2)) && canImport(_Concurrency)
    private func writeTransactionItems<WriteEntryType: PolymorphicWriteEntry,
                                       TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
                                        _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType]) async throws
    {
        // if there are no items, there is nothing to update
        guard entries.count > 0 else {
            return
        }
        
        let context = StandardPolymorphicWriteEntryContext<AWSDynamoDBPolymorphicWriteEntryTransform,
                                                           AWSDynamoDBPolymorphicTransactionConstraintTransform>(table: self)
        let entryStatements: [ParameterizedStatement] = try entries.map { entry -> ParameterizedStatement in
            let transform: AWSDynamoDBPolymorphicWriteEntryTransform = try entry.handle(context: context)
            let statement = transform.statement
            
            return ParameterizedStatement(statement: statement)
        }
        
        let requiredItemsStatements: [ParameterizedStatement] = try constraints.map { entry -> ParameterizedStatement in
            let transform: AWSDynamoDBPolymorphicTransactionConstraintTransform = try entry.handle(context: context)
            let statement = transform.statement
            
            return ParameterizedStatement(statement: statement)
        }
        
        let transactionInput = ExecuteTransactionInput(transactStatements: entryStatements + requiredItemsStatements)
        
        _ = try await dynamodb.executeTransaction(input: transactionInput)
    }
    
    func transactWrite<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws {
        let noConstraints: [EmptyPolymorphicTransactionConstraintEntry] = []
        return try await transactWrite(entries, constraints: noConstraints)
    }
    
    func transactWrite<WriteEntryType: PolymorphicWriteEntry,
                       TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
                        _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType]) async throws {
        let entryCount = entries.count + constraints.count
            
        if entryCount > AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement {
            throw SmokeDynamoDBError.transactionSizeExceeded(attemptedSize: entryCount,
                                                             maximumSize: AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement)
        }
        
        do {
            try await self.writeTransactionItems(entries, constraints: constraints)
        } catch DynamoDBError.transactionCanceled(let exception) {
            guard let cancellationReasons = exception.cancellationReasons else {
                throw SmokeDynamoDBError.transactionCanceled(reasons: [])
            }
            
            let keys = entries.map { $0.compositePrimaryKey } + constraints.map { $0.compositePrimaryKey }
            
            let reasons = try zip(cancellationReasons, keys).compactMap { (cancellationReason, entryKey) -> SmokeDynamoDBError? in
                let key: StandardCompositePrimaryKey?
                if let item = cancellationReason.item {
                    key = try DynamoDBDecoder().decode(.init(M: item))
                } else {
                    key = nil
                }
                
                let partitionKey = key?.partitionKey ?? entryKey?.partitionKey
                let sortKey = key?.sortKey ?? entryKey?.sortKey
                
                // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ExecuteTransaction.html
                switch cancellationReason.code {
                case "None":
                    return nil
                case "ConditionalCheckFailed":
                    return SmokeDynamoDBError.transactionConditionalCheckFailed(partitionKey: partitionKey,
                                                                                sortKey: sortKey,
                                                                                message: cancellationReason.message)
                case "DuplicateItem":
                    return SmokeDynamoDBError.duplicateItem(partitionKey: partitionKey, sortKey: sortKey,
                                                            message: cancellationReason.message)
                case "ItemCollectionSizeLimitExceeded":
                    return SmokeDynamoDBError.transactionSizeExceeded(attemptedSize: entryCount,
                                                                      maximumSize: AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement)
                case "TransactionConflict":
                    return SmokeDynamoDBError.transactionConflict(message: cancellationReason.message)

                case "ProvisionedThroughputExceeded":
                    return SmokeDynamoDBError.transactionProvisionedThroughputExceeded(message: cancellationReason.message)
                case "ThrottlingError":
                    return SmokeDynamoDBError.transactionThrottling(message: cancellationReason.message)
                case "ValidationError":
                    return SmokeDynamoDBError.transactionValidation(partitionKey: partitionKey, sortKey: sortKey,
                                                                    message: cancellationReason.message)
                default:
                    return SmokeDynamoDBError.transactionUnknown(code: cancellationReason.code, partitionKey: partitionKey,
                                                                 sortKey: sortKey,message: cancellationReason.message)
                }
            }
            
            throw SmokeDynamoDBError.transactionCanceled(reasons: reasons)
        } catch DynamoDBError.transactionConflict(let exception) {
            let reason = SmokeDynamoDBError.transactionConflict(message: exception.message)
            
            throw SmokeDynamoDBError.transactionCanceled(reasons: [reason])
        }
    }
    
    private func writeChunkedItems<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws
    {
        // if there are no items, there is nothing to update
        guard entries.count > 0 else {
            return
        }
        
        let context = StandardPolymorphicWriteEntryContext<AWSDynamoDBPolymorphicWriteEntryTransform,
                                                           AWSDynamoDBPolymorphicTransactionConstraintTransform>(table: self)
        let statements: [BatchStatementRequest] = try entries.map { entry -> BatchStatementRequest in
            let transform: AWSDynamoDBPolymorphicWriteEntryTransform = try entry.handle(context: context)
            let statement = transform.statement
            
            return BatchStatementRequest(consistentRead: true, statement: statement)
        }
        
        let executeInput = BatchExecuteStatementInput(statements: statements)
        
        let response = try await dynamodb.batchExecuteStatement(input: executeInput)
        try throwOnBatchExecuteStatementErrors(response: response)
    }
    
    func bulkWrite<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedEntries = entries.chunked(by: AWSDynamoDBLimits.maximumUpdatesPerExecuteStatement)
        try await chunkedEntries.concurrentForEach { chunk in
            try await self.writeChunkedItems(chunk)
        }
    }
    
    private func writeChunkedItems<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws {
        // if there are no items, there is nothing to update
        guard entries.count > 0 else {
            return
        }
        
        let statements: [BatchStatementRequest] = try entries.map { entry -> BatchStatementRequest in
            let statement: String
            switch entry {
            case .update(new: let new, existing: let existing):
                statement = try getUpdateExpression(tableName: self.targetTableName,
                                                    newItem: new,
                                                    existingItem: existing)
            case .insert(new: let new):
                statement = try getInsertExpression(tableName: self.targetTableName,
                                                    newItem: new)
            case .deleteAtKey(key: let key):
                statement = try getDeleteExpression(tableName: self.targetTableName,
                                                    existingKey: key)
            case .deleteItem(existing: let existing):
                statement = try getDeleteExpression(tableName: self.targetTableName,
                                                    existingItem: existing)
            }
            
            return BatchStatementRequest(consistentRead: true, statement: statement)
        }
        
        let executeInput = BatchExecuteStatementInput(statements: statements)
        
        let response = try await dynamodb.batchExecuteStatement(input: executeInput)
        try throwOnBatchExecuteStatementErrors(response: response)
    }
    
    func monomorphicBulkWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedEntries = entries.chunked(by: AWSDynamoDBLimits.maximumUpdatesPerExecuteStatement)
        try await chunkedEntries.concurrentForEach { chunk in
            try await self.writeChunkedItems(chunk)
        }
    }
    
    func writeChunkedItemsWithoutThrowing<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws
    -> Set<BatchStatementErrorCodeEnum> {
        // if there are no items, there is nothing to update
        
        guard entries.count > 0 else {
            self.logger.trace("\(entries) with count = 0")
            
            return []
        }
        
        let statements: [BatchStatementRequest] = try entries.map { try entryToBatchStatementRequest( $0 ) }
        let executeInput = BatchExecuteStatementInput(statements: statements)
        let result = try await dynamodb.batchExecuteStatement(input: executeInput)
        
        var errorCodeSet: Set<BatchStatementErrorCodeEnum> = Set()
        // TODO: Remove errorCodeSet and return errorSet instead
        var errorSet: Set<BatchStatementError> = Set()
        result.responses?.forEach { response in
            if let error = response.error, let code = error.code {
                errorCodeSet.insert(code)
                errorSet.insert(error)
            }
        }

        // if there are errors
        if !errorSet.isEmpty {
            self.logger.error("Received BatchStatmentErrors from dynamodb are \(errorSet)")
        }
        return errorCodeSet
    }
    
    func monomorphicBulkWriteWithoutThrowing<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws
    -> Set<BatchStatementErrorCodeEnum> {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedEntries = entries.chunked(by: AWSDynamoDBLimits.maximumUpdatesPerExecuteStatement)

        let results = try await chunkedEntries.concurrentMap { chunk in
            return try await self.writeChunkedItemsWithoutThrowing(chunk)
        }
        
        return results.reduce([]) { partialResult, currentResult in
            return partialResult.union(currentResult)
        }
    }
#endif
}

extension BatchStatementError: Hashable {

    public func hash(into hasher: inout Hasher) {
        hasher.combine(self.code)
        hasher.combine(self.message)
    }
}
