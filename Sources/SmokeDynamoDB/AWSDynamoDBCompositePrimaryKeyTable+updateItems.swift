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
import SmokeAWSCore
import DynamoDBModel
import SmokeHTTPClient
import Logging
import NIO
import CollectionConcurrencyKit

private let millisecondsToNanoSeconds: UInt64 = 1000000

public struct AWSDynamoDBLimits {
    // BatchExecuteStatement has a maximum of 25 statements
    public static let maximumUpdatesPerExecuteStatement = 25
    public static let maximumUpdatesPerTransactionStatement = 100
    public static let maxStatementLength = 8192
}

private struct AWSDynamoDBPolymorphicWriteEntryTransform<InvocationReportingType: HTTPClientCoreInvocationReporting>: PolymorphicWriteEntryTransform {
    typealias TableType = AWSDynamoDBCompositePrimaryKeyTable<InvocationReportingType>

    let statement: String

    init<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: WriteEntry<AttributesType, ItemType>, table: TableType) throws {
        self.statement = try table.entryToStatement(entry)
    }
}

private struct AWSDynamoDBPolymorphicTransactionConstraintTransform<
        InvocationReportingType: HTTPClientCoreInvocationReporting>: PolymorphicTransactionConstraintTransform {
    typealias TableType = AWSDynamoDBCompositePrimaryKeyTable<InvocationReportingType>

    let statement: String
    
    init<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: TransactionConstraintEntry<AttributesType, ItemType>,
                                                                  table: TableType) throws {
        self.statement = try table.entryToStatement(entry)
    }
}

/// DynamoDBTable conformance updateItems function
public extension AWSDynamoDBCompositePrimaryKeyTable {
    
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

    private func writeChunkedItems<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>])
    -> EventLoopFuture<Void> {
        // if there are no items, there is nothing to update
        guard entries.count > 0 else {
            let promise = self.eventLoop.makePromise(of: Void.self)
            promise.succeed(())
            return promise.futureResult
        }
        
        let statements: [BatchStatementRequest]
        do {
            statements = try entries.map { entry -> BatchStatementRequest in
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
                
                // doesn't require read consistency as no items are being read
                return BatchStatementRequest(consistentRead: false, statement: statement)
            }
        } catch {
            let promise = self.eventLoop.makePromise(of: Void.self)
            promise.fail(error)
            return promise.futureResult
        }
        
        let executeInput = BatchExecuteStatementInput(statements: statements)
        
        return dynamodb.batchExecuteStatement(input: executeInput).flatMapThrowing { response in
            try self.throwOnBatchExecuteStatementErrors(response: response)
        }
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
    
    func monomorphicBulkWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>])
    -> EventLoopFuture<Void> {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedEntries = entries.chunked(by: AWSDynamoDBLimits.maximumUpdatesPerExecuteStatement)
        let futures = chunkedEntries.map { chunk -> EventLoopFuture<Void> in
            return writeChunkedItems(chunk)
        }
        
        return EventLoopFuture.andAllSucceed(futures, on: self.eventLoop)
    }
    
    func writeChunkedItemsWithoutThrowing<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>])
    -> EventLoopFuture<Set<BatchStatementErrorCodeEnum>> {
        // if there are no items, there is nothing to update
        
        guard entries.count > 0 else {
            self.logger.trace("\(entries) with count = 0")
            let promise = self.eventLoop.makePromise(of: Set<BatchStatementErrorCodeEnum>.self)
            promise.succeed(Set())
            return promise.futureResult
        }
        
        do {
            let statements: [BatchStatementRequest] = try entries.map { try entryToBatchStatementRequest( $0 ) }
            let executeInput = BatchExecuteStatementInput(statements: statements)
            return dynamodb.batchExecuteStatement(input: executeInput).map { result -> Set<BatchStatementErrorCodeEnum> in
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
        } catch {
            let promise = self.eventLoop.makePromise(of: Set<BatchStatementErrorCodeEnum>.self)
            promise.fail(error)
            return promise.futureResult
        }
    }

    func monomorphicBulkWriteWithoutThrowing<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>])
    -> EventLoopFuture<Set<BatchStatementErrorCodeEnum>> {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedEntries = entries.chunked(by: AWSDynamoDBLimits.maximumUpdatesPerExecuteStatement)

        let futures = chunkedEntries.map { chunk in
            return self.writeChunkedItemsWithoutThrowing(chunk)
        }  

        return EventLoopFuture.whenAllComplete(futures, on: self.eventLoop).flatMapThrowing { results in
            var errors: Set<BatchStatementErrorCodeEnum> = Set()
            try results.forEach { result in
                let error = try result.get()
                errors = errors.union(error)
            }
            
            return errors
        }
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
        return try await transactWrite(entries, constraints: constraints, retriesRemaining: dynamodb.retryConfiguration.numRetries)
    }
    
    private func transactWrite<WriteEntryType: PolymorphicWriteEntry,
                               TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
                        _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType],
                        retriesRemaining: Int) async throws {
        let entryCount = entries.count + constraints.count
            
        if entryCount > AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement {
            throw SmokeDynamoDBError.transactionSizeExceeded(attemptedSize: entryCount,
                                                             maximumSize: AWSDynamoDBLimits.maximumUpdatesPerTransactionStatement)
        }
        
        let result: Swift.Result<Void, SmokeDynamoDBError>
        do {
            try await self.writeTransactionItems(entries, constraints: constraints)
            
            result = .success(())
        } catch DynamoDBError.transactionCanceled(let exception) {
            guard let cancellationReasons = exception.cancellationReasons else {
                throw SmokeDynamoDBError.transactionCanceled(reasons: [])
            }
            
            let keys = entries.map { $0.compositePrimaryKey } + constraints.map { $0.compositePrimaryKey }
            
            var isTransactionConflict = false
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
                    isTransactionConflict = true
                    
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
            
            if isTransactionConflict && retriesRemaining > 0 {
                return try await retryTransactWrite(entries, constraints: constraints, retriesRemaining: retriesRemaining)
            }
            
            result = .failure(SmokeDynamoDBError.transactionCanceled(reasons: reasons))
        } catch DynamoDBError.transactionConflict(let exception) {
            if retriesRemaining > 0 {
                return try await retryTransactWrite(entries, constraints: constraints, retriesRemaining: retriesRemaining)
            }
            
            let reason = SmokeDynamoDBError.transactionConflict(message: exception.message)
            
            result = .failure(SmokeDynamoDBError.transactionCanceled(reasons: [reason]))
        }
                            
        let retryCount = self.dynamodb.retryConfiguration.numRetries - retriesRemaining
        self.tableMetrics.transactWriteRetryCountRecorder?.record(retryCount)
                            
        switch result {
        case .success:
            return
        case .failure(let failure):
            throw failure
        }
    }
    
    private func retryTransactWrite<WriteEntryType: PolymorphicWriteEntry,
                                    TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
                        _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType],
                        retriesRemaining: Int) async throws {
        // determine the required interval
        let retryInterval = Int(self.dynamodb.retryConfiguration.getRetryInterval(retriesRemaining: retriesRemaining))
                
        logger.warning(
            "Transaction retried due to conflict. Remaining retries: \(retriesRemaining). Retrying in \(retryInterval) ms.")
        try await Task.sleep(nanoseconds: UInt64(retryInterval) * millisecondsToNanoSeconds)
                
        logger.trace("Reattempting request due to remaining retries: \(retryInterval)")
        return try await transactWrite(entries, constraints: constraints, retriesRemaining: retriesRemaining - 1)
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
