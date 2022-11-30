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
// BatchExecuteStatement has a maximum of 25 statements
private let maximumUpdatesPerExecuteStatement = 25
private let maxStatementLength = 8192

/// DynamoDBTable conformance updateItems function
public extension AWSDynamoDBCompositePrimaryKeyTable {
    
    func validateEntry<AttributesType, ItemType>(entry: WriteEntry<AttributesType, ItemType>) throws {
        
        let statement: String = try entryToStatement(entry)
        
        if statement.count > maxStatementLength {
            throw SmokeDynamoDBError.statementLengthExceeded(
                reason: "failed to satisfy constraint: Member must have length less than or equal to \(maxStatementLength). Actual length \(statement.count)")
        }
    }
    
    private func entryToStatement<AttributesType, ItemType>(
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
        let chunkedEntries = entries.chunked(by: maximumUpdatesPerExecuteStatement)
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
        let chunkedEntries = entries.chunked(by: maximumUpdatesPerExecuteStatement)

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
}
