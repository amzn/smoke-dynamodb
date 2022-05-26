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
// BatchExecuteStatement has a maximum of 25 statements
private let maximumUpdatesPerExecuteStatement = 25

/// DynamoDBTable conformance updateItems function
public extension AWSDynamoDBCompositePrimaryKeyTable {
    
    private func entryToBatchStatementRequest<AttributesType, ItemType>(
        _ entry: WriteEntry<AttributesType, ItemType>) throws -> BatchStatementRequest {
        
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
                
                return BatchStatementRequest(consistentRead: true, statement: statement)
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
    func writeChunkedItemsWithoutThrowing<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws
    -> [Int: BatchStatementError] {
        // if there are no items, there is nothing to update
        
        guard entries.count > 0 else {
            self.logger.info("\(entries) with count = 0")
            return [:]
        }
        
        let statements: [BatchStatementRequest] = try entries.map { try entryToBatchStatementRequest( $0 ) }

        let executeInput = BatchExecuteStatementInput(statements: statements)
        
        let result =  try await dynamodb.batchExecuteStatement(input: executeInput)

        guard let responses = result.responses else {
            self.logger.info("BatchExecuteStatementOutput: \(result) does not contain responses")
            return [:]
        }
        
        var failedList: [Int: BatchStatementError] = [:]

        for (index, response) in responses.enumerated() {
            if let error = response.error {
                failedList[index] = error
            }
        }

        return failedList
    }

    func monomorphicBulkWriteWithoutThrowing<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws
    -> [Int: BatchStatementError] {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedEntries = entries.chunked(by: maximumUpdatesPerExecuteStatement)
        var result: [Int: BatchStatementError] = [:]
        try await chunkedEntries.enumerated().concurrentForEach { (index, chunk) in
            try await self.writeChunkedItemsWithoutThrowing(chunk).forEach { (key, val) in
                result[index * maximumUpdatesPerExecuteStatement + key] = val
            }
        }  
        return result
    }
}
