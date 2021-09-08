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

/// DynamoDBTable conformance updateItems function
public extension AWSDynamoDBCompositePrimaryKeyTable {
    private func updateChunkedItems<AttributesType, ItemType>(_ items: [(new: TypedDatabaseItem<AttributesType, ItemType>,
                                                                         existing: TypedDatabaseItem<AttributesType, ItemType>?)])
    -> EventLoopFuture<Void> {
        // if there are no items, there is nothing to update
        guard items.count > 0 else {
            let promise = self.eventLoop.makePromise(of: Void.self)
            promise.succeed(())
            return promise.futureResult
        }
        
        let statements: [BatchStatementRequest]
        do {
            statements = try items.map { (new, existing) -> BatchStatementRequest in
                let statement: String
                if let existing = existing {
                    statement = try getUpdateExpression(tableName: self.targetTableName,
                                                        newItem: new,
                                                        existingItem: existing)
                } else {
                    statement = try getInsertExpression(tableName: self.targetTableName,
                                                        newItem: new)
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
    
    func updateOrInsertItems<AttributesType, ItemType>(_ items: [(new: TypedDatabaseItem<AttributesType, ItemType>,
                                                                  existing: TypedDatabaseItem<AttributesType, ItemType>?)])
    -> EventLoopFuture<Void> {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedItems = items.chunked(by: maximumUpdatesPerExecuteStatement)
        let futures = chunkedItems.map { chunk -> EventLoopFuture<Void> in
            return updateChunkedItems(chunk)
        }
        
        return EventLoopFuture.andAllComplete(futures, on: self.eventLoop)
    }
}
