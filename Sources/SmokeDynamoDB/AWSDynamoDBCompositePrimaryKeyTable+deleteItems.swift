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
//  AWSDynamoDBCompositePrimaryKeyTable+deleteItems.swift
//  SmokeDynamoDB
//

import Foundation
import AWSDynamoDB
import Logging

// BatchExecuteStatement has a maximum of 25 statements
private let maximumUpdatesPerExecuteStatement = 25

/// DynamoDBTable conformance updateItems function
public extension AWSDynamoDBCompositePrimaryKeyTable {
    private func deleteChunkedItems<AttributesType>(_ keys: [CompositePrimaryKey<AttributesType>]) async throws {
        // if there are no keys, there is nothing to update
        guard keys.count > 0 else {
            return
        }
        
        let statements = try keys.map { existingKey -> DynamoDBClientTypes.BatchStatementRequest in
            let statement = try getDeleteExpression(tableName: self.targetTableName,
                                                    existingKey: existingKey)
                
            return DynamoDBClientTypes.BatchStatementRequest(consistentRead: true, statement: statement)
        }
        
        let executeInput = BatchExecuteStatementInput(statements: statements)
        
        let response = try await self.dynamodb.batchExecuteStatement(input: executeInput)
        try throwOnBatchExecuteStatementErrors(response: response)
    }
    
    private func deleteChunkedItems<ItemType: DatabaseItem>(_ existingItems: [ItemType]) async throws {
        // if there are no items, there is nothing to update
        guard existingItems.count > 0 else {
            return
        }
        
        let statements = try existingItems.map { existingItem -> DynamoDBClientTypes.BatchStatementRequest in
            let statement = try getDeleteExpression(tableName: self.targetTableName,
                                                    existingItem: existingItem)
                
            return DynamoDBClientTypes.BatchStatementRequest(consistentRead: true, statement: statement)
        }
        
        let executeInput = BatchExecuteStatementInput(statements: statements)
        
        let response = try await self.dynamodb.batchExecuteStatement(input: executeInput)
        try throwOnBatchExecuteStatementErrors(response: response)
    }
    
    func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedKeys = keys.chunked(by: maximumUpdatesPerExecuteStatement)
        try await chunkedKeys.concurrentForEach { chunk in
            try await self.deleteChunkedItems(chunk)
        }
    }
    
    func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType]) async throws {
        // BatchExecuteStatement has a maximum of 25 statements
        // This function handles pagination internally.
        let chunkedItems = existingItems.chunked(by: maximumUpdatesPerExecuteStatement)
        try await chunkedItems.concurrentForEach { chunk in
            try await self.deleteChunkedItems(chunk)
        }
    }
}
