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
//  AWSDynamoDBCompositePrimaryKeysProjection+DynamoDBKeysProjectionAsync.swift
//  SmokeDynamoDB
//

import Foundation
import AWSDynamoDB
import Logging

/// DynamoDBKeysProjection conformance async functions
public extension AWSDynamoDBCompositePrimaryKeysProjection {
    
    func query<AttributesType>(forPartitionKey partitionKey: String,
                                      sortKeyCondition: AttributeCondition?) async throws
    -> [CompositePrimaryKey<AttributesType>] {
        return try await partialQuery(forPartitionKey: partitionKey,
                                      sortKeyCondition: sortKeyCondition,
                                      exclusiveStartKey: nil)
    }
    
    // function to return a future with the results of a query call and all future paginated calls
    private func partialQuery<AttributesType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            exclusiveStartKey: String?) async throws
    -> [CompositePrimaryKey<AttributesType>] {
        let paginatedItems: (keys: [CompositePrimaryKey<AttributesType>], lastEvaluatedKey: String?) =
            try await query(forPartitionKey: partitionKey,
                            sortKeyCondition: sortKeyCondition,
                            limit: nil,
                            scanIndexForward: true,
                            exclusiveStartKey: exclusiveStartKey)
        
        // if there are more items
        if let lastEvaluatedKey = paginatedItems.lastEvaluatedKey {
            // returns the results from all later paginated calls
            let partialResult: [CompositePrimaryKey<AttributesType>] = try await self.partialQuery(
                forPartitionKey: partitionKey,
                sortKeyCondition: sortKeyCondition,
                exclusiveStartKey: lastEvaluatedKey)
                
            // return the results from 'this' call and all later paginated calls
            return paginatedItems.keys + partialResult
        } else {
            // this is it, all results have been obtained
            return paginatedItems.keys
        }
    }
    
    func query<AttributesType>(forPartitionKey partitionKey: String,
                                      sortKeyCondition: AttributeCondition?,
                                      limit: Int?,
                                      exclusiveStartKey: String?) async throws
    -> (keys: [CompositePrimaryKey<AttributesType>], lastEvaluatedKey: String?) {
        return try await query(forPartitionKey: partitionKey,
                               sortKeyCondition: sortKeyCondition,
                               limit: limit,
                               scanIndexForward: true,
                               exclusiveStartKey: exclusiveStartKey)
        
    }
    
    func query<AttributesType>(forPartitionKey partitionKey: String,
                                      sortKeyCondition: AttributeCondition?,
                                      limit: Int?,
                                      scanIndexForward: Bool,
                                      exclusiveStartKey: String?) async throws
    -> (keys: [CompositePrimaryKey<AttributesType>], lastEvaluatedKey: String?) {
        let queryInput = try AWSDynamoDB.QueryInput.forSortKeyCondition(partitionKey: partitionKey, targetTableName: targetTableName,
                                                                          primaryKeyType: AttributesType.self,
                                                                          sortKeyCondition: sortKeyCondition, limit: limit,
                                                                          scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                                                          consistentRead: false)
        
        let logMessage = "dynamodb.query with partitionKey: \(partitionKey), " +
            "sortKeyCondition: \(sortKeyCondition.debugDescription), and table name \(targetTableName)."
        self.logger.trace("\(logMessage)")
        
        let queryOutput = try await self.dynamodb.query(input: queryInput)
        
        let lastEvaluatedKey: String?
        if let returnedLastEvaluatedKey = queryOutput.lastEvaluatedKey {
            let encodedLastEvaluatedKey: Data
            
            do {
                encodedLastEvaluatedKey = try JSONEncoder().encode(returnedLastEvaluatedKey)
            } catch {
                throw error.asUnrecognizedSmokeDynamoDBError()
            }
            
            lastEvaluatedKey = String(data: encodedLastEvaluatedKey, encoding: .utf8)
        } else {
            lastEvaluatedKey = nil
        }
        
        if let outputAttributeValues = queryOutput.items {
            let items: [CompositePrimaryKey<AttributesType>]
            
            do {
                items = try outputAttributeValues.map { values in
                    let attributeValue: DynamoDBClientTypes.AttributeValue = .m(values)
                    
                    return try DynamoDBDecoder().decode(attributeValue)
                }
            } catch {
                throw error.asUnrecognizedSmokeDynamoDBError()
            }
            
            return (items, lastEvaluatedKey)
        } else {
            return ([], lastEvaluatedKey)
        }
    }
}
