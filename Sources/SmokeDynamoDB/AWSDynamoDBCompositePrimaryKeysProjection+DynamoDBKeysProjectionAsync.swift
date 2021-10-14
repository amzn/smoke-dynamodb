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
import SmokeAWSCore
import DynamoDBModel
import SmokeHTTPClient
import Logging
import NIO

/// DynamoDBKeysProjection conformance async functions
public extension AWSDynamoDBCompositePrimaryKeysProjection {
    
    func query<AttributesType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?) -> EventLoopFuture<[CompositePrimaryKey<AttributesType>]>
            where AttributesType: PrimaryKeyAttributes {
        return partialQuery(forPartitionKey: partitionKey,
                            sortKeyCondition: sortKeyCondition,
                            exclusiveStartKey: nil)
    }
    
    // function to return a future with the results of a query call and all future paginated calls
    private func partialQuery<AttributesType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            exclusiveStartKey: String?) -> EventLoopFuture<[CompositePrimaryKey<AttributesType>]>
            where AttributesType: PrimaryKeyAttributes {
        let queryFuture: EventLoopFuture<([CompositePrimaryKey<AttributesType>], String?)> =
            query(forPartitionKey: partitionKey,
                  sortKeyCondition: sortKeyCondition,
                  limit: nil,
                  scanIndexForward: true,
                  exclusiveStartKey: exclusiveStartKey)
        
        return queryFuture.flatMap { paginatedItems in
            // if there are more items
            if let lastEvaluatedKey = paginatedItems.1 {
                // returns a future with all the results from all later paginated calls
                return self.partialQuery(forPartitionKey: partitionKey,
                                         sortKeyCondition: sortKeyCondition,
                                         exclusiveStartKey: lastEvaluatedKey)
                    .map { partialResult in
                        // return the results from 'this' call and all later paginated calls
                        return paginatedItems.0 + partialResult
                    }
            } else {
                // this is it, all results have been obtained
                let promise = self.eventLoop.makePromise(of: [CompositePrimaryKey<AttributesType>].self)
                promise.succeed(paginatedItems.0)
                return promise.futureResult
            }
        }
    }
    
    func query<AttributesType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?, exclusiveStartKey: String?) -> EventLoopFuture<([CompositePrimaryKey<AttributesType>], String?)>
            where AttributesType: PrimaryKeyAttributes {
        return query(forPartitionKey: partitionKey,
                     sortKeyCondition: sortKeyCondition,
                     limit: limit,
                     scanIndexForward: true,
                     exclusiveStartKey: exclusiveStartKey)
        
    }
    
    func query<AttributesType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?,
            scanIndexForward: Bool,
            exclusiveStartKey: String?) -> EventLoopFuture<([CompositePrimaryKey<AttributesType>], String?)>
            where AttributesType: PrimaryKeyAttributes {
        let queryInput: DynamoDBModel.QueryInput
        do {
            queryInput = try DynamoDBModel.QueryInput.forSortKeyCondition(forPartitionKey: partitionKey, targetTableName: targetTableName,
                                                                          primaryKeyType: AttributesType.self,
                                                                          sortKeyCondition: sortKeyCondition, limit: limit,
                                                                          scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                                                          consistentRead: false)
        } catch {
            let promise = self.eventLoop.makePromise(of: ([CompositePrimaryKey<AttributesType>], String?).self)
            promise.fail(error)
            return promise.futureResult
        }
        
        let logMessage = "dynamodb.query with partitionKey: \(partitionKey), " +
            "sortKeyCondition: \(sortKeyCondition.debugDescription), and table name \(targetTableName)."
        self.logger.debug("\(logMessage)")
        
        return dynamodb.query(input: queryInput).flatMapThrowing { queryOutput in
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
                        let attributeValue = DynamoDBModel.AttributeValue(M: values)
                        
                        return try DynamoDBDecoder().decode(attributeValue)
                    }
                } catch {
                    throw error.asUnrecognizedSmokeDynamoDBError()
                }
                
                return (items, lastEvaluatedKey)
            } else {
                return ([], lastEvaluatedKey)
            }
        } .flatMapErrorThrowing { error in
            if let typedError = error as? DynamoDBError {
                throw typedError.asSmokeDynamoDBError()
            }
            
            throw error.asUnrecognizedSmokeDynamoDBError()
        }
    }
}
