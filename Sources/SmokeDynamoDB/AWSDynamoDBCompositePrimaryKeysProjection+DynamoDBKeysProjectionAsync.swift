// Copyright 2018-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

/// DynamoDBKeysProjection conformance async functions
public extension AWSDynamoDBCompositePrimaryKeysProjection {
    
    func queryAsync<AttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        completion: @escaping (SmokeDynamoDBErrorResult<[CompositePrimaryKey<AttributesType>]>) -> ())
        throws where AttributesType: PrimaryKeyAttributes {
            let partialResults = QueryPaginationResults<AttributesType>()
            
            try partialQueryAsync(forPartitionKey: partitionKey,
                                  sortKeyCondition: sortKeyCondition,
                                  partialResults: partialResults,
                                  completion: completion)
    }
    
    private func partialQueryAsync<AttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        partialResults: QueryPaginationResults<AttributesType>,
        completion: @escaping (SmokeDynamoDBErrorResult<[CompositePrimaryKey<AttributesType>]>) -> ())
        throws where AttributesType: PrimaryKeyAttributes {
            func handleQueryResult(result: SmokeDynamoDBErrorResult<([CompositePrimaryKey<AttributesType>], String?)>) {
                switch result {
                case .success(let paginatedItems):
                    partialResults.items += paginatedItems.0
            
                    // if there are more items
                    if let lastEvaluatedKey = paginatedItems.1 {
                        partialResults.exclusiveStartKey = lastEvaluatedKey
                        
                        do {
                            try partialQueryAsync(forPartitionKey: partitionKey,
                                                  sortKeyCondition: sortKeyCondition,
                                                  partialResults: partialResults,
                                                  completion: completion)
                        } catch {
                            completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
                        }
                    } else {
                        // we have all the items
                        completion(.success(partialResults.items))
                    }
                case .failure(let error):
                    completion(.failure(error))
                }
            }
            
            try queryAsync(forPartitionKey: partitionKey,
                          sortKeyCondition: sortKeyCondition,
                          limit: nil,
                          exclusiveStartKey: partialResults.exclusiveStartKey,
                          completion: handleQueryResult)
    }
    
    func queryAsync<AttributesType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?, exclusiveStartKey: String?,
            completion: @escaping (SmokeDynamoDBErrorResult<([CompositePrimaryKey<AttributesType>], String?)>) -> ())
        throws where AttributesType: PrimaryKeyAttributes {
            try queryAsync(forPartitionKey: partitionKey,
                           sortKeyCondition: sortKeyCondition,
                           limit: limit,
                           scanIndexForward: true,
                           exclusiveStartKey: exclusiveStartKey,
                           completion: completion)
    }
    
    func queryAsync<AttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        scanIndexForward: Bool,
        exclusiveStartKey: String?,
        completion: @escaping (SmokeDynamoDBErrorResult<([CompositePrimaryKey<AttributesType>], String?)>) -> ())
        throws where AttributesType: PrimaryKeyAttributes {
            let queryInput = try DynamoDBModel.QueryInput.forSortKeyCondition(forPartitionKey: partitionKey, targetTableName: targetTableName,
                                                                              primaryKeyType: AttributesType.self,
                                                                              sortKeyCondition: sortKeyCondition, limit: limit,
                                                                              scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey)
        
            let logMessage = "dynamodb.query with partitionKey: \(partitionKey), " +
                "sortKeyCondition: \(sortKeyCondition.debugDescription), and table name \(targetTableName)."
            self.logger.debug("\(logMessage)")
        
            try dynamodb.queryAsync(input: queryInput) { result in
                switch result {
                case .success(let queryOutput):
                    let lastEvaluatedKey: String?
                    if let returnedLastEvaluatedKey = queryOutput.lastEvaluatedKey {
                        let encodedLastEvaluatedKey: Data
                        
                        do {
                            encodedLastEvaluatedKey = try JSONEncoder().encode(returnedLastEvaluatedKey)
                        } catch {
                            return completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
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
                            return completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
                        }
                        
                        completion(.success((items, lastEvaluatedKey)))
                    } else {
                        completion(.success(([], lastEvaluatedKey)))
                    }
                case .failure(let error):
                    return completion(.failure(error.asSmokeDynamoDBError()))
                }
            }
    }
}
