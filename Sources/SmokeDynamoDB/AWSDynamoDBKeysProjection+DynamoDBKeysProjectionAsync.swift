// Copyright 2018-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
//  AWSDynamoDBKeysProjection+DynamoDBKeysProjectionAsync.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeAWSCore
import DynamoDBModel
import SmokeHTTPClient
import LoggerAPI

/// DynamoDBKeysProjection conformance async functions
public extension AWSDynamoDBKeysProjection {
    
    public func queryAsync<AttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        completion: @escaping (HTTPResult<[CompositePrimaryKey<AttributesType>]>) -> ())
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
        completion: @escaping (HTTPResult<[CompositePrimaryKey<AttributesType>]>) -> ())
        throws where AttributesType: PrimaryKeyAttributes {
            func handleQueryResult(result: HTTPResult<([CompositePrimaryKey<AttributesType>], String?)>) {
                switch result {
                case .response(let paginatedItems):
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
                            completion(.error(error))
                        }
                    } else {
                        // we have all the items
                        completion(.response(partialResults.items))
                    }
                case .error(let error):
                    completion(.error(error))
                }
            }
            
            try queryAsync(forPartitionKey: partitionKey,
                          sortKeyCondition: sortKeyCondition,
                          limit: nil,
                          exclusiveStartKey: partialResults.exclusiveStartKey,
                          completion: handleQueryResult)
    }
    
    public func queryAsync<AttributesType>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        limit: Int?,
        exclusiveStartKey: String?,
        completion: @escaping (HTTPResult<([CompositePrimaryKey<AttributesType>], String?)>) -> ())
        throws where AttributesType: PrimaryKeyAttributes {
            let queryInput = try DynamoDBModel.QueryInput.forSortKeyCondition(forPartitionKey: partitionKey, targetTableName: targetTableName,
                                                                              primaryKeyType: AttributesType.self,
                                                                              sortKeyCondition: sortKeyCondition, limit: limit,
                                                                              exclusiveStartKey: exclusiveStartKey)
            try dynamodb.queryAsync(input: queryInput) { result in
                switch result {
                case .response(let queryOutput):
                    let lastEvaluatedKey: String?
                    if let returnedLastEvaluatedKey = queryOutput.lastEvaluatedKey {
                        let encodedLastEvaluatedKey: Data
                        
                        do {
                            encodedLastEvaluatedKey = try AWSDynamoDBTable.jsonEncoder.encode(returnedLastEvaluatedKey)
                        } catch {
                            return completion(.error(error))
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
                                
                                return try AWSDynamoDBTable.dynamodbDecoder.decode(attributeValue)
                            }
                        } catch {
                            return completion(.error(error))
                        }
                        
                        completion(.response((items, lastEvaluatedKey)))
                    } else {
                        completion(.response(([], lastEvaluatedKey)))
                    }
                case .error(let error):
                    return completion(.error(error))
                }
            }
    }
}
