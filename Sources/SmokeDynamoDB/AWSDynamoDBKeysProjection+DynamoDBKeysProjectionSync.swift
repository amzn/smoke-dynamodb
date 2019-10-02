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
//  AWSDynamoDBKeysProjection+DynamoDBKeysProjectionSync.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeAWSCore
import DynamoDBModel
import SmokeHTTPClient
import LoggerAPI

/// DynamoDBKeysProjection conformance sync functions
public extension AWSDynamoDBKeysProjection {
    
    func querySync<AttributesType>(forPartitionKey partitionKey: String,
                                   sortKeyCondition: AttributeCondition?) throws
        -> [CompositePrimaryKey<AttributesType>] {
          
        var items: [CompositePrimaryKey<AttributesType>] = []
        var exclusiveStartKey: String?
            
        while true {
            let paginatedItems: ([CompositePrimaryKey<AttributesType>], String?) =
                try querySync(forPartitionKey: partitionKey,
                          sortKeyCondition: sortKeyCondition,
                          limit: nil,
                          exclusiveStartKey: exclusiveStartKey)
            
            items += paginatedItems.0
            
            // if there are more items
            if let lastEvaluatedKey = paginatedItems.1 {
                exclusiveStartKey = lastEvaluatedKey
            } else {
                // we have all the items
                return items
            }
        }
    }
    
    func querySync<AttributesType>(forPartitionKey partitionKey: String,
                                   sortKeyCondition: AttributeCondition?,
                                   limit: Int?,
                                   exclusiveStartKey: String?) throws
        -> ([CompositePrimaryKey<AttributesType>], String?)
        where AttributesType: PrimaryKeyAttributes {
            let queryInput = try DynamoDBModel.QueryInput.forSortKeyCondition(forPartitionKey: partitionKey, targetTableName: targetTableName,
                                                                              primaryKeyType: AttributesType.self,
                                                                              sortKeyCondition: sortKeyCondition, limit: limit,
                                                                              exclusiveStartKey: exclusiveStartKey)
            let queryOutput = try dynamodb.querySync(input: queryInput)
            
            let lastEvaluatedKey: String?
            if let returnedLastEvaluatedKey = queryOutput.lastEvaluatedKey {
                let encodedLastEvaluatedKey = try AWSDynamoDBTable.jsonEncoder.encode(returnedLastEvaluatedKey)
                
                lastEvaluatedKey = String(data: encodedLastEvaluatedKey, encoding: .utf8)
            } else {
                lastEvaluatedKey = nil
            }
            
            if let outputAttributeValues = queryOutput.items {
                let items: [CompositePrimaryKey<AttributesType>] = try outputAttributeValues.map { values in
                    let attributeValue = DynamoDBModel.AttributeValue(M: values)
                    
                    return try AWSDynamoDBTable.dynamodbDecoder.decode(attributeValue)
                }
                
                return (items, lastEvaluatedKey)
            } else {
                return ([], lastEvaluatedKey)
            }
    }
}
