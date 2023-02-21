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
//  DynamoDBCompositePrimaryKeyTable+consistentReadQuery.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel

public extension DynamoDBCompositePrimaryKeyTable {
    
#if (os(Linux) && compiler(>=5.5)) || (!os(Linux) && compiler(>=5.5.2)) && canImport(_Concurrency)
    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?) async throws
    -> [ReturnedType] {
        return try await query(forPartitionKey: partitionKey,
                               sortKeyCondition: sortKeyCondition,
                               consistentRead: self.consistentRead)
    }
    
    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             limit: Int?,
                                                             exclusiveStartKey: String?) async throws
    -> ([ReturnedType], String?) {
        return try await query(forPartitionKey: partitionKey,
                               sortKeyCondition: sortKeyCondition,
                               limit: limit,
                               exclusiveStartKey: exclusiveStartKey,
                               consistentRead: self.consistentRead)
    }
    
    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             limit: Int?,
                                                             scanIndexForward: Bool,
                                                             exclusiveStartKey: String?) async throws
    -> ([ReturnedType], String?) {
        return try await query(forPartitionKey: partitionKey,
                               sortKeyCondition: sortKeyCondition,
                               limit: limit,
                               scanIndexForward: scanIndexForward,
                               exclusiveStartKey: exclusiveStartKey,
                               consistentRead: self.consistentRead)
    }
    
    func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                    sortKeyCondition: AttributeCondition?) async throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        return try await monomorphicQuery(forPartitionKey: partitionKey,
                                          sortKeyCondition: sortKeyCondition,
                                          consistentRead: self.consistentRead)
    }
    
    func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                    sortKeyCondition: AttributeCondition?,
                                                    limit: Int?,
                                                    scanIndexForward: Bool,
                                                    exclusiveStartKey: String?) async throws
    -> ([TypedDatabaseItem<AttributesType, ItemType>], String?) {
        return try await monomorphicQuery(forPartitionKey: partitionKey,
                                          sortKeyCondition: sortKeyCondition,
                                          limit: limit,
                                          scanIndexForward: scanIndexForward,
                                          exclusiveStartKey: exclusiveStartKey,
                                          consistentRead: self.consistentRead)
    }
#endif
}
