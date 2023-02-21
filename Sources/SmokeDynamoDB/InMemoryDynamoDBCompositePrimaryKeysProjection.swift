// swiftlint:disable cyclomatic_complexity
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
//  InMemoryDynamoDBCompositePrimaryKeysProjection.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel

public struct InMemoryDynamoDBCompositePrimaryKeysProjection: DynamoDBCompositePrimaryKeysProjection {
    internal let keysWrapper: InMemoryDynamoDBCompositePrimaryKeysProjectionStore

    public init(keys: [Any] = []) {
        self.keysWrapper = InMemoryDynamoDBCompositePrimaryKeysProjectionStore(keys: keys)
    }
    
    @available(*, deprecated, message: "eventLoop no longer needs to be provided")
    public init<EventLoopType>(keys: [Any] = [], eventLoop: EventLoopType) {
        self.keysWrapper = InMemoryDynamoDBCompositePrimaryKeysProjectionStore(keys: keys)
    }
    
    internal init(keysWrapper: InMemoryDynamoDBCompositePrimaryKeysProjectionStore) {
        self.keysWrapper = keysWrapper
    }
    
    public var keys: [Any] {
        get async {
            return await self.keysWrapper.keys
        }
    }

    public func query<AttributesType>(forPartitionKey partitionKey: String,
                                      sortKeyCondition: AttributeCondition?) async throws
    -> [CompositePrimaryKey<AttributesType>] {
        return try await keysWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition)
    }
    
    public func query<AttributesType>(forPartitionKey partitionKey: String, sortKeyCondition: AttributeCondition?,
                                      limit: Int?, exclusiveStartKey: String?) async throws -> ([CompositePrimaryKey<AttributesType>], String?)
    where AttributesType : PrimaryKeyAttributes {
        return try await keysWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                           limit: limit, exclusiveStartKey: exclusiveStartKey)
    }

    public func query<AttributesType>(forPartitionKey partitionKey: String, sortKeyCondition: AttributeCondition?, limit: Int?,
                                      scanIndexForward: Bool, exclusiveStartKey: String?) async throws
    -> ([CompositePrimaryKey<AttributesType>], String?) where AttributesType : PrimaryKeyAttributes {
        return try await keysWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                           limit: limit, scanIndexForward: scanIndexForward,
                                           exclusiveStartKey: exclusiveStartKey)
    }
}
