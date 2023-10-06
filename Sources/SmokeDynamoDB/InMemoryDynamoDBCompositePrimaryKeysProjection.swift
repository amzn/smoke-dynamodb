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
import NIO

public class InMemoryDynamoDBCompositePrimaryKeysProjection: DynamoDBCompositePrimaryKeysProjection {
    public var eventLoop: EventLoop

    internal let keysWrapper: InMemoryDynamoDBCompositePrimaryKeysProjectionStore
    
    public var keys: [Any] {
        do {
            return try keysWrapper.getKeys(eventLoop: self.eventLoop).wait()
        } catch {
            fatalError("Unable to retrieve InMemoryDynamoDBCompositePrimaryKeysProjection keys.")
        }
    }

    public init(keys: [Any] = [], eventLoop: EventLoop) {
        self.keysWrapper = InMemoryDynamoDBCompositePrimaryKeysProjectionStore(keys: keys)
        self.eventLoop = eventLoop
    }
    
    internal init(eventLoop: EventLoop,
                  keysWrapper: InMemoryDynamoDBCompositePrimaryKeysProjectionStore) {
        self.eventLoop = eventLoop
        self.keysWrapper = keysWrapper
    }
    
    public func on(eventLoop: EventLoop) -> InMemoryDynamoDBCompositePrimaryKeysProjection {
        return InMemoryDynamoDBCompositePrimaryKeysProjection(eventLoop: eventLoop,
                                                              keysWrapper: self.keysWrapper)
    }

    public func query<AttributesType>(forPartitionKey partitionKey: String,
                                      sortKeyCondition: AttributeCondition?)
    -> EventLoopFuture<[CompositePrimaryKey<AttributesType>]> {
        return keysWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition, eventLoop: self.eventLoop)
    }
    
    public func query<AttributesType>(forPartitionKey partitionKey: String,
                                      sortKeyCondition: AttributeCondition?,
                                      limit: Int?,
                                      exclusiveStartKey: String?)
    -> EventLoopFuture<([CompositePrimaryKey<AttributesType>], String?)>
            where AttributesType: PrimaryKeyAttributes {
        return keysWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                 limit: limit, exclusiveStartKey: exclusiveStartKey, eventLoop: self.eventLoop)
    }

    public func query<AttributesType>(forPartitionKey partitionKey: String,
                                      sortKeyCondition: AttributeCondition?,
                                      limit: Int?,
                                      scanIndexForward: Bool,
                                      exclusiveStartKey: String?)
    -> EventLoopFuture<([CompositePrimaryKey<AttributesType>], String?)>
    where AttributesType: PrimaryKeyAttributes {
        return keysWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                 limit: limit, scanIndexForward: scanIndexForward,
                                 exclusiveStartKey: exclusiveStartKey, eventLoop: self.eventLoop)
    }
}
