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
//  DynamoDBCompositePrimaryKeysProjection.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel
import NIO

/**
 Protocol presenting a Keys Only projection of a DynamoDB table such as a Keys Only GSI projection.
 Provides the ability to query the projection to get the list of keys without attempting to decode the row into a particular data type.
 */
public protocol DynamoDBCompositePrimaryKeysProjection {
    var eventLoop: EventLoop { get }

    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will potentially make multiple calls to DynamoDB to retrieve all results for
       the query.
     */
    func query<AttributesType>(forPartitionKey partitionKey: String,
                               sortKeyCondition: AttributeCondition?)
        -> EventLoopFuture<[CompositePrimaryKey<AttributesType>]>

    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will return paginated results based on the limit and exclusiveStartKey provided.
     */
    func query<AttributesType>(forPartitionKey partitionKey: String,
                               sortKeyCondition: AttributeCondition?,
                               limit: Int?,
                               exclusiveStartKey: String?)
        -> EventLoopFuture<([CompositePrimaryKey<AttributesType>], String?)>
    
    func query<AttributesType>(forPartitionKey partitionKey: String,
                               sortKeyCondition: AttributeCondition?,
                               limit: Int?,
                               scanIndexForward: Bool,
                               exclusiveStartKey: String?)
        -> EventLoopFuture<([CompositePrimaryKey<AttributesType>], String?)>
}
