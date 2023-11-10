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
//  AWSDynamoDBCompositePrimaryKeysProjection.swift
//  SmokeDynamoDB
//

import Foundation
import Logging
import AWSDynamoDB
import AWSClientRuntime
import AwsCommonRuntimeKit

public class AWSDynamoDBCompositePrimaryKeysProjection: DynamoDBCompositePrimaryKeysProjection {
    internal let dynamodb: AWSDynamoDB.DynamoDBClient
    internal let targetTableName: String
    internal let logger: Logging.Logger

    internal class QueryPaginationResults<AttributesType: PrimaryKeyAttributes> {
        var items: [CompositePrimaryKey<AttributesType>] = []
        var exclusiveStartKey: String?
    }

    public init(tableName: String, region: Swift.String,
                credentialsProvider: AWSClientRuntime.CredentialsProviding? = nil,
                connectTimeoutMs: UInt32? = nil,
                logger: Logging.Logger? = nil) throws {
        self.logger = logger ?? Logging.Logger(label: "AWSDynamoDBCompositePrimaryKeysProjection")
        let config = try DynamoDBClient.DynamoDBClientConfiguration(region: region,
                                                                    credentialsProvider: credentialsProvider,
                                                                    connectTimeoutMs: connectTimeoutMs)
        self.dynamodb = AWSDynamoDB.DynamoDBClient(config: config)
        self.targetTableName = tableName

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(region)'")
    }
    
    public init(tableName: String,
                client: AWSDynamoDB.DynamoDBClient,
                logger: Logging.Logger? = nil) {
        self.logger = logger ?? Logging.Logger(label: "AWSDynamoDBCompositePrimaryKeysProjection")
        self.dynamodb = client
        self.targetTableName = tableName

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created existing client")
    }
}
