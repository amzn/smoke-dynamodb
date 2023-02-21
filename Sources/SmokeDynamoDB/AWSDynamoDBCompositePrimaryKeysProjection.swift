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
import DynamoDBClient
import DynamoDBModel
import AWSCore
import AWSHttp
import SmokeHTTPClient
import ClientRuntime

public struct AWSDynamoDBCompositePrimaryKeysProjection: DynamoDBCompositePrimaryKeysProjection {
    internal let dynamodb: AWSDynamoDBClientV2
    internal let targetTableName: String
    internal let logger: Logger

    internal class QueryPaginationResults<AttributesType: PrimaryKeyAttributes> {
        var items: [CompositePrimaryKey<AttributesType>] = []
        var exclusiveStartKey: String?
    }
    
    public init(credentialsProvider: CredentialsProvider, awsRegion: AWSRegion,
                logger: Logging.Logger = Logger(label: "DynamoDBClient"),
                internalRequestId: String = "none",
                endpointHostName: String,
                endpointPort: Int = 443,
                requiresTLS: Bool? = nil,
                service: String = "dynamodb",
                contentType: String = "application/x-amz-json-1.0",
                target: String? = "DynamoDB_20120810",
                tableName: String,
                runtimeConfig: ClientRuntime.SDKRuntimeConfiguration,
                retryConfiguration: HTTPClientRetryConfiguration = .default,
                reportingConfiguration: HTTPClientReportingConfiguration<DynamoDBModelOperations>
                    = HTTPClientReportingConfiguration<DynamoDBModelOperations>() ) throws {
        self.logger = logger
        self.dynamodb = try AWSDynamoDBClientV2(credentialsProvider: credentialsProvider, awsRegion: awsRegion,
                                                endpointHostName: endpointHostName, endpointPort: endpointPort,
                                                requiresTLS: requiresTLS, service: service,
                                                contentType: contentType, target: target, logger: logger)
        self.targetTableName = tableName

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(awsRegion)' and hostname: '\(endpointHostName)'")
    }
}
