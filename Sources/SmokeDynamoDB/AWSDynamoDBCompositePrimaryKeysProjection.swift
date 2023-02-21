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
import AWSMiddleware

public typealias AWSDynamoDBCompositePrimaryKeysProjection = GenericAWSDynamoDBCompositePrimaryKeysProjection<AWSHTTPMiddlewareStack<DynamoDBError>>

public class GenericAWSDynamoDBCompositePrimaryKeysProjection<MiddlewareStackType: AWSHTTPMiddlewareStackProtocol>: DynamoDBCompositePrimaryKeysProjection {
    public let dynamodb: GenericAWSDynamoDBClientV2<MiddlewareStackType>
    public let targetTableName: String
    public let logger: Logger

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
                retryConfiguration: HTTPClientRetryConfiguration = .default) throws {
        self.logger = logger
        self.dynamodb = try GenericAWSDynamoDBClientV2<MiddlewareStackType>(
            credentialsProvider: credentialsProvider, awsRegion: awsRegion,
            endpointHostName: endpointHostName, endpointPort: endpointPort,
            requiresTLS: requiresTLS, service: service,
            contentType: contentType, target: target, logger: logger,
            retryConfiguration: retryConfiguration)
        self.targetTableName = tableName

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(awsRegion)' and hostname: '\(endpointHostName)'")
    }
    
    public init(config: AWSDynamoDBClientConfiguration,
                logger: Logging.Logger = Logger(label: "DynamoDBClient"),
                internalRequestId: String = "none",
                tableName: String) throws {
        self.logger = logger
        self.dynamodb = try config.getAWSClient(logger: logger)
        self.targetTableName = tableName

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(config.awsRegion)' and hostname: '\(config.endpointHostName)'")
    }
    
    public init(operationsClient: AWSDynamoDBTableOperationsClient,
                logger: Logging.Logger = Logger(label: "DynamoDBClient"),
                internalRequestId: String = "none") {
        let config = operationsClient.config
        
        self.logger = logger
        self.dynamodb = config.getAWSClient(logger: logger,
                                            httpClientEngine: operationsClient.httpClientEngine)
        self.targetTableName = operationsClient.tableName

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(config.awsRegion)' and hostname: '\(config.endpointHostName)'")
    }
}
