// Copyright 2018-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
// AWSDynamoDBTableOperationsClient.swift
// DynamoDBClient
//

import DynamoDBModel
import SmokeAWSCore
import SmokeHTTPClient
import SmokeAWSHttp
import AsyncHTTPClient
import ClientRuntime
import DynamoDBClient
import SmokeHTTPMiddleware

public class AWSDynamoDBTableOperationsClient {
    public let httpClientEngine: SmokeHTTPClientEngine
    public let config: AWSDynamoDBClientConfiguration
    public let tableName: String
    public let consistentRead: Bool
    public let escapeSingleQuoteInPartiQL: Bool
    
    public init(credentialsProvider: CredentialsProvider, awsRegion: AWSRegion,
                endpointHostName: String,
                endpointPort: Int = 443,
                requiresTLS: Bool? = nil,
                service: String = "dynamodb",
                contentType: String = "application/x-amz-json-1.0",
                target: String? = "DynamoDB_20120810",
                tableName: String,
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false,
                runtimeConfig: ClientRuntime.SDKRuntimeConfiguration,
                retryConfiguration: HTTPClientRetryConfiguration = .default) {
        self.config = AWSDynamoDBClientConfiguration(credentialsProvider: credentialsProvider, awsRegion: awsRegion,
                                                     endpointHostName: endpointHostName,
                                                     endpointPort: endpointPort,
                                                     requiresTLS: requiresTLS,
                                                     service: service,
                                                     contentType: contentType,
                                                     target: target,
                                                     consistentRead: consistentRead,
                                                     escapeSingleQuoteInPartiQL: escapeSingleQuoteInPartiQL,
                                                     runtimeConfig: runtimeConfig,
                                                     retryConfiguration: retryConfiguration)
        self.httpClientEngine = SmokeHTTPClientEngine(runtimeConfig: runtimeConfig)
        self.tableName = tableName
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
    }
    
    public init(credentialsProvider: CredentialsProvider, awsRegion: AWSRegion,
                endpointHostName: String,
                endpointPort: Int = 443,
                requiresTLS: Bool? = nil,
                service: String = "dynamodb",
                contentType: String = "application/x-amz-json-1.0",
                target: String? = "DynamoDB_20120810",
                tableName: String,
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false,
                retryConfiguration: HTTPClientRetryConfiguration = .default) throws {
        let runtimeConfig = try ClientRuntime.DefaultSDKRuntimeConfiguration("DynamoDBClient")
        self.config = AWSDynamoDBClientConfiguration(credentialsProvider: credentialsProvider, awsRegion: awsRegion,
                                                     endpointHostName: endpointHostName,
                                                     endpointPort: endpointPort,
                                                     requiresTLS: requiresTLS,
                                                     service: service,
                                                     contentType: contentType,
                                                     target: target,
                                                     consistentRead: consistentRead,
                                                     escapeSingleQuoteInPartiQL: escapeSingleQuoteInPartiQL,
                                                     runtimeConfig: runtimeConfig,
                                                     retryConfiguration: retryConfiguration)
        self.httpClientEngine = SmokeHTTPClientEngine(runtimeConfig: runtimeConfig)
        self.tableName = tableName
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
    }
    
    public init(config: AWSDynamoDBClientConfiguration,
                tableName: String,
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false) {
        self.config = config
        self.httpClientEngine = SmokeHTTPClientEngine(runtimeConfig: config.runtimeConfig)
        self.tableName = tableName
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
    }
}
