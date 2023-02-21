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
// AWSDynamoDBClientConfiguration.swift
// DynamoDBClient
//

import DynamoDBModel
import DynamoDBClient
import SmokeAWSCore
import SmokeHTTPClient
import NIO
import SmokeAWSHttp
import AsyncHTTPClient
import Logging
import ClientRuntime
import AWSMiddleware
import SmokeHTTPMiddleware

/**
 Configuration for an AWS DynamoDB client.
 */
public struct AWSDynamoDBClientConfiguration {
    public let credentialsProvider: CredentialsProvider
    public let awsRegion: AWSRegion
    public let endpointHostName: String
    public let endpointPort: Int
    public let requiresTLS: Bool?
    public let service: String
    public let contentType: String
    public let target: String?
    public let consistentRead: Bool
    public let escapeSingleQuoteInPartiQL: Bool
    public let runtimeConfig: ClientRuntime.SDKRuntimeConfiguration
    public let retryConfiguration: HTTPClientRetryConfiguration
    
    public init(credentialsProvider: CredentialsProvider, awsRegion: AWSRegion,
                endpointHostName endpointHostNameOptional: String? = nil,
                endpointPort: Int = 443,
                requiresTLS: Bool? = nil,
                service: String = "dynamodb",
                contentType: String = "application/x-amz-json-1.0",
                target: String? = "DynamoDB_20120810",
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false,
                runtimeConfig: ClientRuntime.SDKRuntimeConfiguration,
                retryConfiguration: HTTPClientRetryConfiguration = .default) {
        self.credentialsProvider = credentialsProvider
        self.awsRegion = awsRegion
        self.endpointHostName = endpointHostNameOptional ?? "dynamodb.\(awsRegion.rawValue).amazonaws.com"
        self.endpointPort = endpointPort
        self.requiresTLS = requiresTLS
        self.service = service
        self.contentType = contentType
        self.target = target
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.runtimeConfig = runtimeConfig
        self.retryConfiguration = retryConfiguration
    }
    
    internal func getAWSClient<StackType: JSONPayloadTransformStackProtocol>(logger: Logger) throws
    -> GenericAWSDynamoDBClientV2<StackType> {
        return try GenericAWSDynamoDBClientV2(
            credentialsProvider: self.credentialsProvider, awsRegion: self.awsRegion,
            endpointHostName: self.endpointHostName, endpointPort: self.endpointPort,
            requiresTLS: self.requiresTLS, service: self.service,
            contentType: self.contentType, target: self.target, logger: logger,
            retryConfiguration: self.retryConfiguration)
    }
    
    internal func getAWSClient<StackType: JSONPayloadTransformStackProtocol>(logger: Logger,
                                                                             httpClientEngine: SmokeHTTPClientEngine)
    -> GenericAWSDynamoDBClientV2<StackType> {
        return GenericAWSDynamoDBClientV2(
            credentialsProvider: self.credentialsProvider, awsRegion: self.awsRegion,
            endpointHostName: self.endpointHostName, endpointPort: self.endpointPort,
            requiresTLS: self.requiresTLS, service: self.service,
            contentType: self.contentType, target: self.target, logger: logger,
            retryConfiguration: self.retryConfiguration,
            httpClientEngine: httpClientEngine)
    }
}
