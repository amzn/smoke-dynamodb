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

public typealias AWSDynamoDBTableOperationsClient =
    AWSGenericDynamoDBTableOperationsClient<StandardHTTPClientCoreInvocationReporting<AWSClientInvocationTraceContext>>

public struct AWSGenericDynamoDBTableOperationsClient<InvocationReportingType: HTTPClientCoreInvocationReporting> {
    public let config: AWSGenericDynamoDBClientConfiguration<InvocationReportingType>
    public let tableName: String
    public let httpClient: HTTPOperationsClient
    
    public init<NewTraceContextType: InvocationTraceContext>(
        tableName: String,
        credentialsProvider: CredentialsProvider,
        awsRegion: AWSRegion,
        endpointHostName endpointHostNameOptional: String? = nil,
        endpointPort: Int = 443,
        requiresTLS: Bool? = nil,
        service: String = "dynamodb",
        contentType: String = "application/x-amz-json-1.0",
        target: String? = "DynamoDB_20120810",
        traceContext: NewTraceContextType,
        timeoutConfiguration: HTTPClient.Configuration.Timeout = .init(),
        retryConfiguration: HTTPClientRetryConfiguration = .default,
        eventLoopProvider: HTTPClient.EventLoopGroupProvider = .createNew,
        reportingConfiguration: SmokeAWSClientReportingConfiguration<DynamoDBModelOperations>
            = SmokeAWSClientReportingConfiguration<DynamoDBModelOperations>(),
        connectionPoolConfiguration: HTTPClient.Configuration.ConnectionPool? = nil)
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<NewTraceContextType> {
        self.config = AWSGenericDynamoDBClientConfiguration(
            credentialsProvider: credentialsProvider,
            awsRegion: awsRegion,
            endpointHostName: endpointHostNameOptional,
            endpointPort: endpointPort,
            requiresTLS: requiresTLS,
            service: service,
            contentType: contentType,
            target: target,
            traceContext: traceContext,
            timeoutConfiguration: timeoutConfiguration,
            retryConfiguration: retryConfiguration,
            eventLoopProvider: eventLoopProvider,
            reportingConfiguration: reportingConfiguration,
            connectionPoolConfiguration: connectionPoolConfiguration)
        self.httpClient = self.config.createClient()
        self.tableName = tableName
    }
    
    public init(
        tableName: String,
        credentialsProvider: CredentialsProvider,
        awsRegion: AWSRegion,
        endpointHostName endpointHostNameOptional: String? = nil,
        endpointPort: Int = 443,
        requiresTLS: Bool? = nil,
        service: String = "dynamodb",
        contentType: String = "application/x-amz-json-1.0",
        target: String? = "DynamoDB_20120810",
        timeoutConfiguration: HTTPClient.Configuration.Timeout = .init(),
        retryConfiguration: HTTPClientRetryConfiguration = .default,
        eventLoopProvider: HTTPClient.EventLoopGroupProvider = .createNew,
        reportingConfiguration: SmokeAWSClientReportingConfiguration<DynamoDBModelOperations>
            = SmokeAWSClientReportingConfiguration<DynamoDBModelOperations>(),
        connectionPoolConfiguration: HTTPClient.Configuration.ConnectionPool? = nil)
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<AWSClientInvocationTraceContext> {
        self.init(tableName: tableName,
                  credentialsProvider: credentialsProvider,
                  awsRegion: awsRegion,
                  endpointHostName: endpointHostNameOptional,
                  endpointPort: endpointPort,
                  requiresTLS: requiresTLS,
                  service: service,
                  contentType: contentType,
                  target: target,
                  traceContext: AWSClientInvocationTraceContext(),
                  timeoutConfiguration: timeoutConfiguration,
                  retryConfiguration: retryConfiguration,
                  eventLoopProvider: eventLoopProvider,
                  reportingConfiguration: reportingConfiguration,
                  connectionPoolConfiguration: connectionPoolConfiguration)
    }
    
    internal init(config: AWSGenericDynamoDBClientConfiguration<InvocationReportingType>,
                  tableName: String) {
        self.config = config
        self.tableName = tableName
        self.httpClient = self.config.createClient()
    }
    
    /**
     Gracefully shuts down the eventloop if owned by this client.
     This function is idempotent and will handle being called multiple
     times. Will block until shutdown is complete.
     */
    public func syncShutdown() throws {
        try httpClient.syncShutdown()
    }
    
    /**
     Gracefully shuts down the eventloop if owned by this client.
     This function is idempotent and will handle being called multiple
     times. Will return when shutdown is complete.
     */
#if (os(Linux) && compiler(>=5.5)) || (!os(Linux) && compiler(>=5.5.2)) && canImport(_Concurrency)
    public func shutdown() async throws {
        try await httpClient.shutdown()
    }
#endif
}
