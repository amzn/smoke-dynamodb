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

internal extension SmokeHTTPClient.HTTPClientError {
    func isRetriable() -> Bool {
        if let typedError = self.cause as? DynamoDBError, let isRetriable = typedError.isRetriable() {
            return isRetriable
        } else {
            return self.isRetriableAccordingToCategory
        }
    }
}

public typealias AWSDynamoDBClientConfiguration =
    AWSGenericDynamoDBClientConfiguration<StandardHTTPClientCoreInvocationReporting<AWSClientInvocationTraceContext>>

/**
 Configuration for an AWS DynamoDB client.
 */
public struct AWSGenericDynamoDBClientConfiguration<InvocationReportingType: HTTPClientCoreInvocationReporting> {
    public let endpointHostName: String
    public let endpointPort: Int
    public let contentType: String
    public let timeoutConfiguration: HTTPClient.Configuration.Timeout
    public let connectionPoolConfiguration: HTTPClient.Configuration.ConnectionPool?
    public let awsRegion: AWSRegion
    public let retryConfiguration: HTTPClientRetryConfiguration
    public let eventLoopGroup: EventLoopGroup
    public let reportingConfiguration: SmokeAWSClientReportingConfiguration<DynamoDBModelOperations>
    
    internal let clientDelegate: JSONAWSHttpClientDelegate<DynamoDBError>
    internal let reportingProvider: (Logger, String, EventLoop?) -> InvocationReportingType
    internal let credentialsProvider: CredentialsProvider
    
    public init<NewTraceContextType: InvocationTraceContext>(
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
        let useTLS = requiresTLS ?? AWSHTTPClientDelegate.requiresTLS(forEndpointPort: endpointPort)
        
        self.credentialsProvider = credentialsProvider
        self.endpointHostName = endpointHostNameOptional ?? "dynamodb.\(awsRegion.rawValue).amazonaws.com"
        self.endpointPort = endpointPort
        self.contentType = contentType
        self.clientDelegate = JSONAWSHttpClientDelegate<DynamoDBError>(requiresTLS: useTLS)
        self.connectionPoolConfiguration = connectionPoolConfiguration
        self.awsRegion = awsRegion
        self.retryConfiguration = retryConfiguration
        self.eventLoopGroup = AWSClientHelper.getEventLoop(eventLoopGroupProvider: eventLoopProvider)
        self.timeoutConfiguration = timeoutConfiguration
        self.reportingConfiguration = reportingConfiguration
                
        self.reportingProvider = { (logger, internalRequestId, eventLoop) in
            return StandardHTTPClientCoreInvocationReporting(
                logger: logger,
                internalRequestId: internalRequestId,
                traceContext: traceContext,
                eventLoop: eventLoop)
        }
    }
    
    public init(
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
        self.init(credentialsProvider: credentialsProvider,
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
    
    public func createOperationsClient(forTableName tableName: String)
    -> AWSGenericDynamoDBTableOperationsClient<InvocationReportingType> {
        return AWSGenericDynamoDBTableOperationsClient(config: self,
                                                       tableName: tableName)
    }
    
    internal func createClient() -> HTTPOperationsClient {
        return HTTPOperationsClient(
            endpointHostName: self.endpointHostName,
            endpointPort: self.endpointPort,
            contentType: self.contentType,
            clientDelegate: self.clientDelegate,
            timeoutConfiguration: self.timeoutConfiguration,
            eventLoopProvider: .shared(self.eventLoopGroup),
            connectionPoolConfiguration: self.connectionPoolConfiguration)
    }
    
    internal func createAWSClient(logger: Logger, internalRequestId: String,
                                  eventLoopOverride: EventLoop?, httpClientOverride: HTTPOperationsClient?)
    -> _AWSDynamoDBClient<InvocationReportingType> {

        let httpClient: HTTPOperationsClient
        let ownsHttpClient: Bool
        if let httpClientOverride = httpClientOverride {
            httpClient = httpClientOverride
            ownsHttpClient = false
        } else {
            httpClient = createClient()
            ownsHttpClient = true
        }
        
        return _AWSDynamoDBClient(credentialsProvider: self.credentialsProvider,
                                  awsRegion: self.awsRegion,
                                  reporting: self.reportingProvider(logger, internalRequestId, eventLoopOverride),
                                  httpClient: httpClient,
                                  ownsHttpClient: ownsHttpClient,
                                  retryConfiguration: self.retryConfiguration,
                                  reportingConfiguration: self.reportingConfiguration,
                                  retryOnErrorProvider: { error in error.isRetriable() })
    }
    
    internal func createAWSClient<OverrideInvocationReportingType: HTTPClientCoreInvocationReporting>(
        reporting: OverrideInvocationReportingType,
        httpClient: HTTPOperationsClient?)
    -> _AWSDynamoDBClient<OverrideInvocationReportingType> {

        let theHttpClient: HTTPOperationsClient
        let ownsHttpClient: Bool
        if let httpClient = httpClient {
            theHttpClient = httpClient
            ownsHttpClient = false
        } else {
            theHttpClient = createClient()
            ownsHttpClient = true
        }
        
        return _AWSDynamoDBClient(credentialsProvider: self.credentialsProvider,
                                  awsRegion: self.awsRegion,
                                  reporting: reporting,
                                  httpClient: theHttpClient,
                                  ownsHttpClient: ownsHttpClient,
                                  retryConfiguration: self.retryConfiguration,
                                  reportingConfiguration: self.reportingConfiguration,
                                  retryOnErrorProvider: { error in error.isRetriable() })
    }
}
