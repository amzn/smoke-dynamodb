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
    public let service: String
    public let target: String?
    public let retryConfiguration: HTTPClientRetryConfiguration
    public let eventLoopGroup: EventLoopGroup
    public let traceContext: InvocationReportingType.TraceContextType
    public let reportingConfiguration: SmokeAWSClientReportingConfiguration<DynamoDBModelOperations>
    public let ignoreInvocationEventLoop: Bool
    public let enableAHCLogging: Bool
    
    internal let clientDelegate: JSONAWSHttpClientDelegate<DynamoDBError>
    internal let reportingProvider: (Logger, String, EventLoop?, OutwardsRequestAggregator?) -> InvocationReportingType
    internal let credentialsProvider: CredentialsProvider
    
    public init<TraceContextType: InvocationTraceContext>(
        credentialsProvider: CredentialsProvider,
        awsRegion: AWSRegion,
        endpointHostName endpointHostNameOptional: String? = nil,
        endpointPort: Int = 443,
        requiresTLS: Bool? = nil,
        service: String = "dynamodb",
        contentType: String = "application/x-amz-json-1.0",
        target: String? = "DynamoDB_20120810",
        ignoreInvocationEventLoop: Bool = false,
        traceContext: TraceContextType,
        timeoutConfiguration: HTTPClient.Configuration.Timeout = .init(),
        retryConfiguration: HTTPClientRetryConfiguration = .default,
        eventLoopProvider: HTTPClient.EventLoopGroupProvider = .singleton,
        reportingConfiguration: SmokeAWSClientReportingConfiguration<DynamoDBModelOperations>
            = SmokeAWSClientReportingConfiguration<DynamoDBModelOperations>(),
        connectionPoolConfiguration: HTTPClient.Configuration.ConnectionPool? = nil,
        enableAHCLogging: Bool = false)
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<TraceContextType> {
        let useTLS = requiresTLS ?? AWSHTTPClientDelegate.requiresTLS(forEndpointPort: endpointPort)
        
        self.credentialsProvider = credentialsProvider
        self.endpointHostName = endpointHostNameOptional ?? "dynamodb.\(awsRegion.rawValue).amazonaws.com"
        self.endpointPort = endpointPort
        self.service = service
        self.contentType = contentType
        self.target = target
        self.clientDelegate = JSONAWSHttpClientDelegate<DynamoDBError>(requiresTLS: useTLS)
        self.connectionPoolConfiguration = connectionPoolConfiguration
        self.awsRegion = awsRegion
        self.retryConfiguration = retryConfiguration
        self.eventLoopGroup = AWSClientHelper.getEventLoop(eventLoopGroupProvider: eventLoopProvider)
        self.ignoreInvocationEventLoop = ignoreInvocationEventLoop
        self.traceContext = traceContext
        self.timeoutConfiguration = timeoutConfiguration
        self.reportingConfiguration = reportingConfiguration
        self.enableAHCLogging = enableAHCLogging
                
        self.reportingProvider = { (logger, internalRequestId, eventLoop, outwardsRequestAggregator) in
            return StandardHTTPClientCoreInvocationReporting(
                logger: logger,
                internalRequestId: internalRequestId,
                traceContext: traceContext,
                eventLoop: eventLoop,
                outwardsRequestAggregator: outwardsRequestAggregator)
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
        ignoreInvocationEventLoop: Bool = false,
        timeoutConfiguration: HTTPClient.Configuration.Timeout = .init(),
        retryConfiguration: HTTPClientRetryConfiguration = .default,
        eventLoopProvider: HTTPClient.EventLoopGroupProvider = .singleton,
        reportingConfiguration: SmokeAWSClientReportingConfiguration<DynamoDBModelOperations>
            = SmokeAWSClientReportingConfiguration<DynamoDBModelOperations>(),
        connectionPoolConfiguration: HTTPClient.Configuration.ConnectionPool? = nil,
        enableAHCLogging: Bool = false)
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<AWSClientInvocationTraceContext> {
        self.init(credentialsProvider: credentialsProvider,
                  awsRegion: awsRegion,
                  endpointHostName: endpointHostNameOptional,
                  endpointPort: endpointPort,
                  requiresTLS: requiresTLS,
                  service: service,
                  contentType: contentType,
                  target: target,
                  ignoreInvocationEventLoop: ignoreInvocationEventLoop,
                  traceContext: AWSClientInvocationTraceContext(),
                  timeoutConfiguration: timeoutConfiguration,
                  retryConfiguration: retryConfiguration,
                  eventLoopProvider: eventLoopProvider,
                  reportingConfiguration: reportingConfiguration,
                  connectionPoolConfiguration: connectionPoolConfiguration,
                  enableAHCLogging: enableAHCLogging)
    }
    
    public func createOperationsClient(forTableName tableName: String)
    -> AWSGenericDynamoDBTableOperationsClient<InvocationReportingType> {
        return AWSGenericDynamoDBTableOperationsClient(config: self,
                                                       tableName: tableName)
    }
    
    internal func createHTTPOperationsClient() -> HTTPOperationsClient {
        return HTTPOperationsClient(
            endpointHostName: self.endpointHostName,
            endpointPort: self.endpointPort,
            contentType: self.contentType,
            clientDelegate: self.clientDelegate,
            timeoutConfiguration: self.timeoutConfiguration,
            eventLoopProvider: .shared(self.eventLoopGroup),
            connectionPoolConfiguration: self.connectionPoolConfiguration,
            enableAHCLogging: enableAHCLogging)
    }
    
    internal func createAWSClient(logger: Logger, internalRequestId: String,
                                  eventLoopOverride: EventLoop?, httpClientOverride: HTTPOperationsClient?,
                                  outwardsRequestAggregator: OutwardsRequestAggregator?)
    -> _AWSDynamoDBClient<InvocationReportingType> {

        let httpClient: HTTPOperationsClient
        let ownsHttpClient: Bool
        if let httpClientOverride = httpClientOverride {
            httpClient = httpClientOverride
            ownsHttpClient = false
        } else {
            httpClient = createHTTPOperationsClient()
            ownsHttpClient = true
        }
        
        return _AWSDynamoDBClient(credentialsProvider: self.credentialsProvider,
                                  awsRegion: self.awsRegion,
                                  reporting: self.reportingProvider(logger, internalRequestId,
                                                                    eventLoopOverride, outwardsRequestAggregator),
                                  service: self.service,
                                  target: self.target,
                                  httpClient: httpClient,
                                  ownsHttpClient: ownsHttpClient,
                                  retryConfiguration: self.retryConfiguration,
                                  reportingConfiguration: self.reportingConfiguration,
                                  retryOnErrorProvider: { error in error.isRetriable() })
    }
    
    internal func createAWSClient<OverrideInvocationReportingType: HTTPClientCoreInvocationReporting>(
        reporting: OverrideInvocationReportingType,
        httpClientOverride: HTTPOperationsClient?)
    -> _AWSDynamoDBClient<OverrideInvocationReportingType> {

        let httpClient: HTTPOperationsClient
        let ownsHttpClient: Bool
        if let httpClientOverride = httpClientOverride {
            httpClient = httpClientOverride
            ownsHttpClient = false
        } else {
            httpClient = createHTTPOperationsClient()
            ownsHttpClient = true
        }
        
        return _AWSDynamoDBClient(credentialsProvider: self.credentialsProvider,
                                  awsRegion: self.awsRegion,
                                  reporting: reporting,
                                  httpClient: httpClient,
                                  ownsHttpClient: ownsHttpClient,
                                  retryConfiguration: self.retryConfiguration,
                                  reportingConfiguration: self.reportingConfiguration,
                                  retryOnErrorProvider: { error in error.isRetriable() })
    }
}
