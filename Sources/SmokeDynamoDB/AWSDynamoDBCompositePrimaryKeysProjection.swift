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
import SmokeAWSCore
import SmokeAWSHttp
import SmokeHTTPClient
import AsyncHTTPClient
import NIO

public class AWSDynamoDBCompositePrimaryKeysProjection<InvocationReportingType: HTTPClientCoreInvocationReporting>: DynamoDBCompositePrimaryKeysProjection {
    internal let dynamodb: _AWSDynamoDBClient<InvocationReportingType>
    internal let targetTableName: String
    internal let logger: Logger

    internal class QueryPaginationResults<AttributesType: PrimaryKeyAttributes> {
        var items: [CompositePrimaryKey<AttributesType>] = []
        var exclusiveStartKey: String?
    }

    public init(accessKeyId: String, secretAccessKey: String,
                region: AWSRegion, reporting: InvocationReportingType,
                endpointHostName: String, endpointPort: Int = 443,
                requiresTLS: Bool? = nil, tableName: String,
                connectionTimeoutSeconds: Int64 = 10,
                retryConfiguration: HTTPClientRetryConfiguration = .default,
                eventLoopProvider: HTTPClient.EventLoopGroupProvider = .createNew,
                reportingConfiguration: SmokeAWSCore.SmokeAWSClientReportingConfiguration<DynamoDBModel.DynamoDBModelOperations>
                    = SmokeAWSClientReportingConfiguration<DynamoDBModelOperations>()) {
        let staticCredentials = StaticCredentials(accessKeyId: accessKeyId,
                                                  secretAccessKey: secretAccessKey,
                                                  sessionToken: nil)

        self.logger = reporting.logger
        self.dynamodb = _AWSDynamoDBClient(credentialsProvider: staticCredentials,
                                           awsRegion: region, reporting: reporting,
                                           endpointHostName: endpointHostName,
                                           endpointPort: endpointPort, requiresTLS: requiresTLS,
                                           connectionTimeoutSeconds: connectionTimeoutSeconds,
                                           retryConfiguration: retryConfiguration,
                                           eventLoopProvider: eventLoopProvider,
                                           reportingConfiguration: reportingConfiguration)
        self.targetTableName = tableName

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(region)' and hostname: '\(endpointHostName)'")
    }

    public init(credentialsProvider: CredentialsProvider,
                region: AWSRegion, reporting: InvocationReportingType,
                endpointHostName: String, endpointPort: Int = 443,
                requiresTLS: Bool? = nil, tableName: String,
                connectionTimeoutSeconds: Int64 = 10,
                retryConfiguration: HTTPClientRetryConfiguration = .default,
                eventLoopProvider: HTTPClient.EventLoopGroupProvider = .createNew,
                reportingConfiguration: SmokeAWSCore.SmokeAWSClientReportingConfiguration<DynamoDBModel.DynamoDBModelOperations>
                    = SmokeAWSClientReportingConfiguration<DynamoDBModelOperations>()) {
        self.logger = reporting.logger
        self.dynamodb = _AWSDynamoDBClient(credentialsProvider: credentialsProvider,
                                           awsRegion: region, reporting: reporting,
                                           endpointHostName: endpointHostName,
                                           endpointPort: endpointPort, requiresTLS: requiresTLS,
                                           connectionTimeoutSeconds: connectionTimeoutSeconds,
                                           retryConfiguration: retryConfiguration,
                                           eventLoopProvider: eventLoopProvider,
                                           reportingConfiguration: reportingConfiguration)
        self.targetTableName = tableName

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(region)' and hostname: '\(endpointHostName)'")
    }
    
    public init(config: AWSGenericDynamoDBClientConfiguration<InvocationReportingType>,
                reporting: InvocationReportingType,
                tableName: String,
                httpClient: HTTPOperationsClient? = nil) {
        self.logger = reporting.logger
        self.dynamodb = config.createAWSClient(reporting: reporting,
                                               httpClientOverride: httpClient)
        self.targetTableName = tableName
        
        let endpointHostName = self.dynamodb.httpClient.endpointHostName
        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(config.awsRegion)' and hostname: '\(endpointHostName)'")
    }
    
    // This initialiser is generic with respect to the reporting type from the operations client
    // As we are using the providing reporting instance and not creating a reporting instance from
    // the operations client, this generic type can be ignored.
    public init<OperationsClientInvocationReportingType: HTTPClientCoreInvocationReporting>(
                operationsClient: AWSGenericDynamoDBTableOperationsClient<OperationsClientInvocationReportingType>,
                reporting: InvocationReportingType) {
        self.logger = reporting.logger
        self.dynamodb = operationsClient.config.createAWSClient(reporting: reporting,
                                                                httpClientOverride: operationsClient.httpClient)
        self.targetTableName = operationsClient.tableName
        
        let endpointHostName = self.dynamodb.httpClient.endpointHostName
        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(operationsClient.config.awsRegion)' and hostname: '\(endpointHostName)'")
    }
    
    public init<TraceContextType: InvocationTraceContext>(
                config: AWSGenericDynamoDBClientConfiguration<StandardHTTPClientCoreInvocationReporting<TraceContextType>>,
                tableName: String,
                logger: Logging.Logger = Logger(label: "AWSDynamoDBCompositePrimaryKeysProjection"),
                internalRequestId: String = "none",
                eventLoop: EventLoop? = nil,
                httpClient: HTTPOperationsClient? = nil,
                outwardsRequestAggregator: OutwardsRequestAggregator? = nil)
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<TraceContextType> {
        self.logger = logger
        self.dynamodb = config.createAWSClient(logger: logger, internalRequestId: internalRequestId,
                                               eventLoopOverride: eventLoop, httpClientOverride: httpClient,
                                               outwardsRequestAggregator: outwardsRequestAggregator)
        self.targetTableName = tableName
        
        let endpointHostName = self.dynamodb.httpClient.endpointHostName
        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(config.awsRegion)' and hostname: '\(endpointHostName)'")
    }
    
    public convenience init<TraceContextType: InvocationTraceContext, InvocationAttributesType: HTTPClientInvocationAttributes>(
        config: AWSGenericDynamoDBClientConfiguration<StandardHTTPClientCoreInvocationReporting<TraceContextType>>,
        tableName: String,
        invocationAttributes: InvocationAttributesType,
        httpClient: HTTPOperationsClient? = nil)
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<TraceContextType> {
        self.init(config: config, tableName: tableName,
                  logger: invocationAttributes.logger,
                  internalRequestId: invocationAttributes.internalRequestId,
                  eventLoop: !config.ignoreInvocationEventLoop ? invocationAttributes.eventLoop : nil,
                  httpClient: httpClient,
                  outwardsRequestAggregator: invocationAttributes.outwardsRequestAggregator)
    }
    
    public init<TraceContextType: InvocationTraceContext>(
                operationsClient: AWSGenericDynamoDBTableOperationsClient<StandardHTTPClientCoreInvocationReporting<TraceContextType>>,
                logger: Logging.Logger = Logger(label: "AWSDynamoDBCompositePrimaryKeysProjection"),
                internalRequestId: String = "none",
                eventLoop: EventLoop? = nil,
                outwardsRequestAggregator: OutwardsRequestAggregator? = nil)
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<TraceContextType> {
        self.logger = logger
        self.dynamodb = operationsClient.config.createAWSClient(logger: logger, internalRequestId: internalRequestId,
                                                                eventLoopOverride: eventLoop, httpClientOverride: operationsClient.httpClient,
                                                                outwardsRequestAggregator: outwardsRequestAggregator)
        self.targetTableName = operationsClient.tableName
        
        let endpointHostName = self.dynamodb.httpClient.endpointHostName
        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(operationsClient.config.awsRegion)' and hostname: '\(endpointHostName)'")
    }
    
    public convenience init<TraceContextType: InvocationTraceContext, InvocationAttributesType: HTTPClientInvocationAttributes>(
        operationsClient: AWSGenericDynamoDBTableOperationsClient<StandardHTTPClientCoreInvocationReporting<TraceContextType>>,
        invocationAttributes: InvocationAttributesType)
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<TraceContextType> {
        self.init(operationsClient: operationsClient,
                  logger: invocationAttributes.logger,
                  internalRequestId: invocationAttributes.internalRequestId,
                  eventLoop: !operationsClient.config.ignoreInvocationEventLoop ? invocationAttributes.eventLoop : nil,
                  outwardsRequestAggregator: invocationAttributes.outwardsRequestAggregator)
    }
    
    public init(tableName: String,
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
                connectionPoolConfiguration: HTTPClient.Configuration.ConnectionPool? = nil,
                logger: Logging.Logger = Logger(label: "AWSDynamoDBTable"),
                internalRequestId: String = "none",
                outwardsRequestAggregator: OutwardsRequestAggregator? = nil)
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<AWSClientInvocationTraceContext> {
        let config = AWSGenericDynamoDBClientConfiguration(
            credentialsProvider: credentialsProvider,
            awsRegion: awsRegion,
            endpointHostName: endpointHostNameOptional,
            endpointPort: endpointPort,
            requiresTLS: requiresTLS,
            service: service,
            contentType: contentType,
            target: target,
            timeoutConfiguration: timeoutConfiguration,
            retryConfiguration: retryConfiguration,
            eventLoopProvider: eventLoopProvider,
            reportingConfiguration: reportingConfiguration,
            connectionPoolConfiguration: connectionPoolConfiguration)
        self.logger = logger
        self.dynamodb = config.createAWSClient(logger: logger, internalRequestId: internalRequestId,
                                               eventLoopOverride: nil, httpClientOverride: nil,
                                               outwardsRequestAggregator: outwardsRequestAggregator)
        self.targetTableName = tableName
        
        let endpointHostName = self.dynamodb.httpClient.endpointHostName
        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(awsRegion)' and hostname: '\(endpointHostName)'")
    }
    
    internal init(dynamodb: _AWSDynamoDBClient<InvocationReportingType>,
                  targetTableName: String,
                  logger: Logger) {
        self.dynamodb = dynamodb
        self.targetTableName = targetTableName
        self.logger = logger
    }

    /**
     Gracefully shuts down the client behind this table. This function is idempotent and
     will handle being called multiple times. Will block until shutdown is complete.
     */
    public func syncShutdown() throws {
        try self.dynamodb.syncShutdown()
    }

    // renamed `syncShutdown` to make it clearer this version of shutdown will block.
    @available(*, deprecated, renamed: "syncShutdown")
    public func close() throws {
        try self.dynamodb.close()
    }

    /**
     Gracefully shuts down the client behind this table. This function is idempotent and
     will handle being called multiple times. Will return when shutdown is complete.
     */
    #if (os(Linux) && compiler(>=5.5)) || (!os(Linux) && compiler(>=5.5.2)) && canImport(_Concurrency)
    public func shutdown() async throws {
        try await self.dynamodb.shutdown()
    }
    #endif
}

extension AWSDynamoDBCompositePrimaryKeysProjection {
    public var eventLoop: EventLoop {
        return self.dynamodb.reporting.eventLoop ?? self.dynamodb.eventLoopGroup.next()
    }
}
