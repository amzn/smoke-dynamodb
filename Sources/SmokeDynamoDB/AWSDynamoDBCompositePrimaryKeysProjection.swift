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

public struct AWSDynamoDBCompositePrimaryKeysProjection<InvocationReportingType: HTTPClientCoreInvocationReporting>: DynamoDBCompositePrimaryKeysProjection {
    internal let dynamodb: GenericAWSDynamoDBClient<InvocationReportingType>
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
                runtimeConfig: ClientRuntime.SDKRuntimeConfiguration,
                retryConfiguration: HTTPClientRetryConfiguration = .default,
                reportingConfiguration: HTTPClientReportingConfiguration<DynamoDBModel.DynamoDBModelOperations>
                    = HTTPClientReportingConfiguration<DynamoDBModelOperations>()) {
        let staticCredentials = StaticCredentials(accessKeyId: accessKeyId,
                                                  secretAccessKey: secretAccessKey,
                                                  sessionToken: nil)

        self.logger = reporting.logger
        self.dynamodb = GenericAWSDynamoDBClient(credentialsProvider: staticCredentials,
                                                 awsRegion: region, reporting: reporting,
                                                 endpointHostName: endpointHostName,
                                                 endpointPort: endpointPort, requiresTLS: requiresTLS,
                                                 runtimeConfig: runtimeConfig,
                                                 reportingConfiguration: reportingConfiguration)
        self.targetTableName = tableName

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(region)' and hostname: '\(endpointHostName)'")
    }
    
    public init(credentialsProvider: CredentialsProvider, awsRegion: AWSRegion,
                reporting: InvocationReportingType,
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
                    = HTTPClientReportingConfiguration<DynamoDBModelOperations>() ) {
        self.logger = reporting.logger
        self.dynamodb = GenericAWSDynamoDBClient(credentialsProvider: credentialsProvider,
                                                 awsRegion: awsRegion, reporting: reporting,
                                                 endpointHostName: endpointHostName,
                                                 endpointPort: endpointPort, requiresTLS: requiresTLS,
                                                 runtimeConfig: runtimeConfig,
                                                 retryConfiguration: retryConfiguration,
                                                 reportingConfiguration: reportingConfiguration)
        self.targetTableName = tableName

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(awsRegion)' and hostname: '\(endpointHostName)'")
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
                    = HTTPClientReportingConfiguration<DynamoDBModelOperations>() )
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<AWSClientInvocationTraceContext> {
        self.logger = logger
        let reporting = StandardHTTPClientCoreInvocationReporting(logger: logger, internalRequestId: internalRequestId,
                                                                  traceContext: AWSClientInvocationTraceContext())
        self.dynamodb = GenericAWSDynamoDBClient(credentialsProvider: credentialsProvider,
                                                 awsRegion: awsRegion, reporting: reporting,
                                                 endpointHostName: endpointHostName,
                                                 endpointPort: endpointPort, requiresTLS: requiresTLS,
                                                 runtimeConfig: runtimeConfig,
                                                 retryConfiguration: retryConfiguration,
                                                 reportingConfiguration: reportingConfiguration)
        self.targetTableName = tableName

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(awsRegion)' and hostname: '\(endpointHostName)'")
    }
    
    public init<TraceContextType: InvocationTraceContext, InvocationAttributesType: HTTPClientInvocationAttributes>(
        config: GenericAWSDynamoDBClientConfiguration<StandardHTTPClientCoreInvocationReporting<TraceContextType>>,
        invocationAttributes: InvocationAttributesType,
        tableName: String,
        httpClient: HTTPOperationsClient? = nil)
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<TraceContextType> {
        self.logger = invocationAttributes.logger
        self.dynamodb = GenericAWSDynamoDBClient(config: config,
                                                 invocationAttributes: invocationAttributes,
                                                 httpClient: httpClient)
        self.targetTableName = tableName

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(config.awsRegion)' and hostname: '\(config.endpointHostName)'")
    }
    
    public init<TraceContextType: InvocationTraceContext>(
        config: GenericAWSDynamoDBClientConfiguration<StandardHTTPClientCoreInvocationReporting<TraceContextType>>,
        logger: Logging.Logger = Logger(label: "DynamoDBClient"),
        internalRequestId: String = "none",
        tableName: String,
        httpClient: HTTPOperationsClient? = nil,
        outwardsRequestAggregator: OutwardsRequestAggregator? = nil)
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<TraceContextType> {
        self.logger = logger
        self.dynamodb = GenericAWSDynamoDBClient(config: config,
                                                 logger: logger,
                                                 internalRequestId: internalRequestId,
                                                 httpClient: httpClient,
                                                 outwardsRequestAggregator: outwardsRequestAggregator)
        self.targetTableName = tableName

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(config.awsRegion)' and hostname: '\(config.endpointHostName)'")
    }
    
    public init<TraceContextType: InvocationTraceContext, InvocationAttributesType: HTTPClientInvocationAttributes>(
        operationsClient tableOperationsClient: AWSGenericDynamoDBTableOperationsClient<StandardHTTPClientCoreInvocationReporting<TraceContextType>>,
        invocationAttributes: InvocationAttributesType)
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<TraceContextType> {
        self.logger = invocationAttributes.logger
        self.dynamodb = GenericAWSDynamoDBClient(operationsClient: tableOperationsClient.wrappedOperationsClient,
                                                 invocationAttributes: invocationAttributes)
        self.targetTableName = tableOperationsClient.tableName

        let config = tableOperationsClient.wrappedOperationsClient.config
        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(config.awsRegion)' and hostname: '\(config.endpointHostName)'")
    }
    
    public init<TraceContextType: InvocationTraceContext>(
        operationsClient tableOperationsClient: AWSGenericDynamoDBTableOperationsClient<StandardHTTPClientCoreInvocationReporting<TraceContextType>>,
        logger: Logging.Logger = Logger(label: "DynamoDBClient"),
        internalRequestId: String = "none",
        outwardsRequestAggregator: OutwardsRequestAggregator? = nil)
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<TraceContextType> {
        self.logger = logger
        self.dynamodb = GenericAWSDynamoDBClient(operationsClient: tableOperationsClient.wrappedOperationsClient,
                                                 logger: logger,
                                                 internalRequestId: internalRequestId,
                                                 outwardsRequestAggregator: outwardsRequestAggregator)
        self.targetTableName = tableOperationsClient.tableName

        let config = tableOperationsClient.wrappedOperationsClient.config
        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(config.awsRegion)' and hostname: '\(config.endpointHostName)'")
    }
}
