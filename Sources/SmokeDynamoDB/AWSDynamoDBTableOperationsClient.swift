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

import DynamoDBClient
import DynamoDBModel
import AWSCore
import SmokeHTTPClient
import AWSHttp
import ClientRuntime

public typealias AWSDynamoDBTableOperationsClient =
    AWSGenericDynamoDBTableOperationsClient<StandardHTTPClientCoreInvocationReporting<AWSClientInvocationTraceContext>>

public struct AWSGenericDynamoDBTableOperationsClient<InvocationReportingType: HTTPClientCoreInvocationReporting> {
    public let wrappedOperationsClient: GenericAWSDynamoDBOperationsClient<InvocationReportingType>
    public let tableName: String
    public let consistentRead: Bool
    public let escapeSingleQuoteInPartiQL: Bool
    
    public init<TraceContextType: InvocationTraceContext>(credentialsProvider: CredentialsProvider, awsRegion: AWSRegion,
                                                          tableName: String,
                                                          consistentRead: Bool = true,
                                                          escapeSingleQuoteInPartiQL: Bool = false,
                                                          endpointHostName: String,
                                                          endpointPort: Int = 443,
                                                          requiresTLS: Bool? = nil,
                                                          service: String = "dynamodb",
                                                          contentType: String = "application/x-amz-json-1.0",
                                                          target: String? = "DynamoDB_20120810",
                                                          traceContext: TraceContextType,
                                                          runtimeConfig: ClientRuntime.SDKRuntimeConfiguration,
                                                          retryConfiguration: HTTPClientRetryConfiguration = .default,
                                                          reportingConfiguration: HTTPClientReportingConfiguration<DynamoDBModelOperations>
                                                            = HTTPClientReportingConfiguration<DynamoDBModelOperations>() )
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<TraceContextType> {
        self.wrappedOperationsClient = GenericAWSDynamoDBOperationsClient(
            credentialsProvider: credentialsProvider,
            awsRegion: awsRegion,
            endpointHostName: endpointHostName,
            endpointPort: endpointPort,
            requiresTLS: requiresTLS,
            service: service,
            contentType: contentType,
            target: target,
            traceContext: traceContext,
            runtimeConfig: runtimeConfig,
            retryConfiguration: retryConfiguration,
            reportingConfiguration: reportingConfiguration)
        self.tableName = tableName
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
    }
    
    public init(credentialsProvider: CredentialsProvider, awsRegion: AWSRegion,
                tableName: String,
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false,
                endpointHostName: String,
                endpointPort: Int = 443,
                requiresTLS: Bool? = nil,
                service: String = "dynamodb",
                contentType: String = "application/x-amz-json-1.0",
                target: String? = "DynamoDB_20120810",
                runtimeConfig: ClientRuntime.SDKRuntimeConfiguration,
                retryConfiguration: HTTPClientRetryConfiguration = .default,
                reportingConfiguration: HTTPClientReportingConfiguration<DynamoDBModelOperations>
                    = HTTPClientReportingConfiguration<DynamoDBModelOperations>() )
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<AWSClientInvocationTraceContext> {
        self.wrappedOperationsClient = GenericAWSDynamoDBOperationsClient(
            credentialsProvider: credentialsProvider,
            awsRegion: awsRegion,
            endpointHostName: endpointHostName,
            endpointPort: endpointPort,
            requiresTLS: requiresTLS,
            service: service,
            contentType: contentType,
            target: target,
            traceContext: AWSClientInvocationTraceContext(),
            runtimeConfig: runtimeConfig,
            retryConfiguration: retryConfiguration,
            reportingConfiguration: reportingConfiguration)
        self.tableName = tableName
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
    }
    
    public init(wrappedOperationsClient: GenericAWSDynamoDBOperationsClient<InvocationReportingType>,
                tableName: String,
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false) {
        self.wrappedOperationsClient = wrappedOperationsClient
        self.tableName = tableName
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
    }
}
