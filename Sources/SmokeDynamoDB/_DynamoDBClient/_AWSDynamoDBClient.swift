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
// swiftlint:disable superfluous_disable_command
// swiftlint:disable file_length line_length identifier_name type_name vertical_parameter_alignment
// swiftlint:disable type_body_length function_body_length generic_type_name cyclomatic_complexity
// -- Generated Code; do not edit --
//
// _AWSDynamoDBClient.swift
// DynamoDBClient
//

import Foundation
import DynamoDBModel
import DynamoDBClient
import SmokeAWSCore
import SmokeHTTPClient
import NIO
import SmokeAWSHttp
import NIO
import NIOHTTP1
import AsyncHTTPClient
import Logging

private extension SmokeHTTPClient.HTTPClientError {
    func isRetriable() -> Bool {
        if let typedError = self.cause as? DynamoDBError, let isRetriable = typedError.isRetriable() {
            return isRetriable
        } else {
            return self.isRetriableAccordingToCategory
        }
    }
}

/**
 AWS Client for the DynamoDB service.
 */
struct _AWSDynamoDBClient<InvocationReportingType: HTTPClientCoreInvocationReporting>: _DynamoDBClientProtocol, AWSClientProtocol {
    let httpClient: HTTPOperationsClient
    let ownsHttpClients: Bool
    public let awsRegion: AWSRegion
    public let service: String
    public let target: String?
    public let retryConfiguration: HTTPClientRetryConfiguration
    public let retryOnErrorProvider: (SmokeHTTPClient.HTTPClientError) -> Bool
    public let credentialsProvider: CredentialsProvider
    
    public let eventLoopGroup: EventLoopGroup
    public let reporting: InvocationReportingType

    let operationsReporting: DynamoDBOperationsReporting
    let invocationsReporting: DynamoDBInvocationsReporting<InvocationReportingType>
    
    public init(credentialsProvider: CredentialsProvider, awsRegion: AWSRegion,
                reporting: InvocationReportingType,
                endpointHostName: String,
                endpointPort: Int = 443,
                requiresTLS: Bool? = nil,
                service: String = "dynamodb",
                contentType: String = "application/x-amz-json-1.0",
                target: String? = "DynamoDB_20120810",
                connectionTimeoutSeconds: Int64 = 10,
                retryConfiguration: HTTPClientRetryConfiguration = .default,
                eventLoopProvider: HTTPClient.EventLoopGroupProvider = .createNew,
                reportingConfiguration: SmokeAWSClientReportingConfiguration<DynamoDBModelOperations>
                    = SmokeAWSClientReportingConfiguration<DynamoDBModelOperations>() ) {
        self.eventLoopGroup = AWSClientHelper.getEventLoop(eventLoopGroupProvider: eventLoopProvider)
        let useTLS = requiresTLS ?? AWSHTTPClientDelegate.requiresTLS(forEndpointPort: endpointPort)
        let clientDelegate = JSONAWSHttpClientDelegate<DynamoDBError>(requiresTLS: useTLS)

        self.httpClient = HTTPOperationsClient(
            endpointHostName: endpointHostName,
            endpointPort: endpointPort,
            contentType: contentType,
            clientDelegate: clientDelegate,
            connectionTimeoutSeconds: connectionTimeoutSeconds,
            eventLoopProvider: .shared(self.eventLoopGroup))
        self.ownsHttpClients = true
        self.awsRegion = awsRegion
        self.service = service
        self.target = target
        self.credentialsProvider = credentialsProvider
        self.retryConfiguration = retryConfiguration
        self.reporting = reporting
        self.retryOnErrorProvider = { error in error.isRetriable() }
        self.operationsReporting = DynamoDBOperationsReporting(clientName: "AWSDynamoDBClient", reportingConfiguration: reportingConfiguration)
        self.invocationsReporting = DynamoDBInvocationsReporting(reporting: reporting, operationsReporting: self.operationsReporting)
    }
    
    internal init(credentialsProvider: CredentialsProvider, awsRegion: AWSRegion,
                reporting: InvocationReportingType,
                httpClient: HTTPOperationsClient,
                service: String,
                target: String?,
                eventLoopGroup: EventLoopGroup,
                retryOnErrorProvider: @escaping (SmokeHTTPClient.HTTPClientError) -> Bool,
                retryConfiguration: HTTPClientRetryConfiguration,
                operationsReporting: DynamoDBOperationsReporting) {
        self.eventLoopGroup = eventLoopGroup
        self.httpClient = httpClient
        self.ownsHttpClients = false
        self.awsRegion = awsRegion
        self.service = service
        self.target = target
        self.credentialsProvider = credentialsProvider
        self.retryConfiguration = retryConfiguration
        self.reporting = reporting
        self.retryOnErrorProvider = retryOnErrorProvider
        self.operationsReporting = operationsReporting
        self.invocationsReporting = DynamoDBInvocationsReporting(reporting: reporting, operationsReporting: self.operationsReporting)
    }

    /**
     Gracefully shuts down this client. This function is idempotent and
     will handle being called multiple times.
     */
    public func close() throws {
        if self.ownsHttpClients {
            try httpClient.close()
        }
    }

    /**
     Invokes the BatchExecuteStatement operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated BatchExecuteStatementInput object being passed to this operation.
     - Returns: A future to the BatchExecuteStatementOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, requestLimitExceeded.
     */
    public func batchExecuteStatement(
            input: DynamoDBModel.BatchExecuteStatementInput) -> EventLoopFuture<DynamoDBModel.BatchExecuteStatementOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: BatchExecuteStatementOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.batchExecuteStatement.rawValue,
            reporting: self.invocationsReporting.batchExecuteStatement,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the BatchGetItem operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated BatchGetItemInput object being passed to this operation.
     - Returns: A future to the BatchGetItemOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    public func batchGetItem(
            input: DynamoDBModel.BatchGetItemInput) -> EventLoopFuture<DynamoDBModel.BatchGetItemOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: BatchGetItemOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.batchGetItem.rawValue,
            reporting: self.invocationsReporting.batchGetItem,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the BatchWriteItem operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated BatchWriteItemInput object being passed to this operation.
     - Returns: A future to the BatchWriteItemOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    public func batchWriteItem(
            input: DynamoDBModel.BatchWriteItemInput) -> EventLoopFuture<DynamoDBModel.BatchWriteItemOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: BatchWriteItemOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.batchWriteItem.rawValue,
            reporting: self.invocationsReporting.batchWriteItem,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the CreateBackup operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated CreateBackupInput object being passed to this operation.
     - Returns: A future to the CreateBackupOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: backupInUse, continuousBackupsUnavailable, internalServer, limitExceeded, tableInUse, tableNotFound.
     */
    public func createBackup(
            input: DynamoDBModel.CreateBackupInput) -> EventLoopFuture<DynamoDBModel.CreateBackupOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: CreateBackupOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.createBackup.rawValue,
            reporting: self.invocationsReporting.createBackup,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the CreateGlobalTable operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated CreateGlobalTableInput object being passed to this operation.
     - Returns: A future to the CreateGlobalTableOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableAlreadyExists, internalServer, limitExceeded, tableNotFound.
     */
    public func createGlobalTable(
            input: DynamoDBModel.CreateGlobalTableInput) -> EventLoopFuture<DynamoDBModel.CreateGlobalTableOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: CreateGlobalTableOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.createGlobalTable.rawValue,
            reporting: self.invocationsReporting.createGlobalTable,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the CreateTable operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated CreateTableInput object being passed to this operation.
     - Returns: A future to the CreateTableOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse.
     */
    public func createTable(
            input: DynamoDBModel.CreateTableInput) -> EventLoopFuture<DynamoDBModel.CreateTableOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: CreateTableOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.createTable.rawValue,
            reporting: self.invocationsReporting.createTable,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DeleteBackup operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DeleteBackupInput object being passed to this operation.
     - Returns: A future to the DeleteBackupOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: backupInUse, backupNotFound, internalServer, limitExceeded.
     */
    public func deleteBackup(
            input: DynamoDBModel.DeleteBackupInput) -> EventLoopFuture<DynamoDBModel.DeleteBackupOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: DeleteBackupOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.deleteBackup.rawValue,
            reporting: self.invocationsReporting.deleteBackup,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DeleteItem operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DeleteItemInput object being passed to this operation.
     - Returns: A future to the DeleteItemOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: conditionalCheckFailed, internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionConflict.
     */
    public func deleteItem(
            input: DynamoDBModel.DeleteItemInput) -> EventLoopFuture<DynamoDBModel.DeleteItemOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: DeleteItemOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.deleteItem.rawValue,
            reporting: self.invocationsReporting.deleteItem,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DeleteTable operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DeleteTableInput object being passed to this operation.
     - Returns: A future to the DeleteTableOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    public func deleteTable(
            input: DynamoDBModel.DeleteTableInput) -> EventLoopFuture<DynamoDBModel.DeleteTableOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: DeleteTableOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.deleteTable.rawValue,
            reporting: self.invocationsReporting.deleteTable,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeBackup operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeBackupInput object being passed to this operation.
     - Returns: A future to the DescribeBackupOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: backupNotFound, internalServer.
     */
    public func describeBackup(
            input: DynamoDBModel.DescribeBackupInput) -> EventLoopFuture<DynamoDBModel.DescribeBackupOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeBackupOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeBackup.rawValue,
            reporting: self.invocationsReporting.describeBackup,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeContinuousBackups operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeContinuousBackupsInput object being passed to this operation.
     - Returns: A future to the DescribeContinuousBackupsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, tableNotFound.
     */
    public func describeContinuousBackups(
            input: DynamoDBModel.DescribeContinuousBackupsInput) -> EventLoopFuture<DynamoDBModel.DescribeContinuousBackupsOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeContinuousBackupsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeContinuousBackups.rawValue,
            reporting: self.invocationsReporting.describeContinuousBackups,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeContributorInsights operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeContributorInsightsInput object being passed to this operation.
     - Returns: A future to the DescribeContributorInsightsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    public func describeContributorInsights(
            input: DynamoDBModel.DescribeContributorInsightsInput) -> EventLoopFuture<DynamoDBModel.DescribeContributorInsightsOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeContributorInsightsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeContributorInsights.rawValue,
            reporting: self.invocationsReporting.describeContributorInsights,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeEndpoints operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeEndpointsRequest object being passed to this operation.
     - Returns: A future to the DescribeEndpointsResponse object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
     */
    public func describeEndpoints(
            input: DynamoDBModel.DescribeEndpointsRequest) -> EventLoopFuture<DynamoDBModel.DescribeEndpointsResponse> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeEndpointsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeEndpoints.rawValue,
            reporting: self.invocationsReporting.describeEndpoints,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeExport operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeExportInput object being passed to this operation.
     - Returns: A future to the DescribeExportOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: exportNotFound, internalServer, limitExceeded.
     */
    public func describeExport(
            input: DynamoDBModel.DescribeExportInput) -> EventLoopFuture<DynamoDBModel.DescribeExportOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeExportOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeExport.rawValue,
            reporting: self.invocationsReporting.describeExport,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeGlobalTable operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeGlobalTableInput object being passed to this operation.
     - Returns: A future to the DescribeGlobalTableOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableNotFound, internalServer.
     */
    public func describeGlobalTable(
            input: DynamoDBModel.DescribeGlobalTableInput) -> EventLoopFuture<DynamoDBModel.DescribeGlobalTableOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeGlobalTableOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeGlobalTable.rawValue,
            reporting: self.invocationsReporting.describeGlobalTable,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeGlobalTableSettings operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeGlobalTableSettingsInput object being passed to this operation.
     - Returns: A future to the DescribeGlobalTableSettingsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableNotFound, internalServer.
     */
    public func describeGlobalTableSettings(
            input: DynamoDBModel.DescribeGlobalTableSettingsInput) -> EventLoopFuture<DynamoDBModel.DescribeGlobalTableSettingsOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeGlobalTableSettingsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeGlobalTableSettings.rawValue,
            reporting: self.invocationsReporting.describeGlobalTableSettings,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeKinesisStreamingDestination operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeKinesisStreamingDestinationInput object being passed to this operation.
     - Returns: A future to the DescribeKinesisStreamingDestinationOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    public func describeKinesisStreamingDestination(
            input: DynamoDBModel.DescribeKinesisStreamingDestinationInput) -> EventLoopFuture<DynamoDBModel.DescribeKinesisStreamingDestinationOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeKinesisStreamingDestinationOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeKinesisStreamingDestination.rawValue,
            reporting: self.invocationsReporting.describeKinesisStreamingDestination,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeLimits operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeLimitsInput object being passed to this operation.
     - Returns: A future to the DescribeLimitsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer.
     */
    public func describeLimits(
            input: DynamoDBModel.DescribeLimitsInput) -> EventLoopFuture<DynamoDBModel.DescribeLimitsOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeLimitsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeLimits.rawValue,
            reporting: self.invocationsReporting.describeLimits,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeTable operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeTableInput object being passed to this operation.
     - Returns: A future to the DescribeTableOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    public func describeTable(
            input: DynamoDBModel.DescribeTableInput) -> EventLoopFuture<DynamoDBModel.DescribeTableOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeTableOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeTable.rawValue,
            reporting: self.invocationsReporting.describeTable,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeTableReplicaAutoScaling operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeTableReplicaAutoScalingInput object being passed to this operation.
     - Returns: A future to the DescribeTableReplicaAutoScalingOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    public func describeTableReplicaAutoScaling(
            input: DynamoDBModel.DescribeTableReplicaAutoScalingInput) -> EventLoopFuture<DynamoDBModel.DescribeTableReplicaAutoScalingOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeTableReplicaAutoScalingOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeTableReplicaAutoScaling.rawValue,
            reporting: self.invocationsReporting.describeTableReplicaAutoScaling,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeTimeToLive operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeTimeToLiveInput object being passed to this operation.
     - Returns: A future to the DescribeTimeToLiveOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    public func describeTimeToLive(
            input: DynamoDBModel.DescribeTimeToLiveInput) -> EventLoopFuture<DynamoDBModel.DescribeTimeToLiveOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeTimeToLiveOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeTimeToLive.rawValue,
            reporting: self.invocationsReporting.describeTimeToLive,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DisableKinesisStreamingDestination operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated KinesisStreamingDestinationInput object being passed to this operation.
     - Returns: A future to the KinesisStreamingDestinationOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    public func disableKinesisStreamingDestination(
            input: DynamoDBModel.KinesisStreamingDestinationInput) -> EventLoopFuture<DynamoDBModel.KinesisStreamingDestinationOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: DisableKinesisStreamingDestinationOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.disableKinesisStreamingDestination.rawValue,
            reporting: self.invocationsReporting.disableKinesisStreamingDestination,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the EnableKinesisStreamingDestination operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated KinesisStreamingDestinationInput object being passed to this operation.
     - Returns: A future to the KinesisStreamingDestinationOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    public func enableKinesisStreamingDestination(
            input: DynamoDBModel.KinesisStreamingDestinationInput) -> EventLoopFuture<DynamoDBModel.KinesisStreamingDestinationOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: EnableKinesisStreamingDestinationOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.enableKinesisStreamingDestination.rawValue,
            reporting: self.invocationsReporting.enableKinesisStreamingDestination,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ExecuteStatement operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ExecuteStatementInput object being passed to this operation.
     - Returns: A future to the ExecuteStatementOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: conditionalCheckFailed, duplicateItem, internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionConflict.
     */
    public func executeStatement(
            input: DynamoDBModel.ExecuteStatementInput) -> EventLoopFuture<DynamoDBModel.ExecuteStatementOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: ExecuteStatementOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.executeStatement.rawValue,
            reporting: self.invocationsReporting.executeStatement,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ExecuteTransaction operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ExecuteTransactionInput object being passed to this operation.
     - Returns: A future to the ExecuteTransactionOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: idempotentParameterMismatch, internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionCanceled, transactionInProgress.
     */
    public func executeTransaction(
            input: DynamoDBModel.ExecuteTransactionInput) -> EventLoopFuture<DynamoDBModel.ExecuteTransactionOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: ExecuteTransactionOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.executeTransaction.rawValue,
            reporting: self.invocationsReporting.executeTransaction,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ExportTableToPointInTime operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ExportTableToPointInTimeInput object being passed to this operation.
     - Returns: A future to the ExportTableToPointInTimeOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: exportConflict, internalServer, invalidExportTime, limitExceeded, pointInTimeRecoveryUnavailable, tableNotFound.
     */
    public func exportTableToPointInTime(
            input: DynamoDBModel.ExportTableToPointInTimeInput) -> EventLoopFuture<DynamoDBModel.ExportTableToPointInTimeOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: ExportTableToPointInTimeOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.exportTableToPointInTime.rawValue,
            reporting: self.invocationsReporting.exportTableToPointInTime,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the GetItem operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated GetItemInput object being passed to this operation.
     - Returns: A future to the GetItemOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    public func getItem(
            input: DynamoDBModel.GetItemInput) -> EventLoopFuture<DynamoDBModel.GetItemOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: GetItemOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.getItem.rawValue,
            reporting: self.invocationsReporting.getItem,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ListBackups operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ListBackupsInput object being passed to this operation.
     - Returns: A future to the ListBackupsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer.
     */
    public func listBackups(
            input: DynamoDBModel.ListBackupsInput) -> EventLoopFuture<DynamoDBModel.ListBackupsOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: ListBackupsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.listBackups.rawValue,
            reporting: self.invocationsReporting.listBackups,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ListContributorInsights operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ListContributorInsightsInput object being passed to this operation.
     - Returns: A future to the ListContributorInsightsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    public func listContributorInsights(
            input: DynamoDBModel.ListContributorInsightsInput) -> EventLoopFuture<DynamoDBModel.ListContributorInsightsOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: ListContributorInsightsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.listContributorInsights.rawValue,
            reporting: self.invocationsReporting.listContributorInsights,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ListExports operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ListExportsInput object being passed to this operation.
     - Returns: A future to the ListExportsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded.
     */
    public func listExports(
            input: DynamoDBModel.ListExportsInput) -> EventLoopFuture<DynamoDBModel.ListExportsOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: ListExportsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.listExports.rawValue,
            reporting: self.invocationsReporting.listExports,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ListGlobalTables operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ListGlobalTablesInput object being passed to this operation.
     - Returns: A future to the ListGlobalTablesOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer.
     */
    public func listGlobalTables(
            input: DynamoDBModel.ListGlobalTablesInput) -> EventLoopFuture<DynamoDBModel.ListGlobalTablesOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: ListGlobalTablesOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.listGlobalTables.rawValue,
            reporting: self.invocationsReporting.listGlobalTables,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ListTables operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ListTablesInput object being passed to this operation.
     - Returns: A future to the ListTablesOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer.
     */
    public func listTables(
            input: DynamoDBModel.ListTablesInput) -> EventLoopFuture<DynamoDBModel.ListTablesOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: ListTablesOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.listTables.rawValue,
            reporting: self.invocationsReporting.listTables,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ListTagsOfResource operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ListTagsOfResourceInput object being passed to this operation.
     - Returns: A future to the ListTagsOfResourceOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    public func listTagsOfResource(
            input: DynamoDBModel.ListTagsOfResourceInput) -> EventLoopFuture<DynamoDBModel.ListTagsOfResourceOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: ListTagsOfResourceOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.listTagsOfResource.rawValue,
            reporting: self.invocationsReporting.listTagsOfResource,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the PutItem operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated PutItemInput object being passed to this operation.
     - Returns: A future to the PutItemOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: conditionalCheckFailed, internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionConflict.
     */
    public func putItem(
            input: DynamoDBModel.PutItemInput) -> EventLoopFuture<DynamoDBModel.PutItemOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: PutItemOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.putItem.rawValue,
            reporting: self.invocationsReporting.putItem,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the Query operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated QueryInput object being passed to this operation.
     - Returns: A future to the QueryOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    public func query(
            input: DynamoDBModel.QueryInput) -> EventLoopFuture<DynamoDBModel.QueryOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: QueryOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.query.rawValue,
            reporting: self.invocationsReporting.query,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the RestoreTableFromBackup operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated RestoreTableFromBackupInput object being passed to this operation.
     - Returns: A future to the RestoreTableFromBackupOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: backupInUse, backupNotFound, internalServer, limitExceeded, tableAlreadyExists, tableInUse.
     */
    public func restoreTableFromBackup(
            input: DynamoDBModel.RestoreTableFromBackupInput) -> EventLoopFuture<DynamoDBModel.RestoreTableFromBackupOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: RestoreTableFromBackupOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.restoreTableFromBackup.rawValue,
            reporting: self.invocationsReporting.restoreTableFromBackup,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the RestoreTableToPointInTime operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated RestoreTableToPointInTimeInput object being passed to this operation.
     - Returns: A future to the RestoreTableToPointInTimeOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, invalidRestoreTime, limitExceeded, pointInTimeRecoveryUnavailable, tableAlreadyExists, tableInUse, tableNotFound.
     */
    public func restoreTableToPointInTime(
            input: DynamoDBModel.RestoreTableToPointInTimeInput) -> EventLoopFuture<DynamoDBModel.RestoreTableToPointInTimeOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: RestoreTableToPointInTimeOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.restoreTableToPointInTime.rawValue,
            reporting: self.invocationsReporting.restoreTableToPointInTime,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the Scan operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ScanInput object being passed to this operation.
     - Returns: A future to the ScanOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    public func scan(
            input: DynamoDBModel.ScanInput) -> EventLoopFuture<DynamoDBModel.ScanOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: ScanOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.scan.rawValue,
            reporting: self.invocationsReporting.scan,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the TagResource operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated TagResourceInput object being passed to this operation.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    public func tagResource(
            input: DynamoDBModel.TagResourceInput) -> EventLoopFuture<Void> {
        return executeWithoutOutput(
            httpClient: httpClient,
            requestInput: TagResourceOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.tagResource.rawValue,
            reporting: self.invocationsReporting.tagResource,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the TransactGetItems operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated TransactGetItemsInput object being passed to this operation.
     - Returns: A future to the TransactGetItemsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionCanceled.
     */
    public func transactGetItems(
            input: DynamoDBModel.TransactGetItemsInput) -> EventLoopFuture<DynamoDBModel.TransactGetItemsOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: TransactGetItemsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.transactGetItems.rawValue,
            reporting: self.invocationsReporting.transactGetItems,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the TransactWriteItems operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated TransactWriteItemsInput object being passed to this operation.
     - Returns: A future to the TransactWriteItemsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: idempotentParameterMismatch, internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionCanceled, transactionInProgress.
     */
    public func transactWriteItems(
            input: DynamoDBModel.TransactWriteItemsInput) -> EventLoopFuture<DynamoDBModel.TransactWriteItemsOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: TransactWriteItemsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.transactWriteItems.rawValue,
            reporting: self.invocationsReporting.transactWriteItems,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UntagResource operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UntagResourceInput object being passed to this operation.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    public func untagResource(
            input: DynamoDBModel.UntagResourceInput) -> EventLoopFuture<Void> {
        return executeWithoutOutput(
            httpClient: httpClient,
            requestInput: UntagResourceOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.untagResource.rawValue,
            reporting: self.invocationsReporting.untagResource,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UpdateContinuousBackups operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UpdateContinuousBackupsInput object being passed to this operation.
     - Returns: A future to the UpdateContinuousBackupsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: continuousBackupsUnavailable, internalServer, tableNotFound.
     */
    public func updateContinuousBackups(
            input: DynamoDBModel.UpdateContinuousBackupsInput) -> EventLoopFuture<DynamoDBModel.UpdateContinuousBackupsOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: UpdateContinuousBackupsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.updateContinuousBackups.rawValue,
            reporting: self.invocationsReporting.updateContinuousBackups,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UpdateContributorInsights operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UpdateContributorInsightsInput object being passed to this operation.
     - Returns: A future to the UpdateContributorInsightsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    public func updateContributorInsights(
            input: DynamoDBModel.UpdateContributorInsightsInput) -> EventLoopFuture<DynamoDBModel.UpdateContributorInsightsOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: UpdateContributorInsightsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.updateContributorInsights.rawValue,
            reporting: self.invocationsReporting.updateContributorInsights,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UpdateGlobalTable operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UpdateGlobalTableInput object being passed to this operation.
     - Returns: A future to the UpdateGlobalTableOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableNotFound, internalServer, replicaAlreadyExists, replicaNotFound, tableNotFound.
     */
    public func updateGlobalTable(
            input: DynamoDBModel.UpdateGlobalTableInput) -> EventLoopFuture<DynamoDBModel.UpdateGlobalTableOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: UpdateGlobalTableOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.updateGlobalTable.rawValue,
            reporting: self.invocationsReporting.updateGlobalTable,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UpdateGlobalTableSettings operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UpdateGlobalTableSettingsInput object being passed to this operation.
     - Returns: A future to the UpdateGlobalTableSettingsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableNotFound, indexNotFound, internalServer, limitExceeded, replicaNotFound, resourceInUse.
     */
    public func updateGlobalTableSettings(
            input: DynamoDBModel.UpdateGlobalTableSettingsInput) -> EventLoopFuture<DynamoDBModel.UpdateGlobalTableSettingsOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: UpdateGlobalTableSettingsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.updateGlobalTableSettings.rawValue,
            reporting: self.invocationsReporting.updateGlobalTableSettings,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UpdateItem operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UpdateItemInput object being passed to this operation.
     - Returns: A future to the UpdateItemOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: conditionalCheckFailed, internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionConflict.
     */
    public func updateItem(
            input: DynamoDBModel.UpdateItemInput) -> EventLoopFuture<DynamoDBModel.UpdateItemOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: UpdateItemOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.updateItem.rawValue,
            reporting: self.invocationsReporting.updateItem,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UpdateTable operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UpdateTableInput object being passed to this operation.
     - Returns: A future to the UpdateTableOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    public func updateTable(
            input: DynamoDBModel.UpdateTableInput) -> EventLoopFuture<DynamoDBModel.UpdateTableOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: UpdateTableOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.updateTable.rawValue,
            reporting: self.invocationsReporting.updateTable,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UpdateTableReplicaAutoScaling operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UpdateTableReplicaAutoScalingInput object being passed to this operation.
     - Returns: A future to the UpdateTableReplicaAutoScalingOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    public func updateTableReplicaAutoScaling(
            input: DynamoDBModel.UpdateTableReplicaAutoScalingInput) -> EventLoopFuture<DynamoDBModel.UpdateTableReplicaAutoScalingOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: UpdateTableReplicaAutoScalingOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.updateTableReplicaAutoScaling.rawValue,
            reporting: self.invocationsReporting.updateTableReplicaAutoScaling,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UpdateTimeToLive operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UpdateTimeToLiveInput object being passed to this operation.
     - Returns: A future to the UpdateTimeToLiveOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    public func updateTimeToLive(
            input: DynamoDBModel.UpdateTimeToLiveInput) -> EventLoopFuture<DynamoDBModel.UpdateTimeToLiveOutput> {
        return executeWithOutput(
            httpClient: httpClient,
            requestInput: UpdateTimeToLiveOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.updateTimeToLive.rawValue,
            reporting: self.invocationsReporting.updateTimeToLive,
            errorType: DynamoDBError.self)
    }

    #if compiler(>=5.5) && canImport(_Concurrency)
    /**
     Invokes the BatchExecuteStatement operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated BatchExecuteStatementInput object being passed to this operation.
     - Returns: The BatchExecuteStatementOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, requestLimitExceeded.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func batchExecuteStatement(
            input: DynamoDBModel.BatchExecuteStatementInput) async throws -> DynamoDBModel.BatchExecuteStatementOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: BatchExecuteStatementOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.batchExecuteStatement.rawValue,
            reporting: self.invocationsReporting.batchExecuteStatement,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the BatchGetItem operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated BatchGetItemInput object being passed to this operation.
     - Returns: The BatchGetItemOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func batchGetItem(
            input: DynamoDBModel.BatchGetItemInput) async throws -> DynamoDBModel.BatchGetItemOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: BatchGetItemOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.batchGetItem.rawValue,
            reporting: self.invocationsReporting.batchGetItem,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the BatchWriteItem operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated BatchWriteItemInput object being passed to this operation.
     - Returns: The BatchWriteItemOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func batchWriteItem(
            input: DynamoDBModel.BatchWriteItemInput) async throws -> DynamoDBModel.BatchWriteItemOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: BatchWriteItemOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.batchWriteItem.rawValue,
            reporting: self.invocationsReporting.batchWriteItem,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the CreateBackup operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated CreateBackupInput object being passed to this operation.
     - Returns: The CreateBackupOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: backupInUse, continuousBackupsUnavailable, internalServer, limitExceeded, tableInUse, tableNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func createBackup(
            input: DynamoDBModel.CreateBackupInput) async throws -> DynamoDBModel.CreateBackupOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: CreateBackupOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.createBackup.rawValue,
            reporting: self.invocationsReporting.createBackup,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the CreateGlobalTable operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated CreateGlobalTableInput object being passed to this operation.
     - Returns: The CreateGlobalTableOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableAlreadyExists, internalServer, limitExceeded, tableNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func createGlobalTable(
            input: DynamoDBModel.CreateGlobalTableInput) async throws -> DynamoDBModel.CreateGlobalTableOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: CreateGlobalTableOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.createGlobalTable.rawValue,
            reporting: self.invocationsReporting.createGlobalTable,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the CreateTable operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated CreateTableInput object being passed to this operation.
     - Returns: The CreateTableOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func createTable(
            input: DynamoDBModel.CreateTableInput) async throws -> DynamoDBModel.CreateTableOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: CreateTableOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.createTable.rawValue,
            reporting: self.invocationsReporting.createTable,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DeleteBackup operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DeleteBackupInput object being passed to this operation.
     - Returns: The DeleteBackupOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: backupInUse, backupNotFound, internalServer, limitExceeded.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func deleteBackup(
            input: DynamoDBModel.DeleteBackupInput) async throws -> DynamoDBModel.DeleteBackupOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: DeleteBackupOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.deleteBackup.rawValue,
            reporting: self.invocationsReporting.deleteBackup,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DeleteItem operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DeleteItemInput object being passed to this operation.
     - Returns: The DeleteItemOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: conditionalCheckFailed, internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionConflict.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func deleteItem(
            input: DynamoDBModel.DeleteItemInput) async throws -> DynamoDBModel.DeleteItemOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: DeleteItemOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.deleteItem.rawValue,
            reporting: self.invocationsReporting.deleteItem,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DeleteTable operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DeleteTableInput object being passed to this operation.
     - Returns: The DeleteTableOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func deleteTable(
            input: DynamoDBModel.DeleteTableInput) async throws -> DynamoDBModel.DeleteTableOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: DeleteTableOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.deleteTable.rawValue,
            reporting: self.invocationsReporting.deleteTable,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeBackup operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeBackupInput object being passed to this operation.
     - Returns: The DescribeBackupOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: backupNotFound, internalServer.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func describeBackup(
            input: DynamoDBModel.DescribeBackupInput) async throws -> DynamoDBModel.DescribeBackupOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeBackupOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeBackup.rawValue,
            reporting: self.invocationsReporting.describeBackup,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeContinuousBackups operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeContinuousBackupsInput object being passed to this operation.
     - Returns: The DescribeContinuousBackupsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, tableNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func describeContinuousBackups(
            input: DynamoDBModel.DescribeContinuousBackupsInput) async throws -> DynamoDBModel.DescribeContinuousBackupsOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeContinuousBackupsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeContinuousBackups.rawValue,
            reporting: self.invocationsReporting.describeContinuousBackups,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeContributorInsights operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeContributorInsightsInput object being passed to this operation.
     - Returns: The DescribeContributorInsightsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func describeContributorInsights(
            input: DynamoDBModel.DescribeContributorInsightsInput) async throws -> DynamoDBModel.DescribeContributorInsightsOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeContributorInsightsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeContributorInsights.rawValue,
            reporting: self.invocationsReporting.describeContributorInsights,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeEndpoints operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeEndpointsRequest object being passed to this operation.
     - Returns: The DescribeEndpointsResponse object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func describeEndpoints(
            input: DynamoDBModel.DescribeEndpointsRequest) async throws -> DynamoDBModel.DescribeEndpointsResponse {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeEndpointsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeEndpoints.rawValue,
            reporting: self.invocationsReporting.describeEndpoints,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeExport operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeExportInput object being passed to this operation.
     - Returns: The DescribeExportOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: exportNotFound, internalServer, limitExceeded.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func describeExport(
            input: DynamoDBModel.DescribeExportInput) async throws -> DynamoDBModel.DescribeExportOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeExportOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeExport.rawValue,
            reporting: self.invocationsReporting.describeExport,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeGlobalTable operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeGlobalTableInput object being passed to this operation.
     - Returns: The DescribeGlobalTableOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableNotFound, internalServer.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func describeGlobalTable(
            input: DynamoDBModel.DescribeGlobalTableInput) async throws -> DynamoDBModel.DescribeGlobalTableOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeGlobalTableOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeGlobalTable.rawValue,
            reporting: self.invocationsReporting.describeGlobalTable,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeGlobalTableSettings operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeGlobalTableSettingsInput object being passed to this operation.
     - Returns: The DescribeGlobalTableSettingsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableNotFound, internalServer.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func describeGlobalTableSettings(
            input: DynamoDBModel.DescribeGlobalTableSettingsInput) async throws -> DynamoDBModel.DescribeGlobalTableSettingsOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeGlobalTableSettingsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeGlobalTableSettings.rawValue,
            reporting: self.invocationsReporting.describeGlobalTableSettings,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeKinesisStreamingDestination operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeKinesisStreamingDestinationInput object being passed to this operation.
     - Returns: The DescribeKinesisStreamingDestinationOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func describeKinesisStreamingDestination(
            input: DynamoDBModel.DescribeKinesisStreamingDestinationInput) async throws -> DynamoDBModel.DescribeKinesisStreamingDestinationOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeKinesisStreamingDestinationOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeKinesisStreamingDestination.rawValue,
            reporting: self.invocationsReporting.describeKinesisStreamingDestination,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeLimits operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeLimitsInput object being passed to this operation.
     - Returns: The DescribeLimitsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func describeLimits(
            input: DynamoDBModel.DescribeLimitsInput) async throws -> DynamoDBModel.DescribeLimitsOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeLimitsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeLimits.rawValue,
            reporting: self.invocationsReporting.describeLimits,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeTable operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeTableInput object being passed to this operation.
     - Returns: The DescribeTableOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func describeTable(
            input: DynamoDBModel.DescribeTableInput) async throws -> DynamoDBModel.DescribeTableOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeTableOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeTable.rawValue,
            reporting: self.invocationsReporting.describeTable,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeTableReplicaAutoScaling operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeTableReplicaAutoScalingInput object being passed to this operation.
     - Returns: The DescribeTableReplicaAutoScalingOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func describeTableReplicaAutoScaling(
            input: DynamoDBModel.DescribeTableReplicaAutoScalingInput) async throws -> DynamoDBModel.DescribeTableReplicaAutoScalingOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeTableReplicaAutoScalingOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeTableReplicaAutoScaling.rawValue,
            reporting: self.invocationsReporting.describeTableReplicaAutoScaling,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DescribeTimeToLive operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeTimeToLiveInput object being passed to this operation.
     - Returns: The DescribeTimeToLiveOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func describeTimeToLive(
            input: DynamoDBModel.DescribeTimeToLiveInput) async throws -> DynamoDBModel.DescribeTimeToLiveOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: DescribeTimeToLiveOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.describeTimeToLive.rawValue,
            reporting: self.invocationsReporting.describeTimeToLive,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the DisableKinesisStreamingDestination operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated KinesisStreamingDestinationInput object being passed to this operation.
     - Returns: The KinesisStreamingDestinationOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func disableKinesisStreamingDestination(
            input: DynamoDBModel.KinesisStreamingDestinationInput) async throws -> DynamoDBModel.KinesisStreamingDestinationOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: DisableKinesisStreamingDestinationOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.disableKinesisStreamingDestination.rawValue,
            reporting: self.invocationsReporting.disableKinesisStreamingDestination,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the EnableKinesisStreamingDestination operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated KinesisStreamingDestinationInput object being passed to this operation.
     - Returns: The KinesisStreamingDestinationOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func enableKinesisStreamingDestination(
            input: DynamoDBModel.KinesisStreamingDestinationInput) async throws -> DynamoDBModel.KinesisStreamingDestinationOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: EnableKinesisStreamingDestinationOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.enableKinesisStreamingDestination.rawValue,
            reporting: self.invocationsReporting.enableKinesisStreamingDestination,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ExecuteStatement operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ExecuteStatementInput object being passed to this operation.
     - Returns: The ExecuteStatementOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: conditionalCheckFailed, duplicateItem, internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionConflict.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func executeStatement(
            input: DynamoDBModel.ExecuteStatementInput) async throws -> DynamoDBModel.ExecuteStatementOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: ExecuteStatementOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.executeStatement.rawValue,
            reporting: self.invocationsReporting.executeStatement,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ExecuteTransaction operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ExecuteTransactionInput object being passed to this operation.
     - Returns: The ExecuteTransactionOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: idempotentParameterMismatch, internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionCanceled, transactionInProgress.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func executeTransaction(
            input: DynamoDBModel.ExecuteTransactionInput) async throws -> DynamoDBModel.ExecuteTransactionOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: ExecuteTransactionOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.executeTransaction.rawValue,
            reporting: self.invocationsReporting.executeTransaction,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ExportTableToPointInTime operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ExportTableToPointInTimeInput object being passed to this operation.
     - Returns: The ExportTableToPointInTimeOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: exportConflict, internalServer, invalidExportTime, limitExceeded, pointInTimeRecoveryUnavailable, tableNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func exportTableToPointInTime(
            input: DynamoDBModel.ExportTableToPointInTimeInput) async throws -> DynamoDBModel.ExportTableToPointInTimeOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: ExportTableToPointInTimeOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.exportTableToPointInTime.rawValue,
            reporting: self.invocationsReporting.exportTableToPointInTime,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the GetItem operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated GetItemInput object being passed to this operation.
     - Returns: The GetItemOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func getItem(
            input: DynamoDBModel.GetItemInput) async throws -> DynamoDBModel.GetItemOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: GetItemOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.getItem.rawValue,
            reporting: self.invocationsReporting.getItem,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ListBackups operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ListBackupsInput object being passed to this operation.
     - Returns: The ListBackupsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func listBackups(
            input: DynamoDBModel.ListBackupsInput) async throws -> DynamoDBModel.ListBackupsOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: ListBackupsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.listBackups.rawValue,
            reporting: self.invocationsReporting.listBackups,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ListContributorInsights operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ListContributorInsightsInput object being passed to this operation.
     - Returns: The ListContributorInsightsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func listContributorInsights(
            input: DynamoDBModel.ListContributorInsightsInput) async throws -> DynamoDBModel.ListContributorInsightsOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: ListContributorInsightsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.listContributorInsights.rawValue,
            reporting: self.invocationsReporting.listContributorInsights,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ListExports operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ListExportsInput object being passed to this operation.
     - Returns: The ListExportsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func listExports(
            input: DynamoDBModel.ListExportsInput) async throws -> DynamoDBModel.ListExportsOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: ListExportsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.listExports.rawValue,
            reporting: self.invocationsReporting.listExports,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ListGlobalTables operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ListGlobalTablesInput object being passed to this operation.
     - Returns: The ListGlobalTablesOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func listGlobalTables(
            input: DynamoDBModel.ListGlobalTablesInput) async throws -> DynamoDBModel.ListGlobalTablesOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: ListGlobalTablesOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.listGlobalTables.rawValue,
            reporting: self.invocationsReporting.listGlobalTables,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ListTables operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ListTablesInput object being passed to this operation.
     - Returns: The ListTablesOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func listTables(
            input: DynamoDBModel.ListTablesInput) async throws -> DynamoDBModel.ListTablesOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: ListTablesOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.listTables.rawValue,
            reporting: self.invocationsReporting.listTables,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the ListTagsOfResource operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ListTagsOfResourceInput object being passed to this operation.
     - Returns: The ListTagsOfResourceOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func listTagsOfResource(
            input: DynamoDBModel.ListTagsOfResourceInput) async throws -> DynamoDBModel.ListTagsOfResourceOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: ListTagsOfResourceOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.listTagsOfResource.rawValue,
            reporting: self.invocationsReporting.listTagsOfResource,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the PutItem operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated PutItemInput object being passed to this operation.
     - Returns: The PutItemOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: conditionalCheckFailed, internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionConflict.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func putItem(
            input: DynamoDBModel.PutItemInput) async throws -> DynamoDBModel.PutItemOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: PutItemOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.putItem.rawValue,
            reporting: self.invocationsReporting.putItem,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the Query operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated QueryInput object being passed to this operation.
     - Returns: The QueryOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func query(
            input: DynamoDBModel.QueryInput) async throws -> DynamoDBModel.QueryOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: QueryOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.query.rawValue,
            reporting: self.invocationsReporting.query,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the RestoreTableFromBackup operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated RestoreTableFromBackupInput object being passed to this operation.
     - Returns: The RestoreTableFromBackupOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: backupInUse, backupNotFound, internalServer, limitExceeded, tableAlreadyExists, tableInUse.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func restoreTableFromBackup(
            input: DynamoDBModel.RestoreTableFromBackupInput) async throws -> DynamoDBModel.RestoreTableFromBackupOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: RestoreTableFromBackupOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.restoreTableFromBackup.rawValue,
            reporting: self.invocationsReporting.restoreTableFromBackup,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the RestoreTableToPointInTime operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated RestoreTableToPointInTimeInput object being passed to this operation.
     - Returns: The RestoreTableToPointInTimeOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, invalidRestoreTime, limitExceeded, pointInTimeRecoveryUnavailable, tableAlreadyExists, tableInUse, tableNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func restoreTableToPointInTime(
            input: DynamoDBModel.RestoreTableToPointInTimeInput) async throws -> DynamoDBModel.RestoreTableToPointInTimeOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: RestoreTableToPointInTimeOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.restoreTableToPointInTime.rawValue,
            reporting: self.invocationsReporting.restoreTableToPointInTime,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the Scan operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ScanInput object being passed to this operation.
     - Returns: The ScanOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func scan(
            input: DynamoDBModel.ScanInput) async throws -> DynamoDBModel.ScanOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: ScanOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.scan.rawValue,
            reporting: self.invocationsReporting.scan,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the TagResource operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated TagResourceInput object being passed to this operation.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func tagResource(
            input: DynamoDBModel.TagResourceInput) async throws {
        return try await executeWithoutOutput(
            httpClient: httpClient,
            requestInput: TagResourceOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.tagResource.rawValue,
            reporting: self.invocationsReporting.tagResource,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the TransactGetItems operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated TransactGetItemsInput object being passed to this operation.
     - Returns: The TransactGetItemsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionCanceled.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func transactGetItems(
            input: DynamoDBModel.TransactGetItemsInput) async throws -> DynamoDBModel.TransactGetItemsOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: TransactGetItemsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.transactGetItems.rawValue,
            reporting: self.invocationsReporting.transactGetItems,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the TransactWriteItems operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated TransactWriteItemsInput object being passed to this operation.
     - Returns: The TransactWriteItemsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: idempotentParameterMismatch, internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionCanceled, transactionInProgress.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func transactWriteItems(
            input: DynamoDBModel.TransactWriteItemsInput) async throws -> DynamoDBModel.TransactWriteItemsOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: TransactWriteItemsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.transactWriteItems.rawValue,
            reporting: self.invocationsReporting.transactWriteItems,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UntagResource operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UntagResourceInput object being passed to this operation.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func untagResource(
            input: DynamoDBModel.UntagResourceInput) async throws {
        return try await executeWithoutOutput(
            httpClient: httpClient,
            requestInput: UntagResourceOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.untagResource.rawValue,
            reporting: self.invocationsReporting.untagResource,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UpdateContinuousBackups operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UpdateContinuousBackupsInput object being passed to this operation.
     - Returns: The UpdateContinuousBackupsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: continuousBackupsUnavailable, internalServer, tableNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func updateContinuousBackups(
            input: DynamoDBModel.UpdateContinuousBackupsInput) async throws -> DynamoDBModel.UpdateContinuousBackupsOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: UpdateContinuousBackupsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.updateContinuousBackups.rawValue,
            reporting: self.invocationsReporting.updateContinuousBackups,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UpdateContributorInsights operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UpdateContributorInsightsInput object being passed to this operation.
     - Returns: The UpdateContributorInsightsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func updateContributorInsights(
            input: DynamoDBModel.UpdateContributorInsightsInput) async throws -> DynamoDBModel.UpdateContributorInsightsOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: UpdateContributorInsightsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.updateContributorInsights.rawValue,
            reporting: self.invocationsReporting.updateContributorInsights,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UpdateGlobalTable operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UpdateGlobalTableInput object being passed to this operation.
     - Returns: The UpdateGlobalTableOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableNotFound, internalServer, replicaAlreadyExists, replicaNotFound, tableNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func updateGlobalTable(
            input: DynamoDBModel.UpdateGlobalTableInput) async throws -> DynamoDBModel.UpdateGlobalTableOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: UpdateGlobalTableOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.updateGlobalTable.rawValue,
            reporting: self.invocationsReporting.updateGlobalTable,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UpdateGlobalTableSettings operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UpdateGlobalTableSettingsInput object being passed to this operation.
     - Returns: The UpdateGlobalTableSettingsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableNotFound, indexNotFound, internalServer, limitExceeded, replicaNotFound, resourceInUse.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func updateGlobalTableSettings(
            input: DynamoDBModel.UpdateGlobalTableSettingsInput) async throws -> DynamoDBModel.UpdateGlobalTableSettingsOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: UpdateGlobalTableSettingsOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.updateGlobalTableSettings.rawValue,
            reporting: self.invocationsReporting.updateGlobalTableSettings,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UpdateItem operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UpdateItemInput object being passed to this operation.
     - Returns: The UpdateItemOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: conditionalCheckFailed, internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionConflict.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func updateItem(
            input: DynamoDBModel.UpdateItemInput) async throws -> DynamoDBModel.UpdateItemOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: UpdateItemOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.updateItem.rawValue,
            reporting: self.invocationsReporting.updateItem,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UpdateTable operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UpdateTableInput object being passed to this operation.
     - Returns: The UpdateTableOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func updateTable(
            input: DynamoDBModel.UpdateTableInput) async throws -> DynamoDBModel.UpdateTableOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: UpdateTableOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.updateTable.rawValue,
            reporting: self.invocationsReporting.updateTable,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UpdateTableReplicaAutoScaling operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UpdateTableReplicaAutoScalingInput object being passed to this operation.
     - Returns: The UpdateTableReplicaAutoScalingOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func updateTableReplicaAutoScaling(
            input: DynamoDBModel.UpdateTableReplicaAutoScalingInput) async throws -> DynamoDBModel.UpdateTableReplicaAutoScalingOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: UpdateTableReplicaAutoScalingOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.updateTableReplicaAutoScaling.rawValue,
            reporting: self.invocationsReporting.updateTableReplicaAutoScaling,
            errorType: DynamoDBError.self)
    }

    /**
     Invokes the UpdateTimeToLive operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UpdateTimeToLiveInput object being passed to this operation.
     - Returns: The UpdateTimeToLiveOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    @available(macOS 12, iOS 15, tvOS 15, watchOS 8, *)
    public func updateTimeToLive(
            input: DynamoDBModel.UpdateTimeToLiveInput) async throws -> DynamoDBModel.UpdateTimeToLiveOutput {
        return try await executeWithOutput(
            httpClient: httpClient,
            requestInput: UpdateTimeToLiveOperationHTTPRequestInput(encodable: input),
            operation: DynamoDBModelOperations.updateTimeToLive.rawValue,
            reporting: self.invocationsReporting.updateTimeToLive,
            errorType: DynamoDBError.self)
    }
    #endif
}
