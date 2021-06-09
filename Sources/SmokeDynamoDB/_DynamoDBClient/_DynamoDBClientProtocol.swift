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
// _DynamoDBClientProtocol.swift
// DynamoDBClient
//

import Foundation
import DynamoDBModel
import SmokeAWSCore
import SmokeHTTPClient
import NIO

/**
 Client Protocol for the DynamoDB service.
 */
protocol _DynamoDBClientProtocol {
    typealias BatchExecuteStatementEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.BatchExecuteStatementInput) -> EventLoopFuture<DynamoDBModel.BatchExecuteStatementOutput>
    typealias BatchGetItemEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.BatchGetItemInput) -> EventLoopFuture<DynamoDBModel.BatchGetItemOutput>
    typealias BatchWriteItemEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.BatchWriteItemInput) -> EventLoopFuture<DynamoDBModel.BatchWriteItemOutput>
    typealias CreateBackupEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.CreateBackupInput) -> EventLoopFuture<DynamoDBModel.CreateBackupOutput>
    typealias CreateGlobalTableEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.CreateGlobalTableInput) -> EventLoopFuture<DynamoDBModel.CreateGlobalTableOutput>
    typealias CreateTableEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.CreateTableInput) -> EventLoopFuture<DynamoDBModel.CreateTableOutput>
    typealias DeleteBackupEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.DeleteBackupInput) -> EventLoopFuture<DynamoDBModel.DeleteBackupOutput>
    typealias DeleteItemEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.DeleteItemInput) -> EventLoopFuture<DynamoDBModel.DeleteItemOutput>
    typealias DeleteTableEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.DeleteTableInput) -> EventLoopFuture<DynamoDBModel.DeleteTableOutput>
    typealias DescribeBackupEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.DescribeBackupInput) -> EventLoopFuture<DynamoDBModel.DescribeBackupOutput>
    typealias DescribeContinuousBackupsEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.DescribeContinuousBackupsInput) -> EventLoopFuture<DynamoDBModel.DescribeContinuousBackupsOutput>
    typealias DescribeContributorInsightsEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.DescribeContributorInsightsInput) -> EventLoopFuture<DynamoDBModel.DescribeContributorInsightsOutput>
    typealias DescribeEndpointsEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.DescribeEndpointsRequest) -> EventLoopFuture<DynamoDBModel.DescribeEndpointsResponse>
    typealias DescribeExportEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.DescribeExportInput) -> EventLoopFuture<DynamoDBModel.DescribeExportOutput>
    typealias DescribeGlobalTableEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.DescribeGlobalTableInput) -> EventLoopFuture<DynamoDBModel.DescribeGlobalTableOutput>
    typealias DescribeGlobalTableSettingsEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.DescribeGlobalTableSettingsInput) -> EventLoopFuture<DynamoDBModel.DescribeGlobalTableSettingsOutput>
    typealias DescribeKinesisStreamingDestinationEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.DescribeKinesisStreamingDestinationInput) -> EventLoopFuture<DynamoDBModel.DescribeKinesisStreamingDestinationOutput>
    typealias DescribeLimitsEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.DescribeLimitsInput) -> EventLoopFuture<DynamoDBModel.DescribeLimitsOutput>
    typealias DescribeTableEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.DescribeTableInput) -> EventLoopFuture<DynamoDBModel.DescribeTableOutput>
    typealias DescribeTableReplicaAutoScalingEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.DescribeTableReplicaAutoScalingInput) -> EventLoopFuture<DynamoDBModel.DescribeTableReplicaAutoScalingOutput>
    typealias DescribeTimeToLiveEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.DescribeTimeToLiveInput) -> EventLoopFuture<DynamoDBModel.DescribeTimeToLiveOutput>
    typealias DisableKinesisStreamingDestinationEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.KinesisStreamingDestinationInput) -> EventLoopFuture<DynamoDBModel.KinesisStreamingDestinationOutput>
    typealias EnableKinesisStreamingDestinationEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.KinesisStreamingDestinationInput) -> EventLoopFuture<DynamoDBModel.KinesisStreamingDestinationOutput>
    typealias ExecuteStatementEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.ExecuteStatementInput) -> EventLoopFuture<DynamoDBModel.ExecuteStatementOutput>
    typealias ExecuteTransactionEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.ExecuteTransactionInput) -> EventLoopFuture<DynamoDBModel.ExecuteTransactionOutput>
    typealias ExportTableToPointInTimeEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.ExportTableToPointInTimeInput) -> EventLoopFuture<DynamoDBModel.ExportTableToPointInTimeOutput>
    typealias GetItemEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.GetItemInput) -> EventLoopFuture<DynamoDBModel.GetItemOutput>
    typealias ListBackupsEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.ListBackupsInput) -> EventLoopFuture<DynamoDBModel.ListBackupsOutput>
    typealias ListContributorInsightsEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.ListContributorInsightsInput) -> EventLoopFuture<DynamoDBModel.ListContributorInsightsOutput>
    typealias ListExportsEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.ListExportsInput) -> EventLoopFuture<DynamoDBModel.ListExportsOutput>
    typealias ListGlobalTablesEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.ListGlobalTablesInput) -> EventLoopFuture<DynamoDBModel.ListGlobalTablesOutput>
    typealias ListTablesEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.ListTablesInput) -> EventLoopFuture<DynamoDBModel.ListTablesOutput>
    typealias ListTagsOfResourceEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.ListTagsOfResourceInput) -> EventLoopFuture<DynamoDBModel.ListTagsOfResourceOutput>
    typealias PutItemEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.PutItemInput) -> EventLoopFuture<DynamoDBModel.PutItemOutput>
    typealias QueryEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.QueryInput) -> EventLoopFuture<DynamoDBModel.QueryOutput>
    typealias RestoreTableFromBackupEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.RestoreTableFromBackupInput) -> EventLoopFuture<DynamoDBModel.RestoreTableFromBackupOutput>
    typealias RestoreTableToPointInTimeEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.RestoreTableToPointInTimeInput) -> EventLoopFuture<DynamoDBModel.RestoreTableToPointInTimeOutput>
    typealias ScanEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.ScanInput) -> EventLoopFuture<DynamoDBModel.ScanOutput>
    typealias TagResourceEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.TagResourceInput) -> EventLoopFuture<Void>
    typealias TransactGetItemsEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.TransactGetItemsInput) -> EventLoopFuture<DynamoDBModel.TransactGetItemsOutput>
    typealias TransactWriteItemsEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.TransactWriteItemsInput) -> EventLoopFuture<DynamoDBModel.TransactWriteItemsOutput>
    typealias UntagResourceEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.UntagResourceInput) -> EventLoopFuture<Void>
    typealias UpdateContinuousBackupsEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.UpdateContinuousBackupsInput) -> EventLoopFuture<DynamoDBModel.UpdateContinuousBackupsOutput>
    typealias UpdateContributorInsightsEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.UpdateContributorInsightsInput) -> EventLoopFuture<DynamoDBModel.UpdateContributorInsightsOutput>
    typealias UpdateGlobalTableEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.UpdateGlobalTableInput) -> EventLoopFuture<DynamoDBModel.UpdateGlobalTableOutput>
    typealias UpdateGlobalTableSettingsEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.UpdateGlobalTableSettingsInput) -> EventLoopFuture<DynamoDBModel.UpdateGlobalTableSettingsOutput>
    typealias UpdateItemEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.UpdateItemInput) -> EventLoopFuture<DynamoDBModel.UpdateItemOutput>
    typealias UpdateTableEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.UpdateTableInput) -> EventLoopFuture<DynamoDBModel.UpdateTableOutput>
    typealias UpdateTableReplicaAutoScalingEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.UpdateTableReplicaAutoScalingInput) -> EventLoopFuture<DynamoDBModel.UpdateTableReplicaAutoScalingOutput>
    typealias UpdateTimeToLiveEventLoopFutureAsyncType = (
            _ input: DynamoDBModel.UpdateTimeToLiveInput) -> EventLoopFuture<DynamoDBModel.UpdateTimeToLiveOutput>
    #if compiler(>=5.5) && $AsyncAwait
    typealias BatchExecuteStatementFunctionType = (
            _ input: DynamoDBModel.BatchExecuteStatementInput) async throws -> DynamoDBModel.BatchExecuteStatementOutput
    typealias BatchGetItemFunctionType = (
            _ input: DynamoDBModel.BatchGetItemInput) async throws -> DynamoDBModel.BatchGetItemOutput
    typealias BatchWriteItemFunctionType = (
            _ input: DynamoDBModel.BatchWriteItemInput) async throws -> DynamoDBModel.BatchWriteItemOutput
    typealias CreateBackupFunctionType = (
            _ input: DynamoDBModel.CreateBackupInput) async throws -> DynamoDBModel.CreateBackupOutput
    typealias CreateGlobalTableFunctionType = (
            _ input: DynamoDBModel.CreateGlobalTableInput) async throws -> DynamoDBModel.CreateGlobalTableOutput
    typealias CreateTableFunctionType = (
            _ input: DynamoDBModel.CreateTableInput) async throws -> DynamoDBModel.CreateTableOutput
    typealias DeleteBackupFunctionType = (
            _ input: DynamoDBModel.DeleteBackupInput) async throws -> DynamoDBModel.DeleteBackupOutput
    typealias DeleteItemFunctionType = (
            _ input: DynamoDBModel.DeleteItemInput) async throws -> DynamoDBModel.DeleteItemOutput
    typealias DeleteTableFunctionType = (
            _ input: DynamoDBModel.DeleteTableInput) async throws -> DynamoDBModel.DeleteTableOutput
    typealias DescribeBackupFunctionType = (
            _ input: DynamoDBModel.DescribeBackupInput) async throws -> DynamoDBModel.DescribeBackupOutput
    typealias DescribeContinuousBackupsFunctionType = (
            _ input: DynamoDBModel.DescribeContinuousBackupsInput) async throws -> DynamoDBModel.DescribeContinuousBackupsOutput
    typealias DescribeContributorInsightsFunctionType = (
            _ input: DynamoDBModel.DescribeContributorInsightsInput) async throws -> DynamoDBModel.DescribeContributorInsightsOutput
    typealias DescribeEndpointsFunctionType = (
            _ input: DynamoDBModel.DescribeEndpointsRequest) async throws -> DynamoDBModel.DescribeEndpointsResponse
    typealias DescribeExportFunctionType = (
            _ input: DynamoDBModel.DescribeExportInput) async throws -> DynamoDBModel.DescribeExportOutput
    typealias DescribeGlobalTableFunctionType = (
            _ input: DynamoDBModel.DescribeGlobalTableInput) async throws -> DynamoDBModel.DescribeGlobalTableOutput
    typealias DescribeGlobalTableSettingsFunctionType = (
            _ input: DynamoDBModel.DescribeGlobalTableSettingsInput) async throws -> DynamoDBModel.DescribeGlobalTableSettingsOutput
    typealias DescribeKinesisStreamingDestinationFunctionType = (
            _ input: DynamoDBModel.DescribeKinesisStreamingDestinationInput) async throws -> DynamoDBModel.DescribeKinesisStreamingDestinationOutput
    typealias DescribeLimitsFunctionType = (
            _ input: DynamoDBModel.DescribeLimitsInput) async throws -> DynamoDBModel.DescribeLimitsOutput
    typealias DescribeTableFunctionType = (
            _ input: DynamoDBModel.DescribeTableInput) async throws -> DynamoDBModel.DescribeTableOutput
    typealias DescribeTableReplicaAutoScalingFunctionType = (
            _ input: DynamoDBModel.DescribeTableReplicaAutoScalingInput) async throws -> DynamoDBModel.DescribeTableReplicaAutoScalingOutput
    typealias DescribeTimeToLiveFunctionType = (
            _ input: DynamoDBModel.DescribeTimeToLiveInput) async throws -> DynamoDBModel.DescribeTimeToLiveOutput
    typealias DisableKinesisStreamingDestinationFunctionType = (
            _ input: DynamoDBModel.KinesisStreamingDestinationInput) async throws -> DynamoDBModel.KinesisStreamingDestinationOutput
    typealias EnableKinesisStreamingDestinationFunctionType = (
            _ input: DynamoDBModel.KinesisStreamingDestinationInput) async throws -> DynamoDBModel.KinesisStreamingDestinationOutput
    typealias ExecuteStatementFunctionType = (
            _ input: DynamoDBModel.ExecuteStatementInput) async throws -> DynamoDBModel.ExecuteStatementOutput
    typealias ExecuteTransactionFunctionType = (
            _ input: DynamoDBModel.ExecuteTransactionInput) async throws -> DynamoDBModel.ExecuteTransactionOutput
    typealias ExportTableToPointInTimeFunctionType = (
            _ input: DynamoDBModel.ExportTableToPointInTimeInput) async throws -> DynamoDBModel.ExportTableToPointInTimeOutput
    typealias GetItemFunctionType = (
            _ input: DynamoDBModel.GetItemInput) async throws -> DynamoDBModel.GetItemOutput
    typealias ListBackupsFunctionType = (
            _ input: DynamoDBModel.ListBackupsInput) async throws -> DynamoDBModel.ListBackupsOutput
    typealias ListContributorInsightsFunctionType = (
            _ input: DynamoDBModel.ListContributorInsightsInput) async throws -> DynamoDBModel.ListContributorInsightsOutput
    typealias ListExportsFunctionType = (
            _ input: DynamoDBModel.ListExportsInput) async throws -> DynamoDBModel.ListExportsOutput
    typealias ListGlobalTablesFunctionType = (
            _ input: DynamoDBModel.ListGlobalTablesInput) async throws -> DynamoDBModel.ListGlobalTablesOutput
    typealias ListTablesFunctionType = (
            _ input: DynamoDBModel.ListTablesInput) async throws -> DynamoDBModel.ListTablesOutput
    typealias ListTagsOfResourceFunctionType = (
            _ input: DynamoDBModel.ListTagsOfResourceInput) async throws -> DynamoDBModel.ListTagsOfResourceOutput
    typealias PutItemFunctionType = (
            _ input: DynamoDBModel.PutItemInput) async throws -> DynamoDBModel.PutItemOutput
    typealias QueryFunctionType = (
            _ input: DynamoDBModel.QueryInput) async throws -> DynamoDBModel.QueryOutput
    typealias RestoreTableFromBackupFunctionType = (
            _ input: DynamoDBModel.RestoreTableFromBackupInput) async throws -> DynamoDBModel.RestoreTableFromBackupOutput
    typealias RestoreTableToPointInTimeFunctionType = (
            _ input: DynamoDBModel.RestoreTableToPointInTimeInput) async throws -> DynamoDBModel.RestoreTableToPointInTimeOutput
    typealias ScanFunctionType = (
            _ input: DynamoDBModel.ScanInput) async throws -> DynamoDBModel.ScanOutput
    typealias TagResourceFunctionType = (
            _ input: DynamoDBModel.TagResourceInput) async throws -> ()
    typealias TransactGetItemsFunctionType = (
            _ input: DynamoDBModel.TransactGetItemsInput) async throws -> DynamoDBModel.TransactGetItemsOutput
    typealias TransactWriteItemsFunctionType = (
            _ input: DynamoDBModel.TransactWriteItemsInput) async throws -> DynamoDBModel.TransactWriteItemsOutput
    typealias UntagResourceFunctionType = (
            _ input: DynamoDBModel.UntagResourceInput) async throws -> ()
    typealias UpdateContinuousBackupsFunctionType = (
            _ input: DynamoDBModel.UpdateContinuousBackupsInput) async throws -> DynamoDBModel.UpdateContinuousBackupsOutput
    typealias UpdateContributorInsightsFunctionType = (
            _ input: DynamoDBModel.UpdateContributorInsightsInput) async throws -> DynamoDBModel.UpdateContributorInsightsOutput
    typealias UpdateGlobalTableFunctionType = (
            _ input: DynamoDBModel.UpdateGlobalTableInput) async throws -> DynamoDBModel.UpdateGlobalTableOutput
    typealias UpdateGlobalTableSettingsFunctionType = (
            _ input: DynamoDBModel.UpdateGlobalTableSettingsInput) async throws -> DynamoDBModel.UpdateGlobalTableSettingsOutput
    typealias UpdateItemFunctionType = (
            _ input: DynamoDBModel.UpdateItemInput) async throws -> DynamoDBModel.UpdateItemOutput
    typealias UpdateTableFunctionType = (
            _ input: DynamoDBModel.UpdateTableInput) async throws -> DynamoDBModel.UpdateTableOutput
    typealias UpdateTableReplicaAutoScalingFunctionType = (
            _ input: DynamoDBModel.UpdateTableReplicaAutoScalingInput) async throws -> DynamoDBModel.UpdateTableReplicaAutoScalingOutput
    typealias UpdateTimeToLiveFunctionType = (
            _ input: DynamoDBModel.UpdateTimeToLiveInput) async throws -> DynamoDBModel.UpdateTimeToLiveOutput
    #endif

    /**
     Invokes the BatchExecuteStatement operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated BatchExecuteStatementInput object being passed to this operation.
     - Returns: A future to the BatchExecuteStatementOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, requestLimitExceeded.
     */
    func batchExecuteStatement(
            input: DynamoDBModel.BatchExecuteStatementInput) -> EventLoopFuture<DynamoDBModel.BatchExecuteStatementOutput>

    /**
     Invokes the BatchGetItem operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated BatchGetItemInput object being passed to this operation.
     - Returns: A future to the BatchGetItemOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    func batchGetItem(
            input: DynamoDBModel.BatchGetItemInput) -> EventLoopFuture<DynamoDBModel.BatchGetItemOutput>

    /**
     Invokes the BatchWriteItem operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated BatchWriteItemInput object being passed to this operation.
     - Returns: A future to the BatchWriteItemOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    func batchWriteItem(
            input: DynamoDBModel.BatchWriteItemInput) -> EventLoopFuture<DynamoDBModel.BatchWriteItemOutput>

    /**
     Invokes the CreateBackup operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated CreateBackupInput object being passed to this operation.
     - Returns: A future to the CreateBackupOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: backupInUse, continuousBackupsUnavailable, internalServer, limitExceeded, tableInUse, tableNotFound.
     */
    func createBackup(
            input: DynamoDBModel.CreateBackupInput) -> EventLoopFuture<DynamoDBModel.CreateBackupOutput>

    /**
     Invokes the CreateGlobalTable operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated CreateGlobalTableInput object being passed to this operation.
     - Returns: A future to the CreateGlobalTableOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableAlreadyExists, internalServer, limitExceeded, tableNotFound.
     */
    func createGlobalTable(
            input: DynamoDBModel.CreateGlobalTableInput) -> EventLoopFuture<DynamoDBModel.CreateGlobalTableOutput>

    /**
     Invokes the CreateTable operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated CreateTableInput object being passed to this operation.
     - Returns: A future to the CreateTableOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse.
     */
    func createTable(
            input: DynamoDBModel.CreateTableInput) -> EventLoopFuture<DynamoDBModel.CreateTableOutput>

    /**
     Invokes the DeleteBackup operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DeleteBackupInput object being passed to this operation.
     - Returns: A future to the DeleteBackupOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: backupInUse, backupNotFound, internalServer, limitExceeded.
     */
    func deleteBackup(
            input: DynamoDBModel.DeleteBackupInput) -> EventLoopFuture<DynamoDBModel.DeleteBackupOutput>

    /**
     Invokes the DeleteItem operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DeleteItemInput object being passed to this operation.
     - Returns: A future to the DeleteItemOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: conditionalCheckFailed, internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionConflict.
     */
    func deleteItem(
            input: DynamoDBModel.DeleteItemInput) -> EventLoopFuture<DynamoDBModel.DeleteItemOutput>

    /**
     Invokes the DeleteTable operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DeleteTableInput object being passed to this operation.
     - Returns: A future to the DeleteTableOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    func deleteTable(
            input: DynamoDBModel.DeleteTableInput) -> EventLoopFuture<DynamoDBModel.DeleteTableOutput>

    /**
     Invokes the DescribeBackup operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeBackupInput object being passed to this operation.
     - Returns: A future to the DescribeBackupOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: backupNotFound, internalServer.
     */
    func describeBackup(
            input: DynamoDBModel.DescribeBackupInput) -> EventLoopFuture<DynamoDBModel.DescribeBackupOutput>

    /**
     Invokes the DescribeContinuousBackups operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeContinuousBackupsInput object being passed to this operation.
     - Returns: A future to the DescribeContinuousBackupsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, tableNotFound.
     */
    func describeContinuousBackups(
            input: DynamoDBModel.DescribeContinuousBackupsInput) -> EventLoopFuture<DynamoDBModel.DescribeContinuousBackupsOutput>

    /**
     Invokes the DescribeContributorInsights operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeContributorInsightsInput object being passed to this operation.
     - Returns: A future to the DescribeContributorInsightsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    func describeContributorInsights(
            input: DynamoDBModel.DescribeContributorInsightsInput) -> EventLoopFuture<DynamoDBModel.DescribeContributorInsightsOutput>

    /**
     Invokes the DescribeEndpoints operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeEndpointsRequest object being passed to this operation.
     - Returns: A future to the DescribeEndpointsResponse object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
     */
    func describeEndpoints(
            input: DynamoDBModel.DescribeEndpointsRequest) -> EventLoopFuture<DynamoDBModel.DescribeEndpointsResponse>

    /**
     Invokes the DescribeExport operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeExportInput object being passed to this operation.
     - Returns: A future to the DescribeExportOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: exportNotFound, internalServer, limitExceeded.
     */
    func describeExport(
            input: DynamoDBModel.DescribeExportInput) -> EventLoopFuture<DynamoDBModel.DescribeExportOutput>

    /**
     Invokes the DescribeGlobalTable operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeGlobalTableInput object being passed to this operation.
     - Returns: A future to the DescribeGlobalTableOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableNotFound, internalServer.
     */
    func describeGlobalTable(
            input: DynamoDBModel.DescribeGlobalTableInput) -> EventLoopFuture<DynamoDBModel.DescribeGlobalTableOutput>

    /**
     Invokes the DescribeGlobalTableSettings operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeGlobalTableSettingsInput object being passed to this operation.
     - Returns: A future to the DescribeGlobalTableSettingsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableNotFound, internalServer.
     */
    func describeGlobalTableSettings(
            input: DynamoDBModel.DescribeGlobalTableSettingsInput) -> EventLoopFuture<DynamoDBModel.DescribeGlobalTableSettingsOutput>

    /**
     Invokes the DescribeKinesisStreamingDestination operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeKinesisStreamingDestinationInput object being passed to this operation.
     - Returns: A future to the DescribeKinesisStreamingDestinationOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    func describeKinesisStreamingDestination(
            input: DynamoDBModel.DescribeKinesisStreamingDestinationInput) -> EventLoopFuture<DynamoDBModel.DescribeKinesisStreamingDestinationOutput>

    /**
     Invokes the DescribeLimits operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeLimitsInput object being passed to this operation.
     - Returns: A future to the DescribeLimitsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer.
     */
    func describeLimits(
            input: DynamoDBModel.DescribeLimitsInput) -> EventLoopFuture<DynamoDBModel.DescribeLimitsOutput>

    /**
     Invokes the DescribeTable operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeTableInput object being passed to this operation.
     - Returns: A future to the DescribeTableOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    func describeTable(
            input: DynamoDBModel.DescribeTableInput) -> EventLoopFuture<DynamoDBModel.DescribeTableOutput>

    /**
     Invokes the DescribeTableReplicaAutoScaling operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeTableReplicaAutoScalingInput object being passed to this operation.
     - Returns: A future to the DescribeTableReplicaAutoScalingOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    func describeTableReplicaAutoScaling(
            input: DynamoDBModel.DescribeTableReplicaAutoScalingInput) -> EventLoopFuture<DynamoDBModel.DescribeTableReplicaAutoScalingOutput>

    /**
     Invokes the DescribeTimeToLive operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated DescribeTimeToLiveInput object being passed to this operation.
     - Returns: A future to the DescribeTimeToLiveOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    func describeTimeToLive(
            input: DynamoDBModel.DescribeTimeToLiveInput) -> EventLoopFuture<DynamoDBModel.DescribeTimeToLiveOutput>

    /**
     Invokes the DisableKinesisStreamingDestination operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated KinesisStreamingDestinationInput object being passed to this operation.
     - Returns: A future to the KinesisStreamingDestinationOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    func disableKinesisStreamingDestination(
            input: DynamoDBModel.KinesisStreamingDestinationInput) -> EventLoopFuture<DynamoDBModel.KinesisStreamingDestinationOutput>

    /**
     Invokes the EnableKinesisStreamingDestination operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated KinesisStreamingDestinationInput object being passed to this operation.
     - Returns: A future to the KinesisStreamingDestinationOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    func enableKinesisStreamingDestination(
            input: DynamoDBModel.KinesisStreamingDestinationInput) -> EventLoopFuture<DynamoDBModel.KinesisStreamingDestinationOutput>

    /**
     Invokes the ExecuteStatement operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ExecuteStatementInput object being passed to this operation.
     - Returns: A future to the ExecuteStatementOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: conditionalCheckFailed, duplicateItem, internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionConflict.
     */
    func executeStatement(
            input: DynamoDBModel.ExecuteStatementInput) -> EventLoopFuture<DynamoDBModel.ExecuteStatementOutput>

    /**
     Invokes the ExecuteTransaction operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ExecuteTransactionInput object being passed to this operation.
     - Returns: A future to the ExecuteTransactionOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: idempotentParameterMismatch, internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionCanceled, transactionInProgress.
     */
    func executeTransaction(
            input: DynamoDBModel.ExecuteTransactionInput) -> EventLoopFuture<DynamoDBModel.ExecuteTransactionOutput>

    /**
     Invokes the ExportTableToPointInTime operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ExportTableToPointInTimeInput object being passed to this operation.
     - Returns: A future to the ExportTableToPointInTimeOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: exportConflict, internalServer, invalidExportTime, limitExceeded, pointInTimeRecoveryUnavailable, tableNotFound.
     */
    func exportTableToPointInTime(
            input: DynamoDBModel.ExportTableToPointInTimeInput) -> EventLoopFuture<DynamoDBModel.ExportTableToPointInTimeOutput>

    /**
     Invokes the GetItem operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated GetItemInput object being passed to this operation.
     - Returns: A future to the GetItemOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    func getItem(
            input: DynamoDBModel.GetItemInput) -> EventLoopFuture<DynamoDBModel.GetItemOutput>

    /**
     Invokes the ListBackups operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ListBackupsInput object being passed to this operation.
     - Returns: A future to the ListBackupsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer.
     */
    func listBackups(
            input: DynamoDBModel.ListBackupsInput) -> EventLoopFuture<DynamoDBModel.ListBackupsOutput>

    /**
     Invokes the ListContributorInsights operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ListContributorInsightsInput object being passed to this operation.
     - Returns: A future to the ListContributorInsightsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    func listContributorInsights(
            input: DynamoDBModel.ListContributorInsightsInput) -> EventLoopFuture<DynamoDBModel.ListContributorInsightsOutput>

    /**
     Invokes the ListExports operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ListExportsInput object being passed to this operation.
     - Returns: A future to the ListExportsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded.
     */
    func listExports(
            input: DynamoDBModel.ListExportsInput) -> EventLoopFuture<DynamoDBModel.ListExportsOutput>

    /**
     Invokes the ListGlobalTables operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ListGlobalTablesInput object being passed to this operation.
     - Returns: A future to the ListGlobalTablesOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer.
     */
    func listGlobalTables(
            input: DynamoDBModel.ListGlobalTablesInput) -> EventLoopFuture<DynamoDBModel.ListGlobalTablesOutput>

    /**
     Invokes the ListTables operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ListTablesInput object being passed to this operation.
     - Returns: A future to the ListTablesOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer.
     */
    func listTables(
            input: DynamoDBModel.ListTablesInput) -> EventLoopFuture<DynamoDBModel.ListTablesOutput>

    /**
     Invokes the ListTagsOfResource operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ListTagsOfResourceInput object being passed to this operation.
     - Returns: A future to the ListTagsOfResourceOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    func listTagsOfResource(
            input: DynamoDBModel.ListTagsOfResourceInput) -> EventLoopFuture<DynamoDBModel.ListTagsOfResourceOutput>

    /**
     Invokes the PutItem operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated PutItemInput object being passed to this operation.
     - Returns: A future to the PutItemOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: conditionalCheckFailed, internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionConflict.
     */
    func putItem(
            input: DynamoDBModel.PutItemInput) -> EventLoopFuture<DynamoDBModel.PutItemOutput>

    /**
     Invokes the Query operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated QueryInput object being passed to this operation.
     - Returns: A future to the QueryOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    func query(
            input: DynamoDBModel.QueryInput) -> EventLoopFuture<DynamoDBModel.QueryOutput>

    /**
     Invokes the RestoreTableFromBackup operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated RestoreTableFromBackupInput object being passed to this operation.
     - Returns: A future to the RestoreTableFromBackupOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: backupInUse, backupNotFound, internalServer, limitExceeded, tableAlreadyExists, tableInUse.
     */
    func restoreTableFromBackup(
            input: DynamoDBModel.RestoreTableFromBackupInput) -> EventLoopFuture<DynamoDBModel.RestoreTableFromBackupOutput>

    /**
     Invokes the RestoreTableToPointInTime operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated RestoreTableToPointInTimeInput object being passed to this operation.
     - Returns: A future to the RestoreTableToPointInTimeOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, invalidRestoreTime, limitExceeded, pointInTimeRecoveryUnavailable, tableAlreadyExists, tableInUse, tableNotFound.
     */
    func restoreTableToPointInTime(
            input: DynamoDBModel.RestoreTableToPointInTimeInput) -> EventLoopFuture<DynamoDBModel.RestoreTableToPointInTimeOutput>

    /**
     Invokes the Scan operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated ScanInput object being passed to this operation.
     - Returns: A future to the ScanOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    func scan(
            input: DynamoDBModel.ScanInput) -> EventLoopFuture<DynamoDBModel.ScanOutput>

    /**
     Invokes the TagResource operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated TagResourceInput object being passed to this operation.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    func tagResource(
            input: DynamoDBModel.TagResourceInput) -> EventLoopFuture<Void>

    /**
     Invokes the TransactGetItems operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated TransactGetItemsInput object being passed to this operation.
     - Returns: A future to the TransactGetItemsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionCanceled.
     */
    func transactGetItems(
            input: DynamoDBModel.TransactGetItemsInput) -> EventLoopFuture<DynamoDBModel.TransactGetItemsOutput>

    /**
     Invokes the TransactWriteItems operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated TransactWriteItemsInput object being passed to this operation.
     - Returns: A future to the TransactWriteItemsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: idempotentParameterMismatch, internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionCanceled, transactionInProgress.
     */
    func transactWriteItems(
            input: DynamoDBModel.TransactWriteItemsInput) -> EventLoopFuture<DynamoDBModel.TransactWriteItemsOutput>

    /**
     Invokes the UntagResource operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UntagResourceInput object being passed to this operation.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    func untagResource(
            input: DynamoDBModel.UntagResourceInput) -> EventLoopFuture<Void>

    /**
     Invokes the UpdateContinuousBackups operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UpdateContinuousBackupsInput object being passed to this operation.
     - Returns: A future to the UpdateContinuousBackupsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: continuousBackupsUnavailable, internalServer, tableNotFound.
     */
    func updateContinuousBackups(
            input: DynamoDBModel.UpdateContinuousBackupsInput) -> EventLoopFuture<DynamoDBModel.UpdateContinuousBackupsOutput>

    /**
     Invokes the UpdateContributorInsights operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UpdateContributorInsightsInput object being passed to this operation.
     - Returns: A future to the UpdateContributorInsightsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    func updateContributorInsights(
            input: DynamoDBModel.UpdateContributorInsightsInput) -> EventLoopFuture<DynamoDBModel.UpdateContributorInsightsOutput>

    /**
     Invokes the UpdateGlobalTable operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UpdateGlobalTableInput object being passed to this operation.
     - Returns: A future to the UpdateGlobalTableOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableNotFound, internalServer, replicaAlreadyExists, replicaNotFound, tableNotFound.
     */
    func updateGlobalTable(
            input: DynamoDBModel.UpdateGlobalTableInput) -> EventLoopFuture<DynamoDBModel.UpdateGlobalTableOutput>

    /**
     Invokes the UpdateGlobalTableSettings operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UpdateGlobalTableSettingsInput object being passed to this operation.
     - Returns: A future to the UpdateGlobalTableSettingsOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableNotFound, indexNotFound, internalServer, limitExceeded, replicaNotFound, resourceInUse.
     */
    func updateGlobalTableSettings(
            input: DynamoDBModel.UpdateGlobalTableSettingsInput) -> EventLoopFuture<DynamoDBModel.UpdateGlobalTableSettingsOutput>

    /**
     Invokes the UpdateItem operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UpdateItemInput object being passed to this operation.
     - Returns: A future to the UpdateItemOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: conditionalCheckFailed, internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionConflict.
     */
    func updateItem(
            input: DynamoDBModel.UpdateItemInput) -> EventLoopFuture<DynamoDBModel.UpdateItemOutput>

    /**
     Invokes the UpdateTable operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UpdateTableInput object being passed to this operation.
     - Returns: A future to the UpdateTableOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    func updateTable(
            input: DynamoDBModel.UpdateTableInput) -> EventLoopFuture<DynamoDBModel.UpdateTableOutput>

    /**
     Invokes the UpdateTableReplicaAutoScaling operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UpdateTableReplicaAutoScalingInput object being passed to this operation.
     - Returns: A future to the UpdateTableReplicaAutoScalingOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    func updateTableReplicaAutoScaling(
            input: DynamoDBModel.UpdateTableReplicaAutoScalingInput) -> EventLoopFuture<DynamoDBModel.UpdateTableReplicaAutoScalingOutput>

    /**
     Invokes the UpdateTimeToLive operation returning immediately with an `EventLoopFuture` that will be completed at a later time.

     - Parameters:
         - input: The validated UpdateTimeToLiveInput object being passed to this operation.
     - Returns: A future to the UpdateTimeToLiveOutput object to be passed back from the caller of this operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    func updateTimeToLive(
            input: DynamoDBModel.UpdateTimeToLiveInput) -> EventLoopFuture<DynamoDBModel.UpdateTimeToLiveOutput>

    #if compiler(>=5.5) && $AsyncAwait
    /**
     Invokes the BatchExecuteStatement operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated BatchExecuteStatementInput object being passed to this operation.
     - Returns: The BatchExecuteStatementOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, requestLimitExceeded.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func batchExecuteStatement(
            input: DynamoDBModel.BatchExecuteStatementInput) async throws -> DynamoDBModel.BatchExecuteStatementOutput

    /**
     Invokes the BatchGetItem operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated BatchGetItemInput object being passed to this operation.
     - Returns: The BatchGetItemOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func batchGetItem(
            input: DynamoDBModel.BatchGetItemInput) async throws -> DynamoDBModel.BatchGetItemOutput

    /**
     Invokes the BatchWriteItem operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated BatchWriteItemInput object being passed to this operation.
     - Returns: The BatchWriteItemOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func batchWriteItem(
            input: DynamoDBModel.BatchWriteItemInput) async throws -> DynamoDBModel.BatchWriteItemOutput

    /**
     Invokes the CreateBackup operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated CreateBackupInput object being passed to this operation.
     - Returns: The CreateBackupOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: backupInUse, continuousBackupsUnavailable, internalServer, limitExceeded, tableInUse, tableNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func createBackup(
            input: DynamoDBModel.CreateBackupInput) async throws -> DynamoDBModel.CreateBackupOutput

    /**
     Invokes the CreateGlobalTable operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated CreateGlobalTableInput object being passed to this operation.
     - Returns: The CreateGlobalTableOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableAlreadyExists, internalServer, limitExceeded, tableNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func createGlobalTable(
            input: DynamoDBModel.CreateGlobalTableInput) async throws -> DynamoDBModel.CreateGlobalTableOutput

    /**
     Invokes the CreateTable operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated CreateTableInput object being passed to this operation.
     - Returns: The CreateTableOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func createTable(
            input: DynamoDBModel.CreateTableInput) async throws -> DynamoDBModel.CreateTableOutput

    /**
     Invokes the DeleteBackup operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DeleteBackupInput object being passed to this operation.
     - Returns: The DeleteBackupOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: backupInUse, backupNotFound, internalServer, limitExceeded.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func deleteBackup(
            input: DynamoDBModel.DeleteBackupInput) async throws -> DynamoDBModel.DeleteBackupOutput

    /**
     Invokes the DeleteItem operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DeleteItemInput object being passed to this operation.
     - Returns: The DeleteItemOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: conditionalCheckFailed, internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionConflict.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func deleteItem(
            input: DynamoDBModel.DeleteItemInput) async throws -> DynamoDBModel.DeleteItemOutput

    /**
     Invokes the DeleteTable operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DeleteTableInput object being passed to this operation.
     - Returns: The DeleteTableOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func deleteTable(
            input: DynamoDBModel.DeleteTableInput) async throws -> DynamoDBModel.DeleteTableOutput

    /**
     Invokes the DescribeBackup operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeBackupInput object being passed to this operation.
     - Returns: The DescribeBackupOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: backupNotFound, internalServer.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func describeBackup(
            input: DynamoDBModel.DescribeBackupInput) async throws -> DynamoDBModel.DescribeBackupOutput

    /**
     Invokes the DescribeContinuousBackups operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeContinuousBackupsInput object being passed to this operation.
     - Returns: The DescribeContinuousBackupsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, tableNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func describeContinuousBackups(
            input: DynamoDBModel.DescribeContinuousBackupsInput) async throws -> DynamoDBModel.DescribeContinuousBackupsOutput

    /**
     Invokes the DescribeContributorInsights operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeContributorInsightsInput object being passed to this operation.
     - Returns: The DescribeContributorInsightsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func describeContributorInsights(
            input: DynamoDBModel.DescribeContributorInsightsInput) async throws -> DynamoDBModel.DescribeContributorInsightsOutput

    /**
     Invokes the DescribeEndpoints operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeEndpointsRequest object being passed to this operation.
     - Returns: The DescribeEndpointsResponse object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func describeEndpoints(
            input: DynamoDBModel.DescribeEndpointsRequest) async throws -> DynamoDBModel.DescribeEndpointsResponse

    /**
     Invokes the DescribeExport operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeExportInput object being passed to this operation.
     - Returns: The DescribeExportOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: exportNotFound, internalServer, limitExceeded.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func describeExport(
            input: DynamoDBModel.DescribeExportInput) async throws -> DynamoDBModel.DescribeExportOutput

    /**
     Invokes the DescribeGlobalTable operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeGlobalTableInput object being passed to this operation.
     - Returns: The DescribeGlobalTableOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableNotFound, internalServer.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func describeGlobalTable(
            input: DynamoDBModel.DescribeGlobalTableInput) async throws -> DynamoDBModel.DescribeGlobalTableOutput

    /**
     Invokes the DescribeGlobalTableSettings operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeGlobalTableSettingsInput object being passed to this operation.
     - Returns: The DescribeGlobalTableSettingsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableNotFound, internalServer.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func describeGlobalTableSettings(
            input: DynamoDBModel.DescribeGlobalTableSettingsInput) async throws -> DynamoDBModel.DescribeGlobalTableSettingsOutput

    /**
     Invokes the DescribeKinesisStreamingDestination operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeKinesisStreamingDestinationInput object being passed to this operation.
     - Returns: The DescribeKinesisStreamingDestinationOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func describeKinesisStreamingDestination(
            input: DynamoDBModel.DescribeKinesisStreamingDestinationInput) async throws -> DynamoDBModel.DescribeKinesisStreamingDestinationOutput

    /**
     Invokes the DescribeLimits operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeLimitsInput object being passed to this operation.
     - Returns: The DescribeLimitsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func describeLimits(
            input: DynamoDBModel.DescribeLimitsInput) async throws -> DynamoDBModel.DescribeLimitsOutput

    /**
     Invokes the DescribeTable operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeTableInput object being passed to this operation.
     - Returns: The DescribeTableOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func describeTable(
            input: DynamoDBModel.DescribeTableInput) async throws -> DynamoDBModel.DescribeTableOutput

    /**
     Invokes the DescribeTableReplicaAutoScaling operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeTableReplicaAutoScalingInput object being passed to this operation.
     - Returns: The DescribeTableReplicaAutoScalingOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func describeTableReplicaAutoScaling(
            input: DynamoDBModel.DescribeTableReplicaAutoScalingInput) async throws -> DynamoDBModel.DescribeTableReplicaAutoScalingOutput

    /**
     Invokes the DescribeTimeToLive operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated DescribeTimeToLiveInput object being passed to this operation.
     - Returns: The DescribeTimeToLiveOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func describeTimeToLive(
            input: DynamoDBModel.DescribeTimeToLiveInput) async throws -> DynamoDBModel.DescribeTimeToLiveOutput

    /**
     Invokes the DisableKinesisStreamingDestination operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated KinesisStreamingDestinationInput object being passed to this operation.
     - Returns: The KinesisStreamingDestinationOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func disableKinesisStreamingDestination(
            input: DynamoDBModel.KinesisStreamingDestinationInput) async throws -> DynamoDBModel.KinesisStreamingDestinationOutput

    /**
     Invokes the EnableKinesisStreamingDestination operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated KinesisStreamingDestinationInput object being passed to this operation.
     - Returns: The KinesisStreamingDestinationOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func enableKinesisStreamingDestination(
            input: DynamoDBModel.KinesisStreamingDestinationInput) async throws -> DynamoDBModel.KinesisStreamingDestinationOutput

    /**
     Invokes the ExecuteStatement operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ExecuteStatementInput object being passed to this operation.
     - Returns: The ExecuteStatementOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: conditionalCheckFailed, duplicateItem, internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionConflict.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func executeStatement(
            input: DynamoDBModel.ExecuteStatementInput) async throws -> DynamoDBModel.ExecuteStatementOutput

    /**
     Invokes the ExecuteTransaction operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ExecuteTransactionInput object being passed to this operation.
     - Returns: The ExecuteTransactionOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: idempotentParameterMismatch, internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionCanceled, transactionInProgress.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func executeTransaction(
            input: DynamoDBModel.ExecuteTransactionInput) async throws -> DynamoDBModel.ExecuteTransactionOutput

    /**
     Invokes the ExportTableToPointInTime operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ExportTableToPointInTimeInput object being passed to this operation.
     - Returns: The ExportTableToPointInTimeOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: exportConflict, internalServer, invalidExportTime, limitExceeded, pointInTimeRecoveryUnavailable, tableNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func exportTableToPointInTime(
            input: DynamoDBModel.ExportTableToPointInTimeInput) async throws -> DynamoDBModel.ExportTableToPointInTimeOutput

    /**
     Invokes the GetItem operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated GetItemInput object being passed to this operation.
     - Returns: The GetItemOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func getItem(
            input: DynamoDBModel.GetItemInput) async throws -> DynamoDBModel.GetItemOutput

    /**
     Invokes the ListBackups operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ListBackupsInput object being passed to this operation.
     - Returns: The ListBackupsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func listBackups(
            input: DynamoDBModel.ListBackupsInput) async throws -> DynamoDBModel.ListBackupsOutput

    /**
     Invokes the ListContributorInsights operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ListContributorInsightsInput object being passed to this operation.
     - Returns: The ListContributorInsightsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func listContributorInsights(
            input: DynamoDBModel.ListContributorInsightsInput) async throws -> DynamoDBModel.ListContributorInsightsOutput

    /**
     Invokes the ListExports operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ListExportsInput object being passed to this operation.
     - Returns: The ListExportsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func listExports(
            input: DynamoDBModel.ListExportsInput) async throws -> DynamoDBModel.ListExportsOutput

    /**
     Invokes the ListGlobalTables operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ListGlobalTablesInput object being passed to this operation.
     - Returns: The ListGlobalTablesOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func listGlobalTables(
            input: DynamoDBModel.ListGlobalTablesInput) async throws -> DynamoDBModel.ListGlobalTablesOutput

    /**
     Invokes the ListTables operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ListTablesInput object being passed to this operation.
     - Returns: The ListTablesOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func listTables(
            input: DynamoDBModel.ListTablesInput) async throws -> DynamoDBModel.ListTablesOutput

    /**
     Invokes the ListTagsOfResource operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ListTagsOfResourceInput object being passed to this operation.
     - Returns: The ListTagsOfResourceOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func listTagsOfResource(
            input: DynamoDBModel.ListTagsOfResourceInput) async throws -> DynamoDBModel.ListTagsOfResourceOutput

    /**
     Invokes the PutItem operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated PutItemInput object being passed to this operation.
     - Returns: The PutItemOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: conditionalCheckFailed, internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionConflict.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func putItem(
            input: DynamoDBModel.PutItemInput) async throws -> DynamoDBModel.PutItemOutput

    /**
     Invokes the Query operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated QueryInput object being passed to this operation.
     - Returns: The QueryOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func query(
            input: DynamoDBModel.QueryInput) async throws -> DynamoDBModel.QueryOutput

    /**
     Invokes the RestoreTableFromBackup operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated RestoreTableFromBackupInput object being passed to this operation.
     - Returns: The RestoreTableFromBackupOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: backupInUse, backupNotFound, internalServer, limitExceeded, tableAlreadyExists, tableInUse.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func restoreTableFromBackup(
            input: DynamoDBModel.RestoreTableFromBackupInput) async throws -> DynamoDBModel.RestoreTableFromBackupOutput

    /**
     Invokes the RestoreTableToPointInTime operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated RestoreTableToPointInTimeInput object being passed to this operation.
     - Returns: The RestoreTableToPointInTimeOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, invalidRestoreTime, limitExceeded, pointInTimeRecoveryUnavailable, tableAlreadyExists, tableInUse, tableNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func restoreTableToPointInTime(
            input: DynamoDBModel.RestoreTableToPointInTimeInput) async throws -> DynamoDBModel.RestoreTableToPointInTimeOutput

    /**
     Invokes the Scan operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated ScanInput object being passed to this operation.
     - Returns: The ScanOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func scan(
            input: DynamoDBModel.ScanInput) async throws -> DynamoDBModel.ScanOutput

    /**
     Invokes the TagResource operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated TagResourceInput object being passed to this operation.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func tagResource(
            input: DynamoDBModel.TagResourceInput) async throws

    /**
     Invokes the TransactGetItems operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated TransactGetItemsInput object being passed to this operation.
     - Returns: The TransactGetItemsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionCanceled.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func transactGetItems(
            input: DynamoDBModel.TransactGetItemsInput) async throws -> DynamoDBModel.TransactGetItemsOutput

    /**
     Invokes the TransactWriteItems operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated TransactWriteItemsInput object being passed to this operation.
     - Returns: The TransactWriteItemsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: idempotentParameterMismatch, internalServer, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionCanceled, transactionInProgress.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func transactWriteItems(
            input: DynamoDBModel.TransactWriteItemsInput) async throws -> DynamoDBModel.TransactWriteItemsOutput

    /**
     Invokes the UntagResource operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UntagResourceInput object being passed to this operation.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func untagResource(
            input: DynamoDBModel.UntagResourceInput) async throws

    /**
     Invokes the UpdateContinuousBackups operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UpdateContinuousBackupsInput object being passed to this operation.
     - Returns: The UpdateContinuousBackupsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: continuousBackupsUnavailable, internalServer, tableNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func updateContinuousBackups(
            input: DynamoDBModel.UpdateContinuousBackupsInput) async throws -> DynamoDBModel.UpdateContinuousBackupsOutput

    /**
     Invokes the UpdateContributorInsights operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UpdateContributorInsightsInput object being passed to this operation.
     - Returns: The UpdateContributorInsightsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func updateContributorInsights(
            input: DynamoDBModel.UpdateContributorInsightsInput) async throws -> DynamoDBModel.UpdateContributorInsightsOutput

    /**
     Invokes the UpdateGlobalTable operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UpdateGlobalTableInput object being passed to this operation.
     - Returns: The UpdateGlobalTableOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableNotFound, internalServer, replicaAlreadyExists, replicaNotFound, tableNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func updateGlobalTable(
            input: DynamoDBModel.UpdateGlobalTableInput) async throws -> DynamoDBModel.UpdateGlobalTableOutput

    /**
     Invokes the UpdateGlobalTableSettings operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UpdateGlobalTableSettingsInput object being passed to this operation.
     - Returns: The UpdateGlobalTableSettingsOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: globalTableNotFound, indexNotFound, internalServer, limitExceeded, replicaNotFound, resourceInUse.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func updateGlobalTableSettings(
            input: DynamoDBModel.UpdateGlobalTableSettingsInput) async throws -> DynamoDBModel.UpdateGlobalTableSettingsOutput

    /**
     Invokes the UpdateItem operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UpdateItemInput object being passed to this operation.
     - Returns: The UpdateItemOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: conditionalCheckFailed, internalServer, itemCollectionSizeLimitExceeded, provisionedThroughputExceeded, requestLimitExceeded, resourceNotFound, transactionConflict.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func updateItem(
            input: DynamoDBModel.UpdateItemInput) async throws -> DynamoDBModel.UpdateItemOutput

    /**
     Invokes the UpdateTable operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UpdateTableInput object being passed to this operation.
     - Returns: The UpdateTableOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func updateTable(
            input: DynamoDBModel.UpdateTableInput) async throws -> DynamoDBModel.UpdateTableOutput

    /**
     Invokes the UpdateTableReplicaAutoScaling operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UpdateTableReplicaAutoScalingInput object being passed to this operation.
     - Returns: The UpdateTableReplicaAutoScalingOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func updateTableReplicaAutoScaling(
            input: DynamoDBModel.UpdateTableReplicaAutoScalingInput) async throws -> DynamoDBModel.UpdateTableReplicaAutoScalingOutput

    /**
     Invokes the UpdateTimeToLive operation returning aynchronously at a later time once the operation is complete.

     - Parameters:
         - input: The validated UpdateTimeToLiveInput object being passed to this operation.
     - Returns: The UpdateTimeToLiveOutput object to be passed back from the caller of this async operation.
         Will be validated before being returned to caller.
           The possible errors are: internalServer, limitExceeded, resourceInUse, resourceNotFound.
     */
    @available(macOS 9999, iOS 9999, watchOS 9999, tvOS 9999, *)
    func updateTimeToLive(
            input: DynamoDBModel.UpdateTimeToLiveInput) async throws -> DynamoDBModel.UpdateTimeToLiveOutput
    #endif
}
