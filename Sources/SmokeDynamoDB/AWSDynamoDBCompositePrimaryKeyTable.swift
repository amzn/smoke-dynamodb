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
//  AWSDynamoDBCompositePrimaryKeyTable.swift
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

public class AWSDynamoDBCompositePrimaryKeyTable<InvocationReportingType: HTTPClientCoreInvocationReporting>: DynamoDBCompositePrimaryKeyTable {
    internal let dynamodb: _AWSDynamoDBClient<InvocationReportingType>
    internal let targetTableName: String
    public let consistentRead: Bool
    public let escapeSingleQuoteInPartiQL: Bool
    internal let logger: Logger

    public init(accessKeyId: String, secretAccessKey: String,
                region: AWSRegion, reporting: InvocationReportingType,
                endpointHostName: String, endpointPort: Int = 443,
                requiresTLS: Bool? = nil, tableName: String,
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false,
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
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL

        self.logger.trace("AWSDynamoDBTable created with region '\(region)' and hostname: '\(endpointHostName)'")
    }

    public init(credentialsProvider: CredentialsProvider,
                region: AWSRegion, reporting: InvocationReportingType,
                endpointHostName: String, endpointPort: Int = 443,
                requiresTLS: Bool? = nil, tableName: String,
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false,
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
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL

        self.logger.trace("AWSDynamoDBTable created with region '\(region)' and hostname: '\(endpointHostName)'")
    }
    
    public init(config: AWSGenericDynamoDBClientConfiguration<InvocationReportingType>,
                reporting: InvocationReportingType,
                tableName: String,
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false,
                httpClient: HTTPOperationsClient? = nil) {
        self.logger = reporting.logger
        self.dynamodb = config.createAWSClient(reporting: reporting,
                                               httpClientOverride: httpClient)
        self.targetTableName = tableName
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        
        let endpointHostName = self.dynamodb.httpClient.endpointHostName
        self.logger.trace("AWSDynamoDBTable created with region '\(config.awsRegion)' and hostname: '\(endpointHostName)'")
    }
    
    // This initialiser is generic with respect to the reporting type from the operations client
    // As we are using the providing reporting instance and not creating a reporting instance from
    // the operations client, this generic type can be ignored.
    public init<OperationsClientInvocationReportingType: HTTPClientCoreInvocationReporting>(
                operationsClient: AWSGenericDynamoDBTableOperationsClient<OperationsClientInvocationReportingType>,
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false,
                reporting: InvocationReportingType) {
        self.logger = reporting.logger
        self.dynamodb = operationsClient.config.createAWSClient(reporting: reporting,
                                                                httpClientOverride: operationsClient.httpClient)
        self.targetTableName = operationsClient.tableName
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        
        let endpointHostName = self.dynamodb.httpClient.endpointHostName
        self.logger.trace("AWSDynamoDBTable created with region '\(operationsClient.config.awsRegion)' and hostname: '\(endpointHostName)'")
    }
    
    public init<TraceContextType: InvocationTraceContext>(
                config: AWSGenericDynamoDBClientConfiguration<StandardHTTPClientCoreInvocationReporting<TraceContextType>>,
                tableName: String,
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false,
                logger: Logging.Logger = Logger(label: "AWSDynamoDBTable"),
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
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        
        let endpointHostName = self.dynamodb.httpClient.endpointHostName
        self.logger.trace("AWSDynamoDBTable created with region '\(config.awsRegion)' and hostname: '\(endpointHostName)'")
    }
    
    public convenience init<TraceContextType: InvocationTraceContext, InvocationAttributesType: HTTPClientInvocationAttributes>(
        config: AWSGenericDynamoDBClientConfiguration<StandardHTTPClientCoreInvocationReporting<TraceContextType>>,
        tableName: String,
        consistentRead: Bool = true,
        escapeSingleQuoteInPartiQL: Bool = false,
        invocationAttributes: InvocationAttributesType,
        httpClient: HTTPOperationsClient? = nil)
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<TraceContextType> {
        self.init(config: config, tableName: tableName,
                  consistentRead: consistentRead,
                  logger: invocationAttributes.logger,
                  internalRequestId: invocationAttributes.internalRequestId,
                  eventLoop: !config.ignoreInvocationEventLoop ? invocationAttributes.eventLoop : nil,
                  httpClient: httpClient,
                  outwardsRequestAggregator: invocationAttributes.outwardsRequestAggregator)
    }
    
    public init<TraceContextType: InvocationTraceContext>(
                operationsClient: AWSGenericDynamoDBTableOperationsClient<StandardHTTPClientCoreInvocationReporting<TraceContextType>>,
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false,
                logger: Logging.Logger = Logger(label: "AWSDynamoDBTable"),
                internalRequestId: String = "none",
                eventLoop: EventLoop? = nil,
                outwardsRequestAggregator: OutwardsRequestAggregator? = nil)
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<TraceContextType> {
        self.logger = logger
        self.dynamodb = operationsClient.config.createAWSClient(logger: logger, internalRequestId: internalRequestId,
                                                                eventLoopOverride: eventLoop, httpClientOverride: operationsClient.httpClient,
                                                                outwardsRequestAggregator: outwardsRequestAggregator)
        self.targetTableName = operationsClient.tableName
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        
        let endpointHostName = self.dynamodb.httpClient.endpointHostName
        self.logger.trace("AWSDynamoDBTable created with region '\(operationsClient.config.awsRegion)' and hostname: '\(endpointHostName)'")
    }
    
    public convenience init<TraceContextType: InvocationTraceContext, InvocationAttributesType: HTTPClientInvocationAttributes>(
        operationsClient: AWSGenericDynamoDBTableOperationsClient<StandardHTTPClientCoreInvocationReporting<TraceContextType>>,
        consistentRead: Bool = true,
        escapeSingleQuoteInPartiQL: Bool = false,
        invocationAttributes: InvocationAttributesType)
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<TraceContextType> {
        self.init(operationsClient: operationsClient,
                  consistentRead: consistentRead,
                  escapeSingleQuoteInPartiQL: escapeSingleQuoteInPartiQL,
                  logger: invocationAttributes.logger,
                  internalRequestId: invocationAttributes.internalRequestId,
                  eventLoop: !operationsClient.config.ignoreInvocationEventLoop ? invocationAttributes.eventLoop : nil,
                  outwardsRequestAggregator: invocationAttributes.outwardsRequestAggregator)
    }
    
    public init(tableName: String,
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false,
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
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        
        let endpointHostName = self.dynamodb.httpClient.endpointHostName
        self.logger.trace("AWSDynamoDBTable created with region '\(awsRegion)' and hostname: '\(endpointHostName)'")
    }
    
    internal init(dynamodb: _AWSDynamoDBClient<InvocationReportingType>,
                  targetTableName: String,
                  consistentRead: Bool = true,
                  escapeSingleQuoteInPartiQL: Bool = false,
                  logger: Logger) {
        self.dynamodb = dynamodb
        self.targetTableName = targetTableName
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
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

extension AWSDynamoDBCompositePrimaryKeyTable {
    internal func getInputForInsert<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws
        -> DynamoDBModel.PutItemInput {
            let attributes = try getAttributes(forItem: item)

            let expressionAttributeNames = ["#pk": AttributesType.partitionKeyAttributeName, "#sk": AttributesType.sortKeyAttributeName]
            let conditionExpression = "attribute_not_exists (#pk) AND attribute_not_exists (#sk)"

            return DynamoDBModel.PutItemInput(conditionExpression: conditionExpression,
                                              expressionAttributeNames: expressionAttributeNames,
                                              item: attributes,
                                              tableName: targetTableName)
    }

    internal func getInputForUpdateItem<AttributesType, ItemType>(
            newItem: TypedDatabaseItem<AttributesType, ItemType>,
            existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws -> DynamoDBModel.PutItemInput {
        let attributes = try getAttributes(forItem: newItem)

        let expressionAttributeNames = [
            "#rowversion": RowStatus.CodingKeys.rowVersion.stringValue,
            "#createdate": TypedDatabaseItem<AttributesType, ItemType>.CodingKeys.createDate.stringValue]
        let expressionAttributeValues = [
            ":versionnumber": DynamoDBModel.AttributeValue(N: String(existingItem.rowStatus.rowVersion)),
            ":creationdate": DynamoDBModel.AttributeValue(S: existingItem.createDate.iso8601)]

        let conditionExpression = "#rowversion = :versionnumber AND #createdate = :creationdate"

        return DynamoDBModel.PutItemInput(conditionExpression: conditionExpression,
                                          expressionAttributeNames: expressionAttributeNames,
                                          expressionAttributeValues: expressionAttributeValues,
                                          item: attributes,
                                          tableName: targetTableName)
    }

    internal func getInputForGetItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws -> DynamoDBModel.GetItemInput {
        let attributeValue = try DynamoDBEncoder().encode(key)

        if let keyAttributes = attributeValue.M {
            return DynamoDBModel.GetItemInput(consistentRead: self.consistentRead,
                                              key: keyAttributes,
                                              tableName: targetTableName)
        } else {
            throw SmokeDynamoDBError.unexpectedResponse(reason: "Expected a structure.")
        }
    }
    
    internal func getInputForBatchGetItem<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) throws
    -> DynamoDBModel.BatchGetItemInput {
        let keys = try keys.map { key -> DynamoDBModel.Key in
            let attributeValue = try DynamoDBEncoder().encode(key)
            
            if let keyAttributes = attributeValue.M {
               return keyAttributes
            } else {
                throw SmokeDynamoDBError.unexpectedResponse(reason: "Expected a structure.")
            }
        }

        let keysAndAttributes = KeysAndAttributes(consistentRead: self.consistentRead,
                                                  keys: keys)
        
        return DynamoDBModel.BatchGetItemInput(requestItems: [self.targetTableName: keysAndAttributes])
    }

    internal func getInputForDeleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws -> DynamoDBModel.DeleteItemInput {
        let attributeValue = try DynamoDBEncoder().encode(key)

        if let keyAttributes = attributeValue.M {
            return DynamoDBModel.DeleteItemInput(key: keyAttributes,
                                                 tableName: targetTableName)
        } else {
            throw SmokeDynamoDBError.unexpectedResponse(reason: "Expected a structure.")
        }
    }
    
    internal func getInputForDeleteItem<AttributesType, ItemType>(
            existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws -> DynamoDBModel.DeleteItemInput {
        let attributeValue = try DynamoDBEncoder().encode(existingItem.compositePrimaryKey)
        
        guard let keyAttributes = attributeValue.M else {
            throw SmokeDynamoDBError.unexpectedResponse(reason: "Expected a structure.")
        }

        let expressionAttributeNames = [
            "#rowversion": RowStatus.CodingKeys.rowVersion.stringValue,
            "#createdate": TypedDatabaseItem<AttributesType, ItemType>.CodingKeys.createDate.stringValue]
        let expressionAttributeValues = [
            ":versionnumber": DynamoDBModel.AttributeValue(N: String(existingItem.rowStatus.rowVersion)),
            ":creationdate": DynamoDBModel.AttributeValue(S: existingItem.createDate.iso8601)]

        let conditionExpression = "#rowversion = :versionnumber AND #createdate = :creationdate"

        return DynamoDBModel.DeleteItemInput(conditionExpression: conditionExpression,
                                             expressionAttributeNames: expressionAttributeNames,
                                             expressionAttributeValues: expressionAttributeValues,
                                             key: keyAttributes,
                                             tableName: targetTableName)
    }

    public var eventLoop: EventLoop {
        return self.dynamodb.reporting.eventLoop ?? self.dynamodb.eventLoopGroup.next()
    }
}

extension Array {
    func chunked(by chunkSize: Int) -> [[Element]] {
        return stride(from: 0, to: self.count, by: chunkSize).map {
            Array(self[$0..<Swift.min($0 + chunkSize, self.count)])
        }
    }
}
