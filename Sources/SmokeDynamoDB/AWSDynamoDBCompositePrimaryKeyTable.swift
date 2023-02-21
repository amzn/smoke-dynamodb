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
import AWSCore
import AWSHttp
import SmokeHTTPClient
import ClientRuntime

public class AWSDynamoDBCompositePrimaryKeyTable<InvocationReportingType: HTTPClientCoreInvocationReporting>: DynamoDBCompositePrimaryKeyTable {
    internal let dynamodb: GenericAWSDynamoDBClient<InvocationReportingType>
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
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL

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
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false,
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
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL

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
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false,
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
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(awsRegion)' and hostname: '\(endpointHostName)'")
    }
    
    public init<TraceContextType: InvocationTraceContext, InvocationAttributesType: HTTPClientInvocationAttributes>(
        config: GenericAWSDynamoDBClientConfiguration<StandardHTTPClientCoreInvocationReporting<TraceContextType>>,
        invocationAttributes: InvocationAttributesType,
        tableName: String,
        consistentRead: Bool = true,
        escapeSingleQuoteInPartiQL: Bool = false,
        httpClient: HTTPOperationsClient? = nil)
    where InvocationReportingType == StandardHTTPClientCoreInvocationReporting<TraceContextType> {
        self.logger = invocationAttributes.logger
        self.dynamodb = GenericAWSDynamoDBClient(config: config,
                                                 invocationAttributes: invocationAttributes,
                                                 httpClient: httpClient)
        self.targetTableName = tableName
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(config.awsRegion)' and hostname: '\(config.endpointHostName)'")
    }
    
    public init<TraceContextType: InvocationTraceContext>(
        config: GenericAWSDynamoDBClientConfiguration<StandardHTTPClientCoreInvocationReporting<TraceContextType>>,
        logger: Logging.Logger = Logger(label: "DynamoDBClient"),
        internalRequestId: String = "none",
        tableName: String,
        consistentRead: Bool = true,
        escapeSingleQuoteInPartiQL: Bool = false,
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
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL

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
        self.consistentRead = tableOperationsClient.consistentRead
        self.escapeSingleQuoteInPartiQL = tableOperationsClient.escapeSingleQuoteInPartiQL

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
        self.consistentRead = tableOperationsClient.consistentRead
        self.escapeSingleQuoteInPartiQL = tableOperationsClient.escapeSingleQuoteInPartiQL

        let config = tableOperationsClient.wrappedOperationsClient.config
        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(config.awsRegion)' and hostname: '\(config.endpointHostName)'")
    }
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
}

extension Array {
    func chunked(by chunkSize: Int) -> [[Element]] {
        return stride(from: 0, to: self.count, by: chunkSize).map {
            Array(self[$0..<Swift.min($0 + chunkSize, self.count)])
        }
    }
}
