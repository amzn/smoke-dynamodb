// Copyright 2018-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import SmokeHTTPClient
import AsyncHTTPClient

public class AWSDynamoDBCompositePrimaryKeyTable<InvocationReportingType: HTTPClientCoreInvocationReporting>: DynamoDBCompositePrimaryKeyTable {
    internal let dynamodb: AWSDynamoDBClient<InvocationReportingType>
    internal let targetTableName: String
    internal let logger: Logger

    internal let defaultPaginationLimit = 100

    internal class QueryPaginationResults<AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes> {
        var items: [PolymorphicDatabaseItem<AttributesType, PossibleTypes>] = []
        var exclusiveStartKey: String?
    }
    
    internal class MonomorphicQueryPaginationResults<AttributesType: PrimaryKeyAttributes, ItemType: Codable> {
        var items: [TypedDatabaseItem<AttributesType, ItemType>] = []
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
        self.dynamodb = AWSDynamoDBClient(credentialsProvider: staticCredentials,
                                          awsRegion: region, reporting: reporting,
                                          endpointHostName: endpointHostName,
                                          endpointPort: endpointPort, requiresTLS: requiresTLS,
                                          connectionTimeoutSeconds: connectionTimeoutSeconds,
                                          retryConfiguration: retryConfiguration,
                                          eventLoopProvider: eventLoopProvider,
                                          reportingConfiguration: reportingConfiguration)
        self.targetTableName = tableName

        self.logger.info("AWSDynamoDBTable created with region '\(region)' and hostname: '\(endpointHostName)'")
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
        self.dynamodb = AWSDynamoDBClient(credentialsProvider: credentialsProvider,
                                          awsRegion: region, reporting: reporting,
                                          endpointHostName: endpointHostName,
                                          endpointPort: endpointPort, requiresTLS: requiresTLS,
                                          connectionTimeoutSeconds: connectionTimeoutSeconds,
                                          retryConfiguration: retryConfiguration,
                                          eventLoopProvider: eventLoopProvider,
                                          reportingConfiguration: reportingConfiguration)
        self.targetTableName = tableName

        self.logger.info("AWSDynamoDBTable created with region '\(region)' and hostname: '\(endpointHostName)'")
    }
    
    internal init(dynamodb: AWSDynamoDBClient<InvocationReportingType>,
                  targetTableName: String,
                  logger: Logger) {
        self.dynamodb = dynamodb
        self.targetTableName = targetTableName
        self.logger = logger
    }

    /**
     Gracefully shuts down the client behind this table. This function is idempotent and
     will handle being called multiple times.
     */
    public func close() throws {
        try dynamodb.close()
    }

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

    internal func getAttributes<AttributesType, ItemType>(forItem item: TypedDatabaseItem<AttributesType, ItemType>) throws
        -> [String: DynamoDBModel.AttributeValue] {
            let attributeValue = try DynamoDBEncoder().encode(item)

            let attributes: [String: DynamoDBModel.AttributeValue]
            if let itemAttributes = attributeValue.M {
                attributes = itemAttributes
            } else {
                throw SmokeDynamoDBError.unexpectedResponse(reason: "Expected a map.")
            }

            return attributes
    }

    internal func getInputForGetItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws -> DynamoDBModel.GetItemInput {
        let attributeValue = try DynamoDBEncoder().encode(key)

        if let keyAttributes = attributeValue.M {
            return DynamoDBModel.GetItemInput(consistentRead: true,
                                              key: keyAttributes,
                                              tableName: targetTableName)
        } else {
            throw SmokeDynamoDBError.unexpectedResponse(reason: "Expected a structure.")
        }
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
