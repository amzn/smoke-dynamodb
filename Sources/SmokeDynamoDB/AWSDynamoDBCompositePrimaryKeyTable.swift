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
import AWSMiddleware

public typealias AWSDynamoDBCompositePrimaryKeyTable = GenericAWSDynamoDBCompositePrimaryKeyTable<AWSHTTPMiddlewareStack<DynamoDBError>>

public class GenericAWSDynamoDBCompositePrimaryKeyTable<MiddlewareStackType: AWSHTTPMiddlewareStackProtocol>: DynamoDBCompositePrimaryKeyTable {
    public let dynamodb: GenericAWSDynamoDBClientV2<MiddlewareStackType>
    public let targetTableName: String
    public let consistentRead: Bool
    public let escapeSingleQuoteInPartiQL: Bool
    public let logger: Logger
    
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
                retryConfiguration: HTTPClientRetryConfiguration = .default) throws {
        self.logger = logger
        self.dynamodb = try GenericAWSDynamoDBClientV2<MiddlewareStackType>(
            credentialsProvider: credentialsProvider, awsRegion: awsRegion,
            endpointHostName: endpointHostName, endpointPort: endpointPort,
            requiresTLS: requiresTLS, service: service,
            contentType: contentType, target: target, logger: logger,
            retryConfiguration: retryConfiguration)
        self.targetTableName = tableName
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(awsRegion)' and hostname: '\(endpointHostName)'")
    }
    
    public init(config: AWSDynamoDBClientConfiguration,
                logger: Logging.Logger = Logger(label: "DynamoDBClient"),
                internalRequestId: String = "none",
                tableName: String,
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false) throws {
        self.logger = logger
        self.dynamodb = try config.getAWSClient(logger: logger)
        self.targetTableName = tableName
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(config.awsRegion)' and hostname: '\(config.endpointHostName)'")
    }
    
    public init(operationsClient: AWSDynamoDBTableOperationsClient,
                logger: Logging.Logger = Logger(label: "DynamoDBClient"),
                internalRequestId: String = "none") throws {
        let config = operationsClient.config
        
        self.logger = logger
        self.dynamodb = try config.getAWSClient(logger: logger,
                                                httpClientEngine: operationsClient.httpClientEngine)
        self.targetTableName = operationsClient.tableName
        self.consistentRead = operationsClient.consistentRead
        self.escapeSingleQuoteInPartiQL = operationsClient.escapeSingleQuoteInPartiQL

        self.logger.trace("AWSDynamoDBCompositePrimaryKeysProjection created with region '\(config.awsRegion)' and hostname: '\(config.endpointHostName)'")
    }
}

extension GenericAWSDynamoDBCompositePrimaryKeyTable {
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
