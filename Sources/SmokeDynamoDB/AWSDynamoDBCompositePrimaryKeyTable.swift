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
import AWSDynamoDB
import AWSClientRuntime
import AwsCommonRuntimeKit
import Metrics

public struct AWSDynamoDBTableMetrics {
    // metric to record if the `TransactWrite` API is retried
    let transactWriteRetryCountRecorder: Metrics.Recorder?
    
    public init(transactWriteRetryCountRecorder: Metrics.Recorder? = nil) {
        self.transactWriteRetryCountRecorder = transactWriteRetryCountRecorder
    }
}

public struct AWSDynamoDBCompositePrimaryKeyTable: DynamoDBCompositePrimaryKeyTable {
    internal let dynamodb: AWSDynamoDB.DynamoDBClient
    internal let targetTableName: String
    public let consistentRead: Bool
    public let escapeSingleQuoteInPartiQL: Bool
    public let tableMetrics: AWSDynamoDBTableMetrics
    internal let retryConfiguration: RetryConfiguration
    internal let logger: Logging.Logger
    
    public init(tableName: String, region: Swift.String,
                credentialsProvider: AWSClientRuntime.CredentialsProviding? = nil,
                connectTimeoutMs: UInt32? = nil,
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false,
                tableMetrics: AWSDynamoDBTableMetrics = .init(),
                retryConfiguration: RetryConfiguration = .default,
                logger: Logging.Logger? = nil) throws {
        self.logger = logger ?? Logging.Logger(label: "AWSDynamoDBCompositePrimaryKeyTable")
        let config = try DynamoDBClient.DynamoDBClientConfiguration(region: region,
                                                                    credentialsProvider: credentialsProvider,
                                                                    connectTimeoutMs: connectTimeoutMs)
        self.dynamodb = AWSDynamoDB.DynamoDBClient(config: config)
        self.targetTableName = tableName
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.tableMetrics = tableMetrics
        self.retryConfiguration = retryConfiguration

        self.logger.trace("AWSDynamoDBCompositePrimaryKeyTable created with region '\(region)'")
    }
    
    public init(tableName: String,
                client: AWSDynamoDB.DynamoDBClient,
                consistentRead: Bool = true,
                escapeSingleQuoteInPartiQL: Bool = false,
                tableMetrics: AWSDynamoDBTableMetrics = .init(),
                retryConfiguration: RetryConfiguration = .default,
                logger: Logging.Logger? = nil) {
        self.logger = logger ?? Logging.Logger(label: "AWSDynamoDBCompositePrimaryKeyTable")
        self.dynamodb = client
        self.targetTableName = tableName
        self.consistentRead = consistentRead
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
        self.tableMetrics = tableMetrics
        self.retryConfiguration = retryConfiguration

        self.logger.trace("AWSDynamoDBCompositePrimaryKeyTable created existing client")
    }
}

extension AWSDynamoDBCompositePrimaryKeyTable {
    internal func getInputForInsert<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws
        -> AWSDynamoDB.PutItemInput {
            let attributes = try getAttributes(forItem: item)

            let expressionAttributeNames = ["#pk": AttributesType.partitionKeyAttributeName, "#sk": AttributesType.sortKeyAttributeName]
            let conditionExpression = "attribute_not_exists (#pk) AND attribute_not_exists (#sk)"

            return AWSDynamoDB.PutItemInput(conditionExpression: conditionExpression,
                                              expressionAttributeNames: expressionAttributeNames,
                                              item: attributes,
                                              tableName: targetTableName)
    }

    internal func getInputForUpdateItem<AttributesType, ItemType>(
            newItem: TypedDatabaseItem<AttributesType, ItemType>,
            existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws -> AWSDynamoDB.PutItemInput {
        let attributes = try getAttributes(forItem: newItem)

        let expressionAttributeNames = [
            "#rowversion": RowStatus.CodingKeys.rowVersion.stringValue,
            "#createdate": TypedDatabaseItem<AttributesType, ItemType>.CodingKeys.createDate.stringValue]
        let expressionAttributeValues = [
            ":versionnumber": DynamoDBClientTypes.AttributeValue.n(String(existingItem.rowStatus.rowVersion)),
            ":creationdate": DynamoDBClientTypes.AttributeValue.s(existingItem.createDate.iso8601)]

        let conditionExpression = "#rowversion = :versionnumber AND #createdate = :creationdate"

        return AWSDynamoDB.PutItemInput(conditionExpression: conditionExpression,
                                        expressionAttributeNames: expressionAttributeNames,
                                        expressionAttributeValues: expressionAttributeValues,
                                        item: attributes,
                                          tableName: targetTableName)
    }

    internal func getInputForGetItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws -> AWSDynamoDB.GetItemInput {
        let attributeValue = try DynamoDBEncoder().encode(key)

        if case .m(let keyAttributes) = attributeValue {
            return AWSDynamoDB.GetItemInput(consistentRead: self.consistentRead,
                                              key: keyAttributes,
                                              tableName: targetTableName)
        } else {
            throw SmokeDynamoDBError.unexpectedResponse(reason: "Expected a structure.")
        }
    }
    
    internal func getInputForBatchGetItem<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) throws
    -> AWSDynamoDB.BatchGetItemInput {
        let keys = try keys.map { key -> [String: DynamoDBClientTypes.AttributeValue] in
            let attributeValue = try DynamoDBEncoder().encode(key)
            
            if case .m(let keyAttributes) = attributeValue {
               return keyAttributes
            } else {
                throw SmokeDynamoDBError.unexpectedResponse(reason: "Expected a structure.")
            }
        }

        let keysAndAttributes = DynamoDBClientTypes.KeysAndAttributes(consistentRead: self.consistentRead,
                                                                      keys: keys)
        
        return AWSDynamoDB.BatchGetItemInput(requestItems: [self.targetTableName: keysAndAttributes])
    }

    internal func getInputForDeleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws -> AWSDynamoDB.DeleteItemInput {
        let attributeValue = try DynamoDBEncoder().encode(key)

        if case .m(let keyAttributes) = attributeValue {
            return AWSDynamoDB.DeleteItemInput(key: keyAttributes,
                                                 tableName: targetTableName)
        } else {
            throw SmokeDynamoDBError.unexpectedResponse(reason: "Expected a structure.")
        }
    }
    
    internal func getInputForDeleteItem<AttributesType, ItemType>(
            existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws -> AWSDynamoDB.DeleteItemInput {
        let attributeValue = try DynamoDBEncoder().encode(existingItem.compositePrimaryKey)
        
        guard case .m(let keyAttributes) = attributeValue else {
            throw SmokeDynamoDBError.unexpectedResponse(reason: "Expected a structure.")
        }

        let expressionAttributeNames = [
            "#rowversion": RowStatus.CodingKeys.rowVersion.stringValue,
            "#createdate": TypedDatabaseItem<AttributesType, ItemType>.CodingKeys.createDate.stringValue]
        let expressionAttributeValues = [
            ":versionnumber": DynamoDBClientTypes.AttributeValue.n(String(existingItem.rowStatus.rowVersion)),
            ":creationdate": DynamoDBClientTypes.AttributeValue.s(existingItem.createDate.iso8601)]

        let conditionExpression = "#rowversion = :versionnumber AND #createdate = :creationdate"

        return AWSDynamoDB.DeleteItemInput(conditionExpression: conditionExpression,
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
