// Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
//  AWSDynamoDBTable+commonFunctions.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeAWSCore
import DynamoDBModel
import SmokeHTTPClient
import LoggerAPI

/// DynamoDBTable conformance sync functions
public extension AWSDynamoDBTable {
    internal func getInputForInsert<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws
        -> DynamoDBModel.PutItemInput {
            let attributes = try getAttributes(forItem: item)

            let expressionAttributeNames = ["#pk": AttributesType.paritionKeyAttributeName, "#sk": AttributesType.sortKeyAttributeName]
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
        
        let expressionAttributeNames = ["#rowversion": RowStatus.CodingKeys.rowVersion.stringValue]
        let expressionAttributeValues = [":versionnumber": DynamoDBModel.AttributeValue(N: String(existingItem.rowStatus.rowVersion))]
        
        let conditionExpression = "#rowversion = :versionnumber"
        
        return DynamoDBModel.PutItemInput(conditionExpression: conditionExpression,
                                                      expressionAttributeNames: expressionAttributeNames,
                                                      expressionAttributeValues: expressionAttributeValues,
                                                      item: attributes,
                                                      tableName: targetTableName)
    }
    
    internal func getAttributes<AttributesType, ItemType>(forItem item: TypedDatabaseItem<AttributesType, ItemType>) throws
        -> [String: DynamoDBModel.AttributeValue] {
            let attributeValue = try AWSDynamoDBTable.dynamodbEncoder.encode(item)
            
            let attributes: [String: DynamoDBModel.AttributeValue]
            if let itemAttributes = attributeValue.M {
                attributes = itemAttributes
            } else {
                throw SmokeDynamoDBError.databaseError(reason: "Expected a map.")
            }
            
            return attributes
    }
    
    internal func getInputForGetItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws -> DynamoDBModel.GetItemInput {
        let attributeValue = try AWSDynamoDBTable.dynamodbEncoder.encode(key)
        
        if let keyAttributes = attributeValue.M {
            return DynamoDBModel.GetItemInput(consistentRead: true,
                                              key: keyAttributes,
                                              tableName: targetTableName)
        } else {
            throw SmokeDynamoDBError.databaseError(reason: "Expected a structure.")
        }
    }
    
    internal func getInputForDeleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws -> DynamoDBModel.DeleteItemInput {
        let attributeValue = try AWSDynamoDBTable.dynamodbEncoder.encode(key)
        
        if let keyAttributes = attributeValue.M {
            return DynamoDBModel.DeleteItemInput(key: keyAttributes,
                                                 tableName: targetTableName)
        } else {
            throw SmokeDynamoDBError.databaseError(reason: "Expected a structure.")
        }
    }
    
    internal func getQueryInput<AttributesType>(forPartitionKey partitionKey: String,
                                                primaryKeyType: AttributesType.Type,
                                                sortKeyCondition: AttributeCondition?,
                                                limit: Int,
                                                exclusiveStartKey: String?) throws
        -> DynamoDBModel.QueryInput where AttributesType: PrimaryKeyAttributes {
        let expressionAttributeValues: [String: DynamoDBModel.AttributeValue]
        let expressionAttributeNames: [String: String]
        let keyConditionExpression: String
        if let currentSortKeyCondition = sortKeyCondition {
            var withSortConditionAttributeValues: [String: DynamoDBModel.AttributeValue] = [
                ":pk": DynamoDBModel.AttributeValue(S: partitionKey)]
            
            let sortKeyExpression: String
            switch currentSortKeyCondition {
            case .equals(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue(S: value)
                sortKeyExpression = "#sk = :sortkeyval"
            case .lessThan(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue(S: value)
                sortKeyExpression = "#sk < :sortkeyval"
            case .lessThanOrEqual(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue(S: value)
                sortKeyExpression = "#sk <= :sortkeyval"
            case .greaterThan(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue(S: value)
                sortKeyExpression = "#sk > :sortkeyval"
            case .greaterThanOrEqual(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue(S: value)
                sortKeyExpression = "#sk >= :sortkeyval"
            case .between(let value1, let value2):
                withSortConditionAttributeValues[":sortkeyval1"] = DynamoDBModel.AttributeValue(S: value1)
                withSortConditionAttributeValues[":sortkeyval2"] = DynamoDBModel.AttributeValue(S: value2)
                sortKeyExpression = "#sk BETWEEN :sortkeyval1 AND :sortkeyval2"
            case .beginsWith(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBModel.AttributeValue(S: value)
                sortKeyExpression = "begins_with ( #sk, :sortkeyval )"
            }
            
            keyConditionExpression = "#pk= :pk AND \(sortKeyExpression)"
            
            expressionAttributeNames = ["#pk": AttributesType.paritionKeyAttributeName,
                                        "#sk": AttributesType.sortKeyAttributeName]
            expressionAttributeValues = withSortConditionAttributeValues
        } else {
            keyConditionExpression = "#pk= :pk"
            
            expressionAttributeNames = ["#pk": AttributesType.paritionKeyAttributeName]
            expressionAttributeValues = [":pk": DynamoDBModel.AttributeValue(S: partitionKey)]
        }
            
        let inputExclusiveStartKey: [String: DynamoDBModel.AttributeValue]?
        if let exclusiveStartKey = exclusiveStartKey?.data(using: .utf8) {
            inputExclusiveStartKey = try AWSDynamoDBTable.jsonDecoder.decode([String: DynamoDBModel.AttributeValue].self,
                                                            from: exclusiveStartKey)
        } else {
            inputExclusiveStartKey = nil
        }
    
        return DynamoDBModel.QueryInput(exclusiveStartKey: inputExclusiveStartKey,
                                        expressionAttributeNames: expressionAttributeNames,
                                        expressionAttributeValues: expressionAttributeValues,
                                        keyConditionExpression: keyConditionExpression,
                                        limit: limit,
                                        tableName: targetTableName)
    }
}
