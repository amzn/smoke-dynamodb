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
//  QueryInput+forSortKeyCondition.swift
//  SmokeDynamoDB
//

import Foundation
import AWSDynamoDB

extension QueryInput {
        internal static func forSortKeyCondition<AttributesType>(partitionKey: String,
                                                                 targetTableName: String,
                                                                 primaryKeyType: AttributesType.Type,
                                                                 sortKeyCondition: AttributeCondition?,
                                                                 limit: Int?,
                                                                 scanIndexForward: Bool,
                                                                 exclusiveStartKey: String?,
                                                                 consistentRead: Bool?) throws
        -> AWSDynamoDB.QueryInput where AttributesType: PrimaryKeyAttributes {
        let expressionAttributeValues: [String: DynamoDBClientTypes.AttributeValue]
        let expressionAttributeNames: [String: String]
        let keyConditionExpression: String
        if let currentSortKeyCondition = sortKeyCondition {
            var withSortConditionAttributeValues: [String: DynamoDBClientTypes.AttributeValue] = [
                ":pk": DynamoDBClientTypes.AttributeValue.s(partitionKey)]

            let sortKeyExpression: String
            switch currentSortKeyCondition {
            case .equals(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBClientTypes.AttributeValue.s(value)
                sortKeyExpression = "#sk = :sortkeyval"
            case .lessThan(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBClientTypes.AttributeValue.s(value)
                sortKeyExpression = "#sk < :sortkeyval"
            case .lessThanOrEqual(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBClientTypes.AttributeValue.s(value)
                sortKeyExpression = "#sk <= :sortkeyval"
            case .greaterThan(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBClientTypes.AttributeValue.s(value)
                sortKeyExpression = "#sk > :sortkeyval"
            case .greaterThanOrEqual(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBClientTypes.AttributeValue.s(value)
                sortKeyExpression = "#sk >= :sortkeyval"
            case .between(let value1, let value2):
                withSortConditionAttributeValues[":sortkeyval1"] = DynamoDBClientTypes.AttributeValue.s(value1)
                withSortConditionAttributeValues[":sortkeyval2"] = DynamoDBClientTypes.AttributeValue.s(value2)
                sortKeyExpression = "#sk BETWEEN :sortkeyval1 AND :sortkeyval2"
            case .beginsWith(let value):
                withSortConditionAttributeValues[":sortkeyval"] = DynamoDBClientTypes.AttributeValue.s(value)
                sortKeyExpression = "begins_with ( #sk, :sortkeyval )"
            }

            keyConditionExpression = "#pk= :pk AND \(sortKeyExpression)"

            expressionAttributeNames = ["#pk": AttributesType.partitionKeyAttributeName,
                                        "#sk": AttributesType.sortKeyAttributeName]
            expressionAttributeValues = withSortConditionAttributeValues
        } else {
            keyConditionExpression = "#pk= :pk"

            expressionAttributeNames = ["#pk": AttributesType.partitionKeyAttributeName]
            expressionAttributeValues = [":pk": DynamoDBClientTypes.AttributeValue.s(partitionKey)]
        }

        let inputExclusiveStartKey: [String: DynamoDBClientTypes.AttributeValue]?
        if let exclusiveStartKey = exclusiveStartKey?.data(using: .utf8) {
            inputExclusiveStartKey = try JSONDecoder().decode([String: DynamoDBClientTypes.AttributeValue].self,
                                                              from: exclusiveStartKey)
        } else {
            inputExclusiveStartKey = nil
        }

        return AWSDynamoDB.QueryInput(consistentRead: consistentRead,
                                      exclusiveStartKey: inputExclusiveStartKey,
                                      expressionAttributeNames: expressionAttributeNames,
                                      expressionAttributeValues: expressionAttributeValues,
                                      indexName: primaryKeyType.indexName,
                                      keyConditionExpression: keyConditionExpression,
                                      limit: limit,
                                      scanIndexForward: scanIndexForward,
                                      tableName: targetTableName)
    }
}
