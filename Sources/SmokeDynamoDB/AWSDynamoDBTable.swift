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
//  AWSDynamoDBTable.swift
//  SmokeDynamoDB
//

import Foundation
import LoggerAPI
import DynamoDBClient
import DynamoDBModel
import SmokeAWSCore
import SmokeHTTPClient

private let dynamodbEncoder = DynamoDBEncoder()
private let dynamodbDecoder = DynamoDBDecoder()
private let jsonEncoder = JSONEncoder()
private let jsonDecoder = JSONDecoder()

public class AWSDynamoDBTable: DynamoDBTable {
    private let dynamodb: AWSDynamoDBClient
    private let targetTableName: String
    
    private let defaultPaginationLimit = 100
    
    private class QueryPaginationResults<AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes> {
        var items: [PolymorphicDatabaseItem<AttributesType, PossibleTypes>] = []
        var exclusiveStartKey: String?
    }
    
    public init(accessKeyId: String, secretAccessKey: String,
                region: AWSRegion, endpointHostName: String,
                tableName: String) {
        let staticCredentials = StaticCredentials(accessKeyId: accessKeyId,
                                                  secretAccessKey: secretAccessKey,
                                                  sessionToken: nil)
        
        self.dynamodb = AWSDynamoDBClient(credentialsProvider: staticCredentials,
                                          awsRegion: region,
                                          endpointHostName: endpointHostName)
        self.targetTableName = tableName
        
        Log.info("AWSDynamoDBTable created with region '\(region)' and hostname: '\(endpointHostName)'")
    }
    
    public init(credentialsProvider: CredentialsProvider,
                region: AWSRegion, endpointHostName: String,
                tableName: String) {
        self.dynamodb = AWSDynamoDBClient(credentialsProvider: credentialsProvider,
                                          awsRegion: region,
                                          endpointHostName: endpointHostName)
        self.targetTableName = tableName
        
        Log.info("AWSDynamoDBTable created with region '\(region)' and hostname: '\(endpointHostName)'")
    }
    
    private func getInputForInsert<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws
        -> DynamoDBModel.PutItemInput {
            let attributes = try getAttributes(forItem: item)

            let expressionAttributeNames = ["#pk": AttributesType.paritionKeyAttributeName, "#sk": AttributesType.sortKeyAttributeName]
            let conditionExpression = "attribute_not_exists (#pk) AND attribute_not_exists (#sk)"
            
            return DynamoDBModel.PutItemInput(conditionExpression: conditionExpression,
                                              expressionAttributeNames: expressionAttributeNames,
                                              item: attributes,
                                              tableName: targetTableName)
    }
    
    public func insertItemSync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws {
        let putItemInput = try getInputForInsert(item)
        
        try putItemSync(forInput: putItemInput, withKey: item.compositePrimaryKey)
    }
    
    public func insertItemAsync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                          completion: @escaping (Error?) -> ())
        throws where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            let putItemInput = try getInputForInsert(item)
        
            try putItemAsync(forInput: putItemInput, withKey: item.compositePrimaryKey,
                             completion: completion)
    }
    
    public func clobberItemSync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws {
        let attributes = try getAttributes(forItem: item)
        
        let putItemInput = DynamoDBModel.PutItemInput(item: attributes,
                                                      tableName: targetTableName)
        
        try putItemSync(forInput: putItemInput, withKey: item.compositePrimaryKey)
    }
    
    public func clobberItemAsync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                           completion: @escaping (Error?) -> ())
        throws where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            let attributes = try getAttributes(forItem: item)
        
            let putItemInput = DynamoDBModel.PutItemInput(item: attributes,
                                                      tableName: targetTableName)
        
            try putItemAsync(forInput: putItemInput, withKey: item.compositePrimaryKey, completion: completion)
    }
    
    private func getInputForUpdateItem<AttributesType, ItemType>(
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
    
    public func updateItemSync<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                         existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws {
        let putItemInput = try getInputForUpdateItem(newItem: newItem, existingItem: existingItem)
        
        try putItemSync(forInput: putItemInput, withKey: newItem.compositePrimaryKey)
    }
    
    public func updateItemAsync<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                          existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                          completion: @escaping (Error?) -> ())
        throws where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            let putItemInput = try getInputForUpdateItem(newItem: newItem, existingItem: existingItem)
        
            try putItemAsync(forInput: putItemInput, withKey: newItem.compositePrimaryKey, completion: completion)
    }
    
    private func getInputForGetItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws -> DynamoDBModel.GetItemInput {
        let attributeValue = try dynamodbEncoder.encode(key)
        
        if let keyAttributes = attributeValue.M {
            return DynamoDBModel.GetItemInput(consistentRead: true,
                                              key: keyAttributes,
                                              tableName: targetTableName)
        } else {
            throw SmokeDynamoDBError.databaseError(reason: "Expected a structure.")
        }
    }
    
    public func getItemSync<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) throws
        -> TypedDatabaseItem<AttributesType, ItemType>? {
            let putItemInput = try getInputForGetItem(forKey: key)
            
            Log.verbose("dynamodb.getItem with key: \(key) and table name \(targetTableName)")
            let attributeValue = try dynamodb.getItemSync(input: putItemInput)
            
            if let item = attributeValue.item {
                Log.verbose("Value returned from DynamoDB.")
                
                return try dynamodbDecoder.decode(DynamoDBModel.AttributeValue(M: item))
            } else {
                Log.verbose("No item returned from DynamoDB.")
                
                return nil
            }
    }
    
    public func getItemAsync<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>,
                                                       completion: @escaping (HTTPResult<TypedDatabaseItem<AttributesType, ItemType>?>) -> ())
        throws where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            let putItemInput = try getInputForGetItem(forKey: key)
            
            Log.verbose("dynamodb.getItem with key: \(key) and table name \(targetTableName)")
            try dynamodb.getItemAsync(input: putItemInput) { result in
                switch result {
                case .response(let attributeValue):
                    if let item = attributeValue.item {
                        Log.verbose("Value returned from DynamoDB.")
                        
                        do {
                            let decodedItem: TypedDatabaseItem<AttributesType, ItemType>? =
                                try dynamodbDecoder.decode(DynamoDBModel.AttributeValue(M: item))
                            completion(.response(decodedItem))
                        } catch {
                            completion(.error(error))
                        }
                    } else {
                        Log.verbose("No item returned from DynamoDB.")
                        
                        completion(.response(nil))
                    }
                case .error(let error):
                    completion(.error(error))
                }
            }
    }
    
    private func getInputForDeleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws -> DynamoDBModel.DeleteItemInput {
        let attributeValue = try dynamodbEncoder.encode(key)
        
        if let keyAttributes = attributeValue.M {
            return DynamoDBModel.DeleteItemInput(key: keyAttributes,
                                                 tableName: targetTableName)
        } else {
            throw SmokeDynamoDBError.databaseError(reason: "Expected a structure.")
        }
    }
    
    public func deleteItemSync<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws {
        let deleteItemInput = try getInputForDeleteItem(forKey: key)
        
        Log.verbose("dynamodb.deleteItem with key: \(key) and table name \(targetTableName)")
        _ = try dynamodb.deleteItemSync(input: deleteItemInput)
    }
    
    public func deleteItemAsync<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>,
                                                completion: @escaping (Error?) -> ())
        throws where AttributesType: PrimaryKeyAttributes {
            let deleteItemInput = try getInputForDeleteItem(forKey: key)
        
            Log.verbose("dynamodb.deleteItem with key: \(key) and table name \(targetTableName)")
            try dynamodb.deleteItemAsync(input: deleteItemInput) { result in
                switch result {
                case .response:
                    // complete the putItem
                    completion(nil)
                case .error(let error):
                    completion(error)
                }
            }
    }
    
    public func querySync<AttributesType, PossibleTypes>(forPartitionKey partitionKey: String,
                                                         sortKeyCondition: AttributeCondition?) throws
        -> [PolymorphicDatabaseItem<AttributesType, PossibleTypes>] {
          
        var items: [PolymorphicDatabaseItem<AttributesType, PossibleTypes>] = []
        var exclusiveStartKey: String?
            
        while true {
            let paginatedItems: ([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?) =
                try querySync(forPartitionKey: partitionKey,
                          sortKeyCondition: sortKeyCondition,
                          limit: defaultPaginationLimit,
                          exclusiveStartKey: exclusiveStartKey)
            
            items += paginatedItems.0
            
            // if there are more items
            if let lastEvaluatedKey = paginatedItems.1 {
                exclusiveStartKey = lastEvaluatedKey
            } else {
                // we have all the items
                return items
            }
        }
    }
    
    public func queryAsync<AttributesType, PossibleTypes>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            completion: @escaping (HTTPResult<[PolymorphicDatabaseItem<AttributesType, PossibleTypes>]>) -> ())
        throws where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            let partialResults = QueryPaginationResults<AttributesType, PossibleTypes>()
            
            try partialQueryAsync(forPartitionKey: partitionKey,
                                  sortKeyCondition: sortKeyCondition,
                                  partialResults: partialResults,
                                  completion: completion)
    }
    
    private func partialQueryAsync<AttributesType, PossibleTypes>(
        forPartitionKey partitionKey: String,
        sortKeyCondition: AttributeCondition?,
        partialResults: QueryPaginationResults<AttributesType, PossibleTypes>,
        completion: @escaping (HTTPResult<[PolymorphicDatabaseItem<AttributesType, PossibleTypes>]>) -> ())
        throws where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            func handleQueryResult(result: HTTPResult<([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)>) {
                switch result {
                case .response(let paginatedItems):
                    partialResults.items += paginatedItems.0
            
                    // if there are more items
                    if let lastEvaluatedKey = paginatedItems.1 {
                        partialResults.exclusiveStartKey = lastEvaluatedKey
                        
                        do {
                            try partialQueryAsync(forPartitionKey: partitionKey,
                                                  sortKeyCondition: sortKeyCondition,
                                                  partialResults: partialResults,
                                                  completion: completion)
                        } catch {
                            completion(.error(error))
                        }
                    } else {
                        // we have all the items
                        completion(.response(partialResults.items))
                    }
                case .error(let error):
                    completion(.error(error))
                }
            }
            
            try queryAsync(forPartitionKey: partitionKey,
                          sortKeyCondition: sortKeyCondition,
                          limit: defaultPaginationLimit,
                          exclusiveStartKey: partialResults.exclusiveStartKey,
                          completion: handleQueryResult)
    }
    
    private func getQueryInput<AttributesType>(forPartitionKey partitionKey: String,
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
            inputExclusiveStartKey = try jsonDecoder.decode([String: DynamoDBModel.AttributeValue].self,
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
    
    public func querySync<AttributesType, PossibleTypes>(forPartitionKey partitionKey: String,
                                                         sortKeyCondition: AttributeCondition?,
                                                         limit: Int,
                                                         exclusiveStartKey: String?) throws
        -> ([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)
        where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            let queryInput = try getQueryInput(forPartitionKey: partitionKey, primaryKeyType: AttributesType.self,
                                           sortKeyCondition: sortKeyCondition, limit: limit,
                                           exclusiveStartKey: exclusiveStartKey)
            let queryOutput = try dynamodb.querySync(input: queryInput)
            
            let lastEvaluatedKey: String?
            if let returnedLastEvaluatedKey = queryOutput.lastEvaluatedKey {
                let encodedLastEvaluatedKey = try jsonEncoder.encode(returnedLastEvaluatedKey)
                
                lastEvaluatedKey = String(data: encodedLastEvaluatedKey, encoding: .utf8)
            } else {
                lastEvaluatedKey = nil
            }
            
            if let outputAttributeValues = queryOutput.items {
                let items: [PolymorphicDatabaseItem<AttributesType, PossibleTypes>] = try outputAttributeValues.map { values in
                    let attributeValue = DynamoDBModel.AttributeValue(M: values)
                    
                    return try dynamodbDecoder.decode(attributeValue)
                }
                
                return (items, lastEvaluatedKey)
            } else {
                return ([], lastEvaluatedKey)
            }
    }
    
    public func queryAsync<AttributesType, PossibleTypes>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int, exclusiveStartKey: String?,
            completion: @escaping (HTTPResult<([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)>) -> ())
        throws where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            let queryInput = try getQueryInput(forPartitionKey: partitionKey, primaryKeyType: AttributesType.self,
                                           sortKeyCondition: sortKeyCondition, limit: limit,
                                           exclusiveStartKey: exclusiveStartKey)
            try dynamodb.queryAsync(input: queryInput) { result in
                switch result {
                case .response(let queryOutput):
                    let lastEvaluatedKey: String?
                    if let returnedLastEvaluatedKey = queryOutput.lastEvaluatedKey {
                        let encodedLastEvaluatedKey: Data
                        
                        do {
                            encodedLastEvaluatedKey = try jsonEncoder.encode(returnedLastEvaluatedKey)
                        } catch {
                            return completion(.error(error))
                        }
                        
                        lastEvaluatedKey = String(data: encodedLastEvaluatedKey, encoding: .utf8)
                    } else {
                        lastEvaluatedKey = nil
                    }
                    
                    if let outputAttributeValues = queryOutput.items {
                        let items: [PolymorphicDatabaseItem<AttributesType, PossibleTypes>]
                        
                        do {
                            items = try outputAttributeValues.map { values in
                                let attributeValue = DynamoDBModel.AttributeValue(M: values)
                                
                                return try dynamodbDecoder.decode(attributeValue)
                            }
                        } catch {
                            return completion(.error(error))
                        }
                        
                        completion(.response((items, lastEvaluatedKey)))
                    } else {
                        completion(.response(([], lastEvaluatedKey)))
                    }
                case .error(let error):
                    return completion(.error(error))
                }
            }
    }
    
    private func getAttributes<AttributesType, ItemType>(forItem item: TypedDatabaseItem<AttributesType, ItemType>) throws
        -> [String: DynamoDBModel.AttributeValue] {
            let attributeValue = try dynamodbEncoder.encode(item)
            
            let attributes: [String: DynamoDBModel.AttributeValue]
            if let itemAttributes = attributeValue.M {
                attributes = itemAttributes
            } else {
                throw SmokeDynamoDBError.databaseError(reason: "Expected a map.")
            }
            
            return attributes
    }
    
    private func putItemSync<AttributesType>(forInput putItemInput: DynamoDBModel.PutItemInput,
                                             withKey compositePrimaryKey: CompositePrimaryKey<AttributesType>) throws {
        do {
            _ = try dynamodb.putItemSync(input: putItemInput)
        } catch DynamoDBError.conditionalCheckFailed(let errorPayload) {
            throw SmokeDynamoDBError.conditionalCheckFailed(paritionKey: compositePrimaryKey.partitionKey,
                                                          sortKey: compositePrimaryKey.sortKey,
                                                          message: errorPayload.message)
        } catch {
            Log.warning("Error from AWSDynamoDBTable: \(error)")
            
            throw SmokeDynamoDBError.databaseError(reason: "\(error)")
        }
    }
    
    private func putItemAsync<AttributesType>(forInput putItemInput: DynamoDBModel.PutItemInput,
                                              withKey compositePrimaryKey: CompositePrimaryKey<AttributesType>,
                                              completion: @escaping (Error?) -> ()) throws {
        do {
            _ = try dynamodb.putItemAsync(input: putItemInput) { result in
                switch result {
                case .response:
                    // complete the putItem
                    completion(nil)
                case .error(let error):
                    switch error {
                    case DynamoDBError.conditionalCheckFailed(let errorPayload):
                        completion(SmokeDynamoDBError.conditionalCheckFailed(paritionKey: compositePrimaryKey.partitionKey,
                                                                           sortKey: compositePrimaryKey.sortKey,
                                                                           message: errorPayload.message))
                    default:
                        Log.warning("Error from AWSDynamoDBTable: \(error)")
            
                        completion(SmokeDynamoDBError.databaseError(reason: "\(error)"))
                    }
                }
            }
        } catch {
            Log.warning("Error from AWSDynamoDBTable: \(error)")
            
            throw SmokeDynamoDBError.databaseError(reason: "\(error)")
        }
    }
}
