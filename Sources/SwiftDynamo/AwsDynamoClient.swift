//
//  AwsDynamoClient.swift
//  SwiftDynamo
//

import Foundation
import LoggerAPI
import DynamoDBClient
import DynamoDBModel
import SwiftAWSCore

private let dynamoEncoder = DynamoEncoder()
private let dynamoDecoder = DynamoDecoder()
private let jsonEncoder = JSONEncoder()
private let jsonDecoder = JSONDecoder()

private let DEFAULT_PAGINATION_LIMIT = 100
private let HTTP_ENDPOINT_PREFIX = "https://"

public class AwsDynamoClient: DynamoClient {
    private let dynamodb: AWSDynamoDBClient
    private let targetTableName: String
    
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
        
        Log.info("AwsDynamoClient created with region '\(region)' and hostname: '\(endpointHostName)'")
    }
    
    public init(credentialsProvider: CredentialsProvider,
                region: AWSRegion, endpointHostName: String,
                playbackObjectsTableName: String) {
        self.dynamodb = AWSDynamoDBClient(credentialsProvider: credentialsProvider,
                                          awsRegion: region,
                                          endpointHostName: endpointHostName)
        self.targetTableName = playbackObjectsTableName
        
        Log.info("AwsDynamoClient created with region '\(region)' and hostname: '\(endpointHostName)'")
    }
    
    
    public func insertItem<RowIdentity, ItemType>(_ item: TypedDatabaseItem<RowIdentity, ItemType>) throws {
        let attributes = try getAttributes(forItem: item)

        let expressionAttributeNames = ["#pk": RowIdentity.paritionKeyAttributeName, "#sk": RowIdentity.sortKeyAttributeName]
        let conditionExpression = "attribute_not_exists (#pk) AND attribute_not_exists (#sk)"
        
        let putItemInput = DynamoDBModel.PutItemInput(conditionExpression: conditionExpression,
                                                      expressionAttributeNames: expressionAttributeNames,
                                                      item: attributes,
                                                      tableName: targetTableName)
        
        try putItem(forInput: putItemInput, withKey: item.compositePrimaryKey)

    }
    
    public func clobberItem<RowIdentity, ItemType>(_ item: TypedDatabaseItem<RowIdentity, ItemType>) throws {
        let attributes = try getAttributes(forItem: item)
        
        let putItemInput = DynamoDBModel.PutItemInput(item: attributes,
                                                      tableName: targetTableName)
        
        try putItem(forInput: putItemInput, withKey: item.compositePrimaryKey)
    }
    
    public func updateItem<RowIdentity, ItemType>(newItem: TypedDatabaseItem<RowIdentity, ItemType>, existingItem: TypedDatabaseItem<RowIdentity, ItemType>) throws {
        let attributes = try getAttributes(forItem: newItem)
        
        let expressionAttributeNames = ["#rowversion": RowStatus.CodingKeys.rowVersion.stringValue]
        let expressionAttributeValues = [":versionnumber": DynamoDBModel.AttributeValue(N: String(existingItem.rowStatus.rowVersion))]
        
        let conditionExpression = "#rowversion = :versionnumber"
        
        let putItemInput = DynamoDBModel.PutItemInput(conditionExpression: conditionExpression,
                                                      expressionAttributeNames: expressionAttributeNames,
                                                      expressionAttributeValues: expressionAttributeValues,
                                                      item: attributes,
                                                      tableName: targetTableName)
        
        try putItem(forInput: putItemInput, withKey: newItem.compositePrimaryKey)
    }
    
    public func getItem<RowIdentity, ItemType>(forKey key: CompositePrimaryKey<RowIdentity>) throws -> TypedDatabaseItem<RowIdentity, ItemType>? {
        let attributeValue = try dynamoEncoder.encode(key)
        
        if let keyAttributes = attributeValue.M {
            let putItemInput = DynamoDBModel.GetItemInput(consistentRead: true,
                                                          key: keyAttributes,
                                                          tableName: targetTableName)
        
            Log.verbose("dynamodb.getItem with key: \(key) and table name \(targetTableName)")
            let attributeValue = try dynamodb.getItemSync(input: putItemInput)
            
            if let item = attributeValue.item {
                Log.verbose("Value returned from dynamodb.")
                
                return try dynamoDecoder.decode(DynamoDBModel.AttributeValue(M: item))
            } else {
                Log.verbose("No item returned from dynamodb.")
                
                return nil
            }
        } else {
            throw SwiftDynamoError.databaseError(reason: "Expected a structure.")
        }
    }
    
    public func deleteItem<RowIdentity>(forKey key: CompositePrimaryKey<RowIdentity>) throws {
        let attributeValue = try dynamoEncoder.encode(key)
        
        if let keyAttributes = attributeValue.M {
            let deleteItemInput = DynamoDBModel.DeleteItemInput(key: keyAttributes,
                                                                tableName: targetTableName)
        
            Log.verbose("dynamodb.deleteItem with key: \(key) and table name \(targetTableName)")
            _ = try dynamodb.deleteItemSync(input: deleteItemInput)
        } else {
            throw SwiftDynamoError.databaseError(reason: "Expected a structure.")
        }
    }
    
    public func query<RowIdentity, PossibleTypes>(forPartitionKey partitionKey: String,
                                                  sortKeyCondition: AttributeCondition?) throws
        -> [PolymorphicDatabaseItem<RowIdentity, PossibleTypes>] {
          
        var items: [PolymorphicDatabaseItem<RowIdentity, PossibleTypes>] = []
        var exclusiveStartKey: String?
            
        while true {
            let paginatedItems: ([PolymorphicDatabaseItem<RowIdentity, PossibleTypes>], String?) =
                try query(forPartitionKey: partitionKey,
                          sortKeyCondition: sortKeyCondition,
                          limit: DEFAULT_PAGINATION_LIMIT,
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
    
    public func query<RowIdentity, PossibleTypes>(forPartitionKey partitionKey: String,
                                                  sortKeyCondition: AttributeCondition?,
                                                  limit: Int,
                                                  exclusiveStartKey: String?) throws
        -> ([PolymorphicDatabaseItem<RowIdentity, PossibleTypes>], String?)
        where RowIdentity : DynamoRowIdentity, PossibleTypes : PossibleItemTypes {
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
            
            expressionAttributeNames = ["#pk": RowIdentity.paritionKeyAttributeName,
                                        "#sk": RowIdentity.sortKeyAttributeName]
            expressionAttributeValues = withSortConditionAttributeValues
        } else {
            keyConditionExpression = "#pk= :pk"
            
            expressionAttributeNames = ["#pk": RowIdentity.paritionKeyAttributeName]
            expressionAttributeValues = [":pk": DynamoDBModel.AttributeValue(S: partitionKey)]
        }
            
        let inputExclusiveStartKey: [String: DynamoDBModel.AttributeValue]?
        if let exclusiveStartKey = exclusiveStartKey?.data(using: .utf8) {
            inputExclusiveStartKey = try jsonDecoder.decode([String: DynamoDBModel.AttributeValue].self,
                                                            from: exclusiveStartKey)
        } else {
            inputExclusiveStartKey = nil
        }
    
        let queryInput = DynamoDBModel.QueryInput(exclusiveStartKey: inputExclusiveStartKey,
                                                  expressionAttributeNames: expressionAttributeNames,
                                                  expressionAttributeValues: expressionAttributeValues,
                                                  keyConditionExpression: keyConditionExpression,
                                                  limit: limit,
                                                  tableName: targetTableName)
        let queryOutput = try dynamodb.querySync(input: queryInput)
            
        let lastEvaluatedKey: String?
        if let returnedLastEvaluatedKey = queryOutput.lastEvaluatedKey {
            let encodedLastEvaluatedKey = try jsonEncoder.encode(returnedLastEvaluatedKey)
            
            lastEvaluatedKey = String(data: encodedLastEvaluatedKey, encoding: .utf8)
        } else {
            lastEvaluatedKey = nil
        }
        
        if let outputAttributeValues = queryOutput.items {
            let items: [PolymorphicDatabaseItem<RowIdentity, PossibleTypes>] = try outputAttributeValues.map { values in
                let attributeValue = DynamoDBModel.AttributeValue(M: values)
                
                return try dynamoDecoder.decode(attributeValue)
            }
            
            return (items, lastEvaluatedKey)
        } else {
            return ([], lastEvaluatedKey)
        }
    }
    
    private func getAttributes<RowIdentity, ItemType>(forItem item: TypedDatabaseItem<RowIdentity, ItemType>) throws -> [String: DynamoDBModel.AttributeValue] {
        let attributeValue = try dynamoEncoder.encode(item)
        
        let attributes: [String: DynamoDBModel.AttributeValue]
        if let itemAttributes = attributeValue.M {
            attributes = itemAttributes
        } else {
            throw SwiftDynamoError.databaseError(reason: "Expected a map.")
        }
        
        return attributes
    }
    
    private func putItem<RowIdentity>(forInput putItemInput: DynamoDBModel.PutItemInput, withKey compositePrimaryKey: CompositePrimaryKey<RowIdentity>) throws {
        do {
            _ = try dynamodb.putItemSync(input: putItemInput)
        } catch DynamoDBError.conditionalCheckFailed(let errorPayload) {
            throw SwiftDynamoError.conditionalCheckFailed(paritionKey: compositePrimaryKey.partitionKey,
                                                          sortKey: compositePrimaryKey.sortKey,
                                                          message: errorPayload.message)
        } catch {
            Log.warning("Error from DynamoClient: \(error)")
            
            throw SwiftDynamoError.databaseError(reason: "\(error)")
        }
    }
}
