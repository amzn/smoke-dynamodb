// Copyright 2018-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
//  AWSDynamoDBTable+DynamoDBTableSync.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeAWSCore
import DynamoDBModel
import SmokeHTTPClient
import LoggerAPI

/// DynamoDBTable conformance sync functions
public extension AWSDynamoDBTable {
    
    public func insertItemSync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws {
        let putItemInput = try getInputForInsert(item)
        
        try putItemSync(forInput: putItemInput, withKey: item.compositePrimaryKey)
    }
    
    public func clobberItemSync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws {
        let attributes = try getAttributes(forItem: item)
        
        let putItemInput = DynamoDBModel.PutItemInput(item: attributes,
                                                      tableName: targetTableName)
        
        try putItemSync(forInput: putItemInput, withKey: item.compositePrimaryKey)
    }
    
    public func updateItemSync<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                         existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws {
        let putItemInput = try getInputForUpdateItem(newItem: newItem, existingItem: existingItem)
        
        try putItemSync(forInput: putItemInput, withKey: newItem.compositePrimaryKey)
    }
    
    public func getItemSync<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) throws
        -> TypedDatabaseItem<AttributesType, ItemType>? {
            let putItemInput = try getInputForGetItem(forKey: key)
            
            Log.verbose("dynamodb.getItem with key: \(key) and table name \(targetTableName)")
            let attributeValue = try dynamodb.getItemSync(input: putItemInput)
            
            if let item = attributeValue.item {
                Log.verbose("Value returned from DynamoDB.")
                
                return try AWSDynamoDBTable.dynamodbDecoder.decode(DynamoDBModel.AttributeValue(M: item))
            } else {
                Log.verbose("No item returned from DynamoDB.")
                
                return nil
            }
    }
    
    public func deleteItemSync<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws {
        let deleteItemInput = try getInputForDeleteItem(forKey: key)
        
        Log.verbose("dynamodb.deleteItem with key: \(key) and table name \(targetTableName)")
        _ = try dynamodb.deleteItemSync(input: deleteItemInput)
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
                let encodedLastEvaluatedKey = try AWSDynamoDBTable.jsonEncoder.encode(returnedLastEvaluatedKey)
                
                lastEvaluatedKey = String(data: encodedLastEvaluatedKey, encoding: .utf8)
            } else {
                lastEvaluatedKey = nil
            }
            
            if let outputAttributeValues = queryOutput.items {
                let items: [PolymorphicDatabaseItem<AttributesType, PossibleTypes>] = try outputAttributeValues.map { values in
                    let attributeValue = DynamoDBModel.AttributeValue(M: values)
                    
                    return try AWSDynamoDBTable.dynamodbDecoder.decode(attributeValue)
                }
                
                return (items, lastEvaluatedKey)
            } else {
                return ([], lastEvaluatedKey)
            }
    }
    
    private func putItemSync<AttributesType>(forInput putItemInput: DynamoDBModel.PutItemInput,
                                             withKey compositePrimaryKey: CompositePrimaryKey<AttributesType>) throws {
        do {
            _ = try dynamodb.putItemSync(input: putItemInput)
        } catch DynamoDBError.conditionalCheckFailed(let errorPayload) {
            throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: compositePrimaryKey.partitionKey,
                                                          sortKey: compositePrimaryKey.sortKey,
                                                          message: errorPayload.message)
        } catch {
            Log.warning("Error from AWSDynamoDBTable: \(error)")
            
            throw SmokeDynamoDBError.databaseError(reason: "\(error)")
        }
    }
}
