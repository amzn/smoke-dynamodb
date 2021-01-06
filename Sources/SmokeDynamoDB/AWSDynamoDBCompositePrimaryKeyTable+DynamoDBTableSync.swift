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
//  AWSDynamoDBCompositePrimaryKeyTable+DynamoDBTableSync.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeAWSCore
import DynamoDBModel
import SmokeHTTPClient
import Logging

/// DynamoDBTable conformance sync functions
public extension AWSDynamoDBCompositePrimaryKeyTable {
    
    func insertItemSync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws {
        let putItemInput = try getInputForInsert(item)
        
        try putItemSync(forInput: putItemInput, withKey: item.compositePrimaryKey)
    }
    
    func clobberItemSync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) throws {
        let attributes = try getAttributes(forItem: item)
        
        let putItemInput = DynamoDBModel.PutItemInput(item: attributes,
                                                      tableName: targetTableName)
        
        try putItemSync(forInput: putItemInput, withKey: item.compositePrimaryKey)
    }
    
    func updateItemSync<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                  existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws {
        let putItemInput = try getInputForUpdateItem(newItem: newItem, existingItem: existingItem)
        
        try putItemSync(forInput: putItemInput, withKey: newItem.compositePrimaryKey)
    }
    
    func getItemSync<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) throws
        -> TypedDatabaseItem<AttributesType, ItemType>? {
            let putItemInput = try getInputForGetItem(forKey: key)
            
            self.logger.debug("dynamodb.getItem with key: \(key) and table name \(targetTableName)")
            let attributeValue = try dynamodb.getItemSync(input: putItemInput)
            
            if let item = attributeValue.item {
                self.logger.debug("Value returned from DynamoDB.")
                
                return try DynamoDBDecoder().decode(DynamoDBModel.AttributeValue(M: item))
            } else {
                self.logger.debug("No item returned from DynamoDB.")
                
                return nil
            }
    }
    
    func deleteItemSync<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) throws {
        let deleteItemInput = try getInputForDeleteItem(forKey: key)
        
        self.logger.debug("dynamodb.deleteItem with key: \(key) and table name \(targetTableName)")
        _ = try dynamodb.deleteItemSync(input: deleteItemInput)
    }
    
    func deleteItemSync<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        let deleteItemInput = try getInputForDeleteItem(existingItem: existingItem)
        
        let logMessage = "dynamodb.deleteItem with key: \(existingItem.compositePrimaryKey), "
            + " version \(existingItem.rowStatus.rowVersion) and table name \(targetTableName)"
        
        self.logger.debug("\(logMessage)")
        _ = try dynamodb.deleteItemSync(input: deleteItemInput)
    }
    
    func querySync<AttributesType, PossibleTypes>(forPartitionKey partitionKey: String,
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
    
    func querySync<AttributesType, PossibleTypes>(forPartitionKey partitionKey: String,
                                                  sortKeyCondition: AttributeCondition?,
                                                  limit: Int?,
                                                  exclusiveStartKey: String?) throws
        -> ([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)
        where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            return try querySync(forPartitionKey: partitionKey,
                                 sortKeyCondition: sortKeyCondition,
                                 limit: limit,
                                 scanIndexForward: true,
                                 exclusiveStartKey: exclusiveStartKey)
    }
    
    func querySync<AttributesType, PossibleTypes>(forPartitionKey partitionKey: String,
                                                  sortKeyCondition: AttributeCondition?,
                                                  limit: Int?,
                                                  scanIndexForward: Bool,
                                                  exclusiveStartKey: String?) throws
        -> ([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)
        where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            let queryInput = try DynamoDBModel.QueryInput.forSortKeyCondition(forPartitionKey: partitionKey, targetTableName: targetTableName,
                                                                              primaryKeyType: AttributesType.self,
                                                                              sortKeyCondition: sortKeyCondition, limit: limit,
                                                                              scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey)
        
            let logMessage = "dynamodb.query with partitionKey: \(partitionKey), " +
                "sortKeyCondition: \(sortKeyCondition.debugDescription), and table name \(targetTableName)."
            self.logger.debug("\(logMessage)")
        
            let queryOutput = try dynamodb.querySync(input: queryInput)
            
            let lastEvaluatedKey: String?
            if let returnedLastEvaluatedKey = queryOutput.lastEvaluatedKey {
                let encodedLastEvaluatedKey = try JSONEncoder().encode(returnedLastEvaluatedKey)
                
                lastEvaluatedKey = String(data: encodedLastEvaluatedKey, encoding: .utf8)
            } else {
                lastEvaluatedKey = nil
            }
            
            if let outputAttributeValues = queryOutput.items {
                let items: [PolymorphicDatabaseItem<AttributesType, PossibleTypes>] = try outputAttributeValues.map { values in
                    let attributeValue = DynamoDBModel.AttributeValue(M: values)
                    
                    return try DynamoDBDecoder().decode(attributeValue)
                }
                
                return (items, lastEvaluatedKey)
            } else {
                return ([], lastEvaluatedKey)
            }
    }
    
    private func putItemSync<AttributesType>(forInput putItemInput: DynamoDBModel.PutItemInput,
                                             withKey compositePrimaryKey: CompositePrimaryKey<AttributesType>) throws {
        let logMessage = "dynamodb.putItem with item: \(putItemInput.item) and table name \(targetTableName)."
        self.logger.debug("\(logMessage)")
        
        do {
            _ = try dynamodb.putItemSync(input: putItemInput)
        } catch DynamoDBError.conditionalCheckFailed(let errorPayload) {
            throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: compositePrimaryKey.partitionKey,
                                                          sortKey: compositePrimaryKey.sortKey,
                                                          message: errorPayload.message)
        } catch {
            self.logger.warning("Error from AWSDynamoDBTable: \(error)")
            
            throw SmokeDynamoDBError.unexpectedError(cause: error)
        }
    }
    
    func monomorphicQuerySync<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                        sortKeyCondition: AttributeCondition?) throws
    -> [TypedDatabaseItem<AttributesType, ItemType>]
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        var items: [TypedDatabaseItem<AttributesType, ItemType>] = []
        var exclusiveStartKey: String?
            
        while true {
            let paginatedItems: ([TypedDatabaseItem<AttributesType, ItemType>], String?) =
                try monomorphicQuerySync(forPartitionKey: partitionKey,
                                         sortKeyCondition: sortKeyCondition,
                                         limit: defaultPaginationLimit,
                                         scanIndexForward: true,
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
    
    func monomorphicQuerySync<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                               sortKeyCondition: AttributeCondition?,
                                                               limit: Int?, scanIndexForward: Bool,
                                                               exclusiveStartKey: String?) throws
    -> ([TypedDatabaseItem<AttributesType, ItemType>], String?)
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        let queryInput = try DynamoDBModel.QueryInput.forSortKeyCondition(
            forPartitionKey: partitionKey, targetTableName: targetTableName,
            primaryKeyType: AttributesType.self,
            sortKeyCondition: sortKeyCondition, limit: limit,
            scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey)
        
        let logMessage = "dynamodb.query with partitionKey: \(partitionKey), " +
            "sortKeyCondition: \(sortKeyCondition.debugDescription), and table name \(targetTableName)."
        self.logger.debug("\(logMessage)")
        
        let queryOutput = try dynamodb.querySync(input: queryInput)
        
        let lastEvaluatedKey: String?
        if let returnedLastEvaluatedKey = queryOutput.lastEvaluatedKey {
            let encodedLastEvaluatedKey = try JSONEncoder().encode(returnedLastEvaluatedKey)
            
            lastEvaluatedKey = String(data: encodedLastEvaluatedKey, encoding: .utf8)
        } else {
            lastEvaluatedKey = nil
        }
        
        if let outputAttributeValues = queryOutput.items {
            let items: [TypedDatabaseItem<AttributesType, ItemType>] = try outputAttributeValues.map { values in
                let attributeValue = DynamoDBModel.AttributeValue(M: values)
                
                return try DynamoDBDecoder().decode(attributeValue)
            }
            
            return (items, lastEvaluatedKey)
        } else {
            return ([], lastEvaluatedKey)
        }
    }
}
