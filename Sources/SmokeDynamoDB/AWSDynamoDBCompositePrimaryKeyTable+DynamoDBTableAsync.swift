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
//  AWSDynamoDBCompositePrimaryKeyTable+DynamoDBTableAsync.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeAWSCore
import DynamoDBModel
import SmokeHTTPClient
import LoggerAPI

/// DynamoDBTable conformance async functions
public extension AWSDynamoDBCompositePrimaryKeyTable {
    
    func insertItemAsync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                   completion: @escaping (Error?) -> ())
        throws where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            let putItemInput = try getInputForInsert(item)
        
            try putItemAsync(forInput: putItemInput, withKey: item.compositePrimaryKey,
                             completion: completion)
    }
    
    func clobberItemAsync<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                    completion: @escaping (Error?) -> ())
        throws where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            let attributes = try getAttributes(forItem: item)
        
            let putItemInput = DynamoDBModel.PutItemInput(item: attributes,
                                                      tableName: targetTableName)
        
            try putItemAsync(forInput: putItemInput, withKey: item.compositePrimaryKey, completion: completion)
    }
    
    func updateItemAsync<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                   existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                   completion: @escaping (Error?) -> ())
        throws where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            let putItemInput = try getInputForUpdateItem(newItem: newItem, existingItem: existingItem)
        
            try putItemAsync(forInput: putItemInput, withKey: newItem.compositePrimaryKey, completion: completion)
    }
    
    func getItemAsync<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>,
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
                                try AWSDynamoDBCompositePrimaryKeyTable.dynamodbDecoder.decode(DynamoDBModel.AttributeValue(M: item))
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
    
    func deleteItemAsync<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>,
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
    
    func queryAsync<AttributesType, PossibleTypes>(
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
    
    func queryAsync<AttributesType, PossibleTypes>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?, exclusiveStartKey: String?,
            completion: @escaping (HTTPResult<([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)>) -> ())
        throws where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            try queryAsync(forPartitionKey: partitionKey,
                           sortKeyCondition: sortKeyCondition,
                           limit: limit,
                           scanIndexForward: true,
                           exclusiveStartKey: exclusiveStartKey,
                           completion: completion)
    }
    
    func queryAsync<AttributesType, PossibleTypes>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?, scanIndexForward: Bool, exclusiveStartKey: String?,
            completion: @escaping (HTTPResult<([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)>) -> ())
        throws where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            let queryInput = try DynamoDBModel.QueryInput.forSortKeyCondition(forPartitionKey: partitionKey, targetTableName: targetTableName,
                                                                              primaryKeyType: AttributesType.self,
                                                                              sortKeyCondition: sortKeyCondition, limit: limit,
                                                                              scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey)
            try dynamodb.queryAsync(input: queryInput) { result in
                switch result {
                case .response(let queryOutput):
                    let lastEvaluatedKey: String?
                    if let returnedLastEvaluatedKey = queryOutput.lastEvaluatedKey {
                        let encodedLastEvaluatedKey: Data
                        
                        do {
                            encodedLastEvaluatedKey = try AWSDynamoDBCompositePrimaryKeyTable.jsonEncoder.encode(returnedLastEvaluatedKey)
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
                                
                                return try AWSDynamoDBCompositePrimaryKeyTable.dynamodbDecoder.decode(attributeValue)
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
                        completion(SmokeDynamoDBError.conditionalCheckFailed(partitionKey: compositePrimaryKey.partitionKey,
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
