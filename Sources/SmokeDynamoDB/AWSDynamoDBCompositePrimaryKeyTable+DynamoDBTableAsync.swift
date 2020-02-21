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
import Logging

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
                                                completion: @escaping (SmokeDynamoDBErrorResult<TypedDatabaseItem<AttributesType, ItemType>?>) -> ())
        throws where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
            let putItemInput = try getInputForGetItem(forKey: key)
            
            self.logger.debug("dynamodb.getItem with key: \(key) and table name \(targetTableName)")
            try dynamodb.getItemAsync(input: putItemInput) { result in
                switch result {
                case .success(let attributeValue):
                    if let item = attributeValue.item {
                        self.logger.debug("Value returned from DynamoDB.")
                        
                        do {
                            let decodedItem: TypedDatabaseItem<AttributesType, ItemType>? =
                                try DynamoDBDecoder().decode(DynamoDBModel.AttributeValue(M: item))
                            completion(.success(decodedItem))
                        } catch {
                            completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
                        }
                    } else {
                        self.logger.debug("No item returned from DynamoDB.")
                        
                        completion(.success(nil))
                    }
                case .failure(let error):
                    completion(.failure(error.asSmokeDynamoDBError()))
                }
            }
    }
    
    func deleteItemAsync<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>,
                                         completion: @escaping (Error?) -> ())
        throws where AttributesType: PrimaryKeyAttributes {
            let deleteItemInput = try getInputForDeleteItem(forKey: key)
        
            self.logger.debug("dynamodb.deleteItem with key: \(key) and table name \(targetTableName)")
            try dynamodb.deleteItemAsync(input: deleteItemInput) { result in
                switch result {
                case .success:
                    // complete the putItem
                    completion(nil)
                case .failure(let error):
                    completion(error)
                }
            }
    }
    
    func queryAsync<AttributesType, PossibleTypes>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            completion: @escaping (SmokeDynamoDBErrorResult<[PolymorphicDatabaseItem<AttributesType, PossibleTypes>]>) -> ())
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
        completion: @escaping (SmokeDynamoDBErrorResult<[PolymorphicDatabaseItem<AttributesType, PossibleTypes>]>) -> ())
        throws where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            func handleQueryResult(result: SmokeDynamoDBErrorResult<([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)>) {
                switch result {
                case .success(let paginatedItems):
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
                            completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
                        }
                    } else {
                        // we have all the items
                        completion(.success(partialResults.items))
                    }
                case .failure(let error):
                    completion(.failure(error))
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
            completion: @escaping (SmokeDynamoDBErrorResult<([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)>) -> ())
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
            completion: @escaping (SmokeDynamoDBErrorResult<([PolymorphicDatabaseItem<AttributesType, PossibleTypes>], String?)>) -> ())
        throws where AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes {
            let queryInput = try DynamoDBModel.QueryInput.forSortKeyCondition(forPartitionKey: partitionKey, targetTableName: targetTableName,
                                                                              primaryKeyType: AttributesType.self,
                                                                              sortKeyCondition: sortKeyCondition, limit: limit,
                                                                              scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey)
            try dynamodb.queryAsync(input: queryInput) { result in
                switch result {
                case .success(let queryOutput):
                    let lastEvaluatedKey: String?
                    if let returnedLastEvaluatedKey = queryOutput.lastEvaluatedKey {
                        let encodedLastEvaluatedKey: Data
                        
                        do {
                            encodedLastEvaluatedKey = try JSONEncoder().encode(returnedLastEvaluatedKey)
                        } catch {
                            return completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
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
                                
                                return try DynamoDBDecoder().decode(attributeValue)
                            }
                        } catch {
                            return completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
                        }
                        
                        completion(.success((items, lastEvaluatedKey)))
                    } else {
                        completion(.success(([], lastEvaluatedKey)))
                    }
                case .failure(let error):
                    return completion(.failure(error.asSmokeDynamoDBError()))
                }
            }
    }
    
    private func putItemAsync<AttributesType>(forInput putItemInput: DynamoDBModel.PutItemInput,
                                              withKey compositePrimaryKey: CompositePrimaryKey<AttributesType>,
                                              completion: @escaping (Error?) -> ()) throws {
        do {
            _ = try dynamodb.putItemAsync(input: putItemInput) { result in
                switch result {
                case .success:
                    // complete the putItem
                    completion(nil)
                case .failure(let error):
                    switch error {
                    case DynamoDBError.conditionalCheckFailed(let errorPayload):
                        completion(SmokeDynamoDBError.conditionalCheckFailed(partitionKey: compositePrimaryKey.partitionKey,
                                                                           sortKey: compositePrimaryKey.sortKey,
                                                                           message: errorPayload.message))
                    default:
                        self.logger.warning("Error from AWSDynamoDBTable: \(error)")
            
                        completion(SmokeDynamoDBError.databaseError(cause: error))
                    }
                }
            }
        } catch {
            self.logger.warning("Error from AWSDynamoDBTable: \(error)")
            
            throw SmokeDynamoDBError.databaseError(cause: error)
        }
    }
}
