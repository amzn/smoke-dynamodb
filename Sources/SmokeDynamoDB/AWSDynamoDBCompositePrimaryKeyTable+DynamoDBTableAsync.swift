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
//  AWSDynamoDBCompositePrimaryKeyTable+DynamoDBTableAsync.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeAWSCore
import DynamoDBModel
import SmokeHTTPClient
import Logging
import NIO

/// DynamoDBTable conformance async functions
public extension AWSDynamoDBCompositePrimaryKeyTable {
    
    func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void>
            where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
        let putItemInput: DynamoDBModel.PutItemInput
        do {
            putItemInput = try getInputForInsert(item)
        } catch {
            let promise = self.eventLoop.makePromise(of: Void.self)
            promise.fail(error)
            return promise.futureResult
        }
        
        return putItem(forInput: putItemInput, withKey: item.compositePrimaryKey)
    }
    
    func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void>
            where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
        let attributes: [String: AttributeValue]
        do {
            attributes = try getAttributes(forItem: item)
        } catch {
            let promise = self.eventLoop.makePromise(of: Void.self)
            promise.fail(error)
            return promise.futureResult
        }
        
        let putItemInput = DynamoDBModel.PutItemInput(item: attributes,
                                                      tableName: targetTableName)
        
        return putItem(forInput: putItemInput, withKey: item.compositePrimaryKey)
    }
    
    func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                              existingItem: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void>
            where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
        let putItemInput: DynamoDBModel.PutItemInput
        do {
            putItemInput = try getInputForUpdateItem(newItem: newItem, existingItem: existingItem)
        } catch {
            let promise = self.eventLoop.makePromise(of: Void.self)
            promise.fail(error)
            return promise.futureResult
        }
                
        return putItem(forInput: putItemInput, withKey: newItem.compositePrimaryKey)
    }
    
    func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>?>
            where AttributesType: PrimaryKeyAttributes, ItemType: Decodable, ItemType: Encodable {
        let getItemInput: DynamoDBModel.GetItemInput
        do {
            getItemInput = try getInputForGetItem(forKey: key)
        } catch {
            let promise = self.eventLoop.makePromise(of: TypedDatabaseItem<AttributesType, ItemType>?.self)
            promise.fail(error)
            return promise.futureResult
        }
            
        self.logger.debug("dynamodb.getItem with key: \(key) and table name \(targetTableName)")
        return dynamodb.getItem(input: getItemInput).flatMapThrowing { attributeValue in
            if let item = attributeValue.item {
                self.logger.debug("Value returned from DynamoDB.")
                
                do {
                    let decodedItem: TypedDatabaseItem<AttributesType, ItemType>? =
                        try DynamoDBDecoder().decode(DynamoDBModel.AttributeValue(M: item))
                    return decodedItem
                } catch {
                    throw error.asUnrecognizedSmokeDynamoDBError()
                }
            } else {
                self.logger.debug("No item returned from DynamoDB.")
                
                return nil
            }
        } .flatMapErrorThrowing { error in
            if let typedError = error as? DynamoDBError {
                throw typedError.asSmokeDynamoDBError()
            }
            
            throw error.asUnrecognizedSmokeDynamoDBError()
        }
    }
    
    func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) -> EventLoopFuture<Void>
            where AttributesType: PrimaryKeyAttributes {
        let deleteItemInput: DynamoDBModel.DeleteItemInput
        do {
            deleteItemInput = try getInputForDeleteItem(forKey: key)
        } catch {
            let promise = self.eventLoop.makePromise(of: Void.self)
            promise.fail(error)
            return promise.futureResult
        }
        
        self.logger.debug("dynamodb.deleteItem with key: \(key) and table name \(targetTableName)")
        return dynamodb.deleteItem(input: deleteItemInput) .map { _ in
            // return Void on success
        }
    }
    
    func deleteItem<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void>
            where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        let deleteItemInput: DynamoDBModel.DeleteItemInput
        do {
            deleteItemInput = try getInputForDeleteItem(existingItem: existingItem)
        } catch {
            let promise = self.eventLoop.makePromise(of: Void.self)
            promise.fail(error)
            return promise.futureResult
        }
        
        let logMessage = "dynamodb.deleteItem with key: \(existingItem.compositePrimaryKey), "
            + " version \(existingItem.rowStatus.rowVersion) and table name \(targetTableName)"
        
        self.logger.debug("\(logMessage)")
        return dynamodb.deleteItem(input: deleteItemInput) .map { _ in
            // return Void on success
        }
    }
    
    func query<ReturnedType: PolymorphicOperationReturnType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?) -> EventLoopFuture<[ReturnedType]> {
        return partialQuery(forPartitionKey: partitionKey,
                            sortKeyCondition: sortKeyCondition,
                            exclusiveStartKey: nil)
    }
    
    // function to return a future with the results of a query call and all future paginated calls
    private func partialQuery<ReturnedType: PolymorphicOperationReturnType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            exclusiveStartKey: String?) -> EventLoopFuture<[ReturnedType]> {
        let queryFuture: EventLoopFuture<([ReturnedType], String?)> =
            query(forPartitionKey: partitionKey,
                  sortKeyCondition: sortKeyCondition,
                  limit: nil,
                  scanIndexForward: true,
                  exclusiveStartKey: exclusiveStartKey)
        
        return queryFuture.flatMap { paginatedItems in
            // if there are more items
            if let lastEvaluatedKey = paginatedItems.1 {
                // returns a future with all the results from all later paginated calls
                return self.partialQuery(forPartitionKey: partitionKey,
                                         sortKeyCondition: sortKeyCondition,
                                         exclusiveStartKey: lastEvaluatedKey)
                    .map { partialResult in
                        // return the results from 'this' call and all later paginated calls
                        return paginatedItems.0 + partialResult
                    }
            } else {
                // this is it, all results have been obtained
                let promise = self.eventLoop.makePromise(of: [ReturnedType].self)
                promise.succeed(paginatedItems.0)
                return promise.futureResult
            }
        }
    }
    
    func query<ReturnedType: PolymorphicOperationReturnType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?, exclusiveStartKey: String?) -> EventLoopFuture<([ReturnedType], String?)> {
        return query(forPartitionKey: partitionKey,
                     sortKeyCondition: sortKeyCondition,
                     limit: limit,
                     scanIndexForward: true,
                     exclusiveStartKey: exclusiveStartKey)
    }
    
    func query<ReturnedType: PolymorphicOperationReturnType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?, scanIndexForward: Bool, exclusiveStartKey: String?)
            -> EventLoopFuture<([ReturnedType], String?)> {
        let queryInput: DynamoDBModel.QueryInput
        do {
            queryInput = try DynamoDBModel.QueryInput.forSortKeyCondition(forPartitionKey: partitionKey, targetTableName: targetTableName, consistentRead: true,
                                                                          primaryKeyType: ReturnedType.AttributesType.self,
                                                                          sortKeyCondition: sortKeyCondition, limit: limit,
                                                                          scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey)
        } catch {
            let promise = self.eventLoop.makePromise(of: ([ReturnedType], String?).self)
            promise.fail(error)
            return promise.futureResult
        }
        
        let logMessage = "dynamodb.query with partitionKey: \(partitionKey), " +
            "sortKeyCondition: \(sortKeyCondition.debugDescription), and table name \(targetTableName)."
        self.logger.debug("\(logMessage)")
        
        return dynamodb.query(input: queryInput).flatMapThrowing { queryOutput in
            let lastEvaluatedKey: String?
            if let returnedLastEvaluatedKey = queryOutput.lastEvaluatedKey {
                let encodedLastEvaluatedKey: Data
                
                do {
                    encodedLastEvaluatedKey = try JSONEncoder().encode(returnedLastEvaluatedKey)
                } catch {
                    throw error.asUnrecognizedSmokeDynamoDBError()
                }
                
                lastEvaluatedKey = String(data: encodedLastEvaluatedKey, encoding: .utf8)
            } else {
                lastEvaluatedKey = nil
            }
            
            if let outputAttributeValues = queryOutput.items {
                let items: [ReturnedType]
                
                do {
                    items = try outputAttributeValues.map { values in
                        let attributeValue = DynamoDBModel.AttributeValue(M: values)
                        
                        let decodedItem: ReturnTypeDecodable<ReturnedType> = try DynamoDBDecoder().decode(attributeValue)
                                                        
                        return decodedItem.decodedValue
                    }
                } catch {
                    throw error.asUnrecognizedSmokeDynamoDBError()
                }
                
                return (items, lastEvaluatedKey)
            } else {
                return ([], lastEvaluatedKey)
            }
        } .flatMapErrorThrowing { error in
            if let typedError = error as? DynamoDBError {
                throw typedError.asSmokeDynamoDBError()
            }
            
            throw error.asUnrecognizedSmokeDynamoDBError()
        }
    }
    
    private func putItem<AttributesType>(forInput putItemInput: DynamoDBModel.PutItemInput,
                                         withKey compositePrimaryKey: CompositePrimaryKey<AttributesType>) -> EventLoopFuture<Void> {
        let logMessage = "dynamodb.putItem with item: \(putItemInput.item) and table name \(targetTableName)."
        self.logger.debug("\(logMessage)")
        
        return self.dynamodb.putItem(input: putItemInput).map { _ in
            // return Void on success
        }.flatMapErrorThrowing { error in
            switch error {
            case DynamoDBError.conditionalCheckFailed(let errorPayload):
                throw SmokeDynamoDBError.conditionalCheckFailed(partitionKey: compositePrimaryKey.partitionKey,
                                                                sortKey: compositePrimaryKey.sortKey,
                                                                message: errorPayload.message)
            default:
                self.logger.warning("Error from AWSDynamoDBTable: \(error)")
    
                throw SmokeDynamoDBError.unexpectedError(cause: error)
            }
        }
    }
    
    func monomorphicQuery<AttributesType, ItemType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?) -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]>
            where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        return monomorphicPartialQuery(forPartitionKey: partitionKey,
                                       sortKeyCondition: sortKeyCondition,
                                       exclusiveStartKey: nil)
    }
    
    // function to return a future with the results of a query call and all future paginated calls
    private func monomorphicPartialQuery<AttributesType, ItemType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            exclusiveStartKey: String?) -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]>
            where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        let queryFuture: EventLoopFuture<([TypedDatabaseItem<AttributesType, ItemType>], String?)> =
            monomorphicQuery(forPartitionKey: partitionKey,
                             sortKeyCondition: sortKeyCondition,
                             limit: nil,
                             scanIndexForward: true,
                             exclusiveStartKey: nil)
        
        return queryFuture.flatMap { paginatedItems in
            // if there are more items
            if let lastEvaluatedKey = paginatedItems.1 {
                // returns a future with all the results from all later paginated calls
                return self.monomorphicPartialQuery(forPartitionKey: partitionKey,
                                                    sortKeyCondition: sortKeyCondition,
                                                    exclusiveStartKey: lastEvaluatedKey)
                    .map { partialResult in
                        // return the results from 'this' call and all later paginated calls
                        return paginatedItems.0 + partialResult
                    }
            } else {
                // this is it, all results have been obtained
                let promise = self.eventLoop.makePromise(of: [TypedDatabaseItem<AttributesType, ItemType>].self)
                promise.succeed(paginatedItems.0)
                return promise.futureResult
            }
        }
    }
    
    func monomorphicQuery<AttributesType, ItemType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?,
            scanIndexForward: Bool,
            exclusiveStartKey: String?) -> EventLoopFuture<([TypedDatabaseItem<AttributesType, ItemType>], String?)>
            where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        let queryInput: DynamoDBModel.QueryInput
        do {
            queryInput = try DynamoDBModel.QueryInput.forSortKeyCondition(
                forPartitionKey: partitionKey, targetTableName: targetTableName, consistentRead: true,
                primaryKeyType: AttributesType.self,
                sortKeyCondition: sortKeyCondition, limit: limit,
                scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey)
        } catch {
            let promise = self.eventLoop.makePromise(of: ([TypedDatabaseItem<AttributesType, ItemType>], String?).self)
            promise.fail(error)
            return promise.futureResult
        }
        
        let logMessage = "dynamodb.query with partitionKey: \(partitionKey), " +
            "sortKeyCondition: \(sortKeyCondition.debugDescription), and table name \(targetTableName)."
        self.logger.debug("\(logMessage)")
        
        return dynamodb.query(input: queryInput).flatMapThrowing { queryOutput in
            let lastEvaluatedKey: String?
            if let returnedLastEvaluatedKey = queryOutput.lastEvaluatedKey {
                let encodedLastEvaluatedKey: Data
                
                do {
                    encodedLastEvaluatedKey = try JSONEncoder().encode(returnedLastEvaluatedKey)
                } catch {
                    throw error.asUnrecognizedSmokeDynamoDBError()
                }
                
                lastEvaluatedKey = String(data: encodedLastEvaluatedKey, encoding: .utf8)
            } else {
                lastEvaluatedKey = nil
            }
            
            if let outputAttributeValues = queryOutput.items {
                let items: [TypedDatabaseItem<AttributesType, ItemType>]
                
                do {
                    items = try outputAttributeValues.map { values in
                        let attributeValue = DynamoDBModel.AttributeValue(M: values)
                        
                        return try DynamoDBDecoder().decode(attributeValue)
                    }
                } catch {
                    throw error.asUnrecognizedSmokeDynamoDBError()
                }
                
                return (items, lastEvaluatedKey)
            } else {
                return ([], lastEvaluatedKey)
            }
        } .flatMapErrorThrowing { error in
            if let typedError = error as? DynamoDBError {
                throw typedError.asSmokeDynamoDBError()
            }
            
            throw error.asUnrecognizedSmokeDynamoDBError()
        }
    }
}
