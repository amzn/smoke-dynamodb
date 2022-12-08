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
//  AWSDynamoDBCompositePrimaryKeyTable+execute.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeAWSCore
import DynamoDBModel
import SmokeHTTPClient
import Logging
import NIO

// ExecuteStatement has a maximum of 50 of decomposed read operations per request
private let maximumKeysPerExecuteStatement = 50

/// DynamoDBTable conformance execute function
public extension AWSDynamoDBCompositePrimaryKeyTable {
    func execute<ReturnedType: PolymorphicOperationReturnType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?,
            nextToken: String?) -> EventLoopFuture<([ReturnedType], String?)> {
        // if there are no partitions, there will be no results to return
        // succeed immediately with empty results
        guard partitionKeys.count > 0 else {
            let promise = self.eventLoop.makePromise(of: ([ReturnedType], String?).self)
            promise.succeed(([], nil))
            return promise.futureResult
        }
        
        // ExecuteStatement API has a maximum limit on the number of decomposed read operations per request.
        // Caller of this function needs to handle pagination on their side.
        guard partitionKeys.count <= maximumKeysPerExecuteStatement else {
            let promise = self.eventLoop.makePromise(of: ([ReturnedType], String?).self)
            promise.fail(SmokeDynamoDBError.validationError(
                            reason: "Execute API has a maximum limit of \(maximumKeysPerExecuteStatement) partition keys per request."))
            return promise.futureResult
        }
        
        let statement = getStatement(partitionKeys: partitionKeys,
                                     attributesFilter: attributesFilter,
                                     partitionKeyAttributeName: ReturnedType.AttributesType.partitionKeyAttributeName,
                                     additionalWhereClause: additionalWhereClause)
        let executeInput = ExecuteStatementInput(consistentRead: self.consistentRead, nextToken: nextToken, statement: statement)
        
        return dynamodb.executeStatement(input: executeInput).flatMapThrowing { executeOutput in
            let nextToken = executeOutput.nextToken
            
            if let outputAttributeValues = executeOutput.items {
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
                
                return (items, nextToken)
            } else {
                return ([], nextToken)
            }
        } .flatMapErrorThrowing { error in
            if let typedError = error as? DynamoDBError {
                throw typedError.asSmokeDynamoDBError()
            }
            
            throw error.asUnrecognizedSmokeDynamoDBError()
        }
    }
    
    func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) -> EventLoopFuture<[ReturnedType]> {
        // ExecuteStatement API has a maximum limit on the number of decomposed read operations per request.
        // This function handles pagination internally.
        let chunkedPartitionKeys = partitionKeys.chunked(by: maximumKeysPerExecuteStatement)
        let futures = chunkedPartitionKeys.map { chunk -> EventLoopFuture<[ReturnedType]> in
            return partialExecute(partitionKeys: chunk,
                                  attributesFilter: attributesFilter,
                                  additionalWhereClause: additionalWhereClause,
                                  nextToken: nil)
        }
        
        return EventLoopFuture.whenAllSucceed(futures, on: self.eventLoop).map { itemLists in
            itemLists.flatMap { $0 }
        }
    }
    
    func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?,
        nextToken: String?) -> EventLoopFuture<([TypedDatabaseItem<AttributesType, ItemType>], String?)> {
        // if there are no partitions, there will be no results to return
        // succeed immediately with empty results
        guard partitionKeys.count > 0 else {
            let promise = self.eventLoop.makePromise(of: ([TypedDatabaseItem<AttributesType, ItemType>], String?).self)
            promise.succeed(([], nil))
            return promise.futureResult
        }
        
        // ExecuteStatement API has a maximum limit on the number of decomposed read operations per request.
        // Caller of this function needs to handle pagination on their side.
        guard partitionKeys.count <= maximumKeysPerExecuteStatement else {
            let promise = self.eventLoop.makePromise(of: ([TypedDatabaseItem<AttributesType, ItemType>], String?).self)
            promise.fail(SmokeDynamoDBError.validationError(
                            reason: "Execute API has a maximum limit of \(maximumKeysPerExecuteStatement) partition keys per request."))
            return promise.futureResult
        }
        
        let statement = getStatement(partitionKeys: partitionKeys,
                                     attributesFilter: attributesFilter,
                                     partitionKeyAttributeName: AttributesType.partitionKeyAttributeName,
                                     additionalWhereClause: additionalWhereClause)
        let executeInput = ExecuteStatementInput(consistentRead: self.consistentRead, nextToken: nextToken, statement: statement)
        
        return dynamodb.executeStatement(input: executeInput).flatMapThrowing { executeOutput in
            let nextToken = executeOutput.nextToken
            
            if let outputAttributeValues = executeOutput.items {
                let items: [TypedDatabaseItem<AttributesType, ItemType>]
                
                do {
                    items = try outputAttributeValues.map { values in
                        let attributeValue = DynamoDBModel.AttributeValue(M: values)
                        
                        return try DynamoDBDecoder().decode(attributeValue)
                    }
                } catch {
                    throw error.asUnrecognizedSmokeDynamoDBError()
                }
                
                return (items, nextToken)
            } else {
                return ([], nextToken)
            }
        } .flatMapErrorThrowing { error in
            if let typedError = error as? DynamoDBError {
                throw typedError.asSmokeDynamoDBError()
            }
            
            throw error.asUnrecognizedSmokeDynamoDBError()
        }
    }
    
    func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]> {
        // ExecuteStatement API has a maximum limit on the number of decomposed read operations per request.
        // This function handles pagination internally.
        let chunkedPartitionKeys = partitionKeys.chunked(by: maximumKeysPerExecuteStatement)
        let futures = chunkedPartitionKeys.map { chunk -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]> in
            return monomorphicPartialExecute(partitionKeys: chunk,
                                             attributesFilter: attributesFilter,
                                             additionalWhereClause: additionalWhereClause,
                                             nextToken: nil)
        }
        
        return EventLoopFuture.whenAllSucceed(futures, on: self.eventLoop).map { itemLists in
            itemLists.flatMap { $0 }
        }
    }
    
    // function to return a future with the results of an execute call and all future paginated calls
    private func partialExecute<ReturnedType: PolymorphicOperationReturnType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?,
            nextToken: String?) -> EventLoopFuture<[ReturnedType]> {
        let executeFuture: EventLoopFuture<([ReturnedType], String?)> =
            execute(partitionKeys: partitionKeys,
                    attributesFilter: attributesFilter,
                    additionalWhereClause: additionalWhereClause,
                    nextToken: nextToken)
        
        return executeFuture.flatMap { paginatedItems in
            // if there are more items
            if let returnedNextToken = paginatedItems.1 {
                // returns a future with all the results from all later paginated calls
                return self.partialExecute(partitionKeys: partitionKeys,
                                           attributesFilter: attributesFilter,
                                           additionalWhereClause: additionalWhereClause,
                                           nextToken: returnedNextToken)
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
    
    private func monomorphicPartialExecute<AttributesType, ItemType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?,
            nextToken: String?) -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]> {
        let executeFuture: EventLoopFuture<([TypedDatabaseItem<AttributesType, ItemType>], String?)> =
            monomorphicExecute(partitionKeys: partitionKeys,
                               attributesFilter: attributesFilter,
                               additionalWhereClause: additionalWhereClause,
                               nextToken: nextToken)
        
        return executeFuture.flatMap { paginatedItems in
            // if there are more items
            if let returnedNextToken = paginatedItems.1 {
                // returns a future with all the results from all later paginated calls
                return self.monomorphicPartialExecute(partitionKeys: partitionKeys,
                                                      attributesFilter: attributesFilter,
                                                      additionalWhereClause: additionalWhereClause,
                                                      nextToken: returnedNextToken)
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
    
    private func getStatement(partitionKeys: [String],
                              attributesFilter: [String]?,
                              partitionKeyAttributeName: String,
                              additionalWhereClause: String?) -> String {
        let attributesFilterString = attributesFilter?.joined(separator: ", ") ?? "*"
        
        let partitionWhereClause: String
        if partitionKeys.count == 1 {
            partitionWhereClause = "\(partitionKeyAttributeName)='\(partitionKeys[0])'"
        } else {
            partitionWhereClause = "\(partitionKeyAttributeName) IN ['\(partitionKeys.joined(separator: "', '"))']"
        }
        
        let whereClausePostfix: String
        if let additionalWhereClause = additionalWhereClause {
            whereClausePostfix = " \(additionalWhereClause)"
        } else {
            whereClausePostfix = ""
        }
        
        return """
            SELECT \(attributesFilterString) FROM "\(self.targetTableName)" WHERE \(partitionWhereClause)\(whereClausePostfix)
            """
    }
    
#if (os(Linux) && compiler(>=5.5)) || (!os(Linux) && compiler(>=5.5.2)) && canImport(_Concurrency)
    func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
    -> ([ReturnedType], String?) {
        // if there are no partitions, there will be no results to return
        // succeed immediately with empty results
        guard partitionKeys.count > 0 else {
            return ([], nil)
        }
        
        // ExecuteStatement API has a maximum limit on the number of decomposed read operations per request.
        // Caller of this function needs to handle pagination on their side.
        guard partitionKeys.count <= maximumKeysPerExecuteStatement else {
            throw SmokeDynamoDBError.validationError(
                            reason: "Execute API has a maximum limit of \(maximumKeysPerExecuteStatement) partition keys per request.")
        }
        
        let statement = getStatement(partitionKeys: partitionKeys,
                                     attributesFilter: attributesFilter,
                                     partitionKeyAttributeName: ReturnedType.AttributesType.partitionKeyAttributeName,
                                     additionalWhereClause: additionalWhereClause)
        let executeInput = ExecuteStatementInput(consistentRead: true, nextToken: nextToken, statement: statement)
        
        do {
            let executeOutput = try await self.dynamodb.executeStatement(input: executeInput)
            
            let nextToken = executeOutput.nextToken
            
            if let outputAttributeValues = executeOutput.items {
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
                
                return (items, nextToken)
            } else {
                return ([], nextToken)
            }
        } catch {
            if let typedError = error as? DynamoDBError {
                throw typedError.asSmokeDynamoDBError()
            }
            
            throw error.asUnrecognizedSmokeDynamoDBError()
        }
    }
    
    func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
    -> [ReturnedType] {
        // ExecuteStatement API has a maximum limit on the number of decomposed read operations per request.
        // This function handles pagination internally.
        let chunkedPartitionKeys = partitionKeys.chunked(by: maximumKeysPerExecuteStatement)
        let itemLists = try await chunkedPartitionKeys.concurrentMap { chunk -> [ReturnedType] in
            return try await self.partialExecute(partitionKeys: chunk,
                                                 attributesFilter: attributesFilter,
                                                 additionalWhereClause: additionalWhereClause,
                                                 nextToken: nil)
        }
        
        return itemLists.flatMap { $0 }
    }
    
    func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws
    -> ([TypedDatabaseItem<AttributesType, ItemType>], String?) {
        // if there are no partitions, there will be no results to return
        // succeed immediately with empty results
        guard partitionKeys.count > 0 else {
            return ([], nil)
        }
        
        // ExecuteStatement API has a maximum limit on the number of decomposed read operations per request.
        // Caller of this function needs to handle pagination on their side.
        guard partitionKeys.count <= maximumKeysPerExecuteStatement else {
            throw SmokeDynamoDBError.validationError(
                            reason: "Execute API has a maximum limit of \(maximumKeysPerExecuteStatement) partition keys per request.")
        }
        
        let statement = getStatement(partitionKeys: partitionKeys,
                                     attributesFilter: attributesFilter,
                                     partitionKeyAttributeName: AttributesType.partitionKeyAttributeName,
                                     additionalWhereClause: additionalWhereClause)
        let executeInput = ExecuteStatementInput(consistentRead: true, nextToken: nextToken, statement: statement)
        
        do {
            let executeOutput = try await self.dynamodb.executeStatement(input: executeInput)
            
            let nextToken = executeOutput.nextToken
            
            if let outputAttributeValues = executeOutput.items {
                let items: [TypedDatabaseItem<AttributesType, ItemType>]
                
                do {
                    items = try outputAttributeValues.map { values in
                        let attributeValue = DynamoDBModel.AttributeValue(M: values)
                        
                        return try DynamoDBDecoder().decode(attributeValue)
                    }
                } catch {
                    throw error.asUnrecognizedSmokeDynamoDBError()
                }
                
                return (items, nextToken)
            } else {
                return ([], nextToken)
            }
        } catch {
            if let typedError = error as? DynamoDBError {
                throw typedError.asSmokeDynamoDBError()
            }
            
            throw error.asUnrecognizedSmokeDynamoDBError()
        }
    }
    
    func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        // ExecuteStatement API has a maximum limit on the number of decomposed read operations per request.
        // This function handles pagination internally.
        let chunkedPartitionKeys = partitionKeys.chunked(by: maximumKeysPerExecuteStatement)
        let itemLists = try await chunkedPartitionKeys.concurrentMap { chunk -> [TypedDatabaseItem<AttributesType, ItemType>] in
            return try await self.monomorphicPartialExecute(partitionKeys: chunk,
                                                            attributesFilter: attributesFilter,
                                                            additionalWhereClause: additionalWhereClause,
                                                            nextToken: nil)
        }
        
        return itemLists.flatMap { $0 }
    }
    
    // function to return a future with the results of an execute call and all future paginated calls
    private func partialExecute<ReturnedType: PolymorphicOperationReturnType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?,
            nextToken: String?) async throws
    -> [ReturnedType] {
        let paginatedItems: ([ReturnedType], String?) =
            try await execute(partitionKeys: partitionKeys,
                    attributesFilter: attributesFilter,
                    additionalWhereClause: additionalWhereClause,
                    nextToken: nextToken)
        
        // if there are more items
        if let returnedNextToken = paginatedItems.1 {
            // returns a future with all the results from all later paginated calls
            let partialResult: [ReturnedType] = try await self.partialExecute(partitionKeys: partitionKeys,
                                                                              attributesFilter: attributesFilter,
                                                                              additionalWhereClause: additionalWhereClause,
                                                                              nextToken: returnedNextToken)
                
            // return the results from 'this' call and all later paginated calls
            return paginatedItems.0 + partialResult
        } else {
            // this is it, all results have been obtained
            return paginatedItems.0
        }
    }
    
    private func monomorphicPartialExecute<AttributesType, ItemType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?,
            nextToken: String?) async throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        let paginatedItems: ([TypedDatabaseItem<AttributesType, ItemType>], String?) =
            try await monomorphicExecute(partitionKeys: partitionKeys,
                                         attributesFilter: attributesFilter,
                                         additionalWhereClause: additionalWhereClause,
                                         nextToken: nextToken)
        
        // if there are more items
        if let returnedNextToken = paginatedItems.1 {
            // returns a future with all the results from all later paginated calls
            let partialResult: [TypedDatabaseItem<AttributesType, ItemType>] = try await self.monomorphicPartialExecute(
                partitionKeys: partitionKeys,
                attributesFilter: attributesFilter,
                additionalWhereClause: additionalWhereClause,
                nextToken: returnedNextToken)
                
            // return the results from 'this' call and all later paginated calls
            return paginatedItems.0 + partialResult
        } else {
            // this is it, all results have been obtained
            return paginatedItems.0
        }
    }
#endif
}
