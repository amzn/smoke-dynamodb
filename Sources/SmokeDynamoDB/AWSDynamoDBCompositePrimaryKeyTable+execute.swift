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

/// DynamoDBTable conformance execute function
public extension AWSDynamoDBCompositePrimaryKeyTable {
    func execute<ReturnedType: PolymorphicOperationReturnType>(
            partitionKeys: [String],
            attributes: [String]?,
            additionalWhereClause: String?,
            nextToken: String?) -> EventLoopFuture<([ReturnedType], String?)> {
        let statement = getStatement(partitionKeys: partitionKeys,
                                     attributes: attributes,
                                     partitionKeyAttributeName: ReturnedType.AttributesType.partitionKeyAttributeName,
                                     additionalWhereClause: additionalWhereClause)
        let executeInput = ExecuteStatementInput(consistentRead: true, nextToken: nextToken, statement: statement)
        
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
    
    func execute<ReturnedType: PolymorphicOperationReturnType>(partitionKeys: [String],
                                                               attributes: [String]?,
                                                               additionalWhereClause: String?) -> EventLoopFuture<[ReturnedType]> {
        return partialExecute(partitionKeys: partitionKeys,
                              attributes: attributes,
                              additionalWhereClause: additionalWhereClause,
                              nextToken: nil)
    }
    
    func monomorphicExecute<AttributesType, ItemType>(
            partitionKeys: [String],
            attributes: [String]?,
            additionalWhereClause: String?, nextToken: String?)
    -> EventLoopFuture<([TypedDatabaseItem<AttributesType, ItemType>], String?)> {
        let statement = getStatement(partitionKeys: partitionKeys,
                                     attributes: attributes,
                                     partitionKeyAttributeName: AttributesType.partitionKeyAttributeName,
                                     additionalWhereClause: additionalWhereClause)
        let executeInput = ExecuteStatementInput(consistentRead: true, nextToken: nextToken, statement: statement)
        
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
    
    func monomorphicExecute<AttributesType, ItemType>(partitionKeys: [String],
                                                      attributes: [String]?,
                                                      additionalWhereClause: String?)
    -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]> {
        return monomorphicPartialExecute(partitionKeys: partitionKeys,
                                         attributes: attributes,
                                         additionalWhereClause: additionalWhereClause,
                                         nextToken: nil)
    }
    
    // function to return a future with the results of an execute call and all future paginated calls
    private func partialExecute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
                                                          attributes: [String]?,
                                                          additionalWhereClause: String?,
            nextToken: String?) -> EventLoopFuture<[ReturnedType]> {
        let executeFuture: EventLoopFuture<([ReturnedType], String?)> =
            execute(partitionKeys: partitionKeys,
                    attributes: attributes,
                    additionalWhereClause: additionalWhereClause,
                    nextToken: nextToken)
        
        return executeFuture.flatMap { paginatedItems in
            // if there are more items
            if let returnedNextToken = paginatedItems.1 {
                // returns a future with all the results from all later paginated calls
                return self.partialExecute(partitionKeys: partitionKeys,
                                           attributes: attributes,
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
            attributes: [String]?,
            additionalWhereClause: String?,
            nextToken: String?) -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]> {
        let executeFuture: EventLoopFuture<([TypedDatabaseItem<AttributesType, ItemType>], String?)> =
            monomorphicExecute(partitionKeys: partitionKeys,
                               attributes: attributes,
                               additionalWhereClause: additionalWhereClause,
                               nextToken: nextToken)
        
        return executeFuture.flatMap { paginatedItems in
            // if there are more items
            if let returnedNextToken = paginatedItems.1 {
                // returns a future with all the results from all later paginated calls
                return self.monomorphicPartialExecute(partitionKeys: partitionKeys,
                                                      attributes: attributes,
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
                              attributes: [String]?,
                              partitionKeyAttributeName: String,
                              additionalWhereClause: String?) -> String {
        let attributesString = attributes?.joined(separator: ", ") ?? "*"
        
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
            SELECT \(attributesString) FROM "\(self.targetTableName)" WHERE \(partitionWhereClause)\(whereClausePostfix)
            """
    }
}
