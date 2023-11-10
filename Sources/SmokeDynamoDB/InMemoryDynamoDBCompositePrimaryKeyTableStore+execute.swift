// swiftlint:disable cyclomatic_complexity
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
//  InMemoryDynamoDBCompositePrimaryKeyTableStore+execute.swift
//  SmokeDynamoDB
//

import Foundation
import AWSDynamoDB

extension InMemoryDynamoDBCompositePrimaryKeyTableStore {
    
    func execute<ReturnedType: PolymorphicOperationReturnType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?) throws
    -> [ReturnedType] {
        let items = self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)
           
        let returnedItems: [ReturnedType] = try items.map { item in
            return try self.convertToQueryableType(input: item)
        }
        
        return returnedItems
    }
    
    func execute<ReturnedType: PolymorphicOperationReturnType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?, nextToken: String?) throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?)  {
        let items = self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)
           
        let returnedItems: [ReturnedType] = try items.map { item in
            return try self.convertToQueryableType(input: item)
        }
        
        return (returnedItems, nil)
    }
    
    func monomorphicExecute<AttributesType, ItemType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?) throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        let items = self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)
           
        let returnedItems: [TypedDatabaseItem<AttributesType, ItemType>] = try items.map { item in
            guard let typedItem = item as? TypedDatabaseItem<AttributesType, ItemType> else {
                let foundType = type(of: item)
                let description = "Expected to decode \(TypedDatabaseItem<AttributesType, ItemType>.self). Instead found \(foundType)."
                let context = DecodingError.Context(codingPath: [], debugDescription: description)
                let error = DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
                    
                throw error
            }
                
            return typedItem
        }
        
        return returnedItems
    }
    
    func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) throws
    -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?) {
        let items = self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)
           
        let returnedItems: [TypedDatabaseItem<AttributesType, ItemType>] = try items.map { item in
            guard let typedItem = item as? TypedDatabaseItem<AttributesType, ItemType> else {
                let foundType = type(of: item)
                let description = "Expected to decode \(TypedDatabaseItem<AttributesType, ItemType>.self). Instead found \(foundType)."
                let context = DecodingError.Context(codingPath: [], debugDescription: description)
                let error = DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
                    
                throw error
            }
                
            return typedItem
        }
        
        return (returnedItems, nil)
    }
    
    func getExecuteItems(partitionKeys: [String],
                         additionalWhereClause: String?) -> [PolymorphicOperationReturnTypeConvertable] {
        var items: [PolymorphicOperationReturnTypeConvertable] = []
        partitionKeys.forEach { partitionKey in
            guard let partition = self.store[partitionKey] else {
                // no such partition, continue
                return
            }
            
            partition.forEach { (sortKey, databaseItem) in
                // if there is an additional where clause
                if let additionalWhereClause = additionalWhereClause {
                    // there must be an executeItemFilter
                    if let executeItemFilter = self.executeItemFilter {
                        if executeItemFilter(partitionKey, sortKey, additionalWhereClause, databaseItem) {
                            // add if the filter says yes
                            items.append(databaseItem)
                        }
                    } else {
                        fatalError("An executeItemFilter must be provided when an excute call includes an additionalWhereClause")
                    }
                } else {
                    // otherwise just add the item
                    items.append(databaseItem)
                }
            }
        }
        
        return items
    }
}
