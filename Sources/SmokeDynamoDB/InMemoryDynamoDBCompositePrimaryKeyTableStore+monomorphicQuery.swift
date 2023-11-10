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
//  InMemoryDynamoDBCompositePrimaryKeyTableStore+monomorphicQuery.swift
//  SmokeDynamoDB
//

import Foundation
import AWSDynamoDB

extension InMemoryDynamoDBCompositePrimaryKeyTableStore {
    
    func monomorphicGetItems<AttributesType, ItemType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>]) throws
    -> [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>] {
        var map: [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>] = [:]
        
        try keys.forEach { key in
            if let partition = self.store[key.partitionKey] {

                guard let value = partition[key.sortKey] else {
                    return
                }
                
                guard let item = value as? TypedDatabaseItem<AttributesType, ItemType> else {
                    let foundType = type(of: value)
                    let description = "Expected to decode \(TypedDatabaseItem<AttributesType, ItemType>.self). Instead found \(foundType)."
                    let context = DecodingError.Context(codingPath: [], debugDescription: description)
                    
                    throw DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
                }
                
                map[key] = item
            }
        }
        
        return map
    }
    
    func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                    sortKeyCondition: AttributeCondition?,
                                                    consistentRead: Bool) throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        var items: [TypedDatabaseItem<AttributesType, ItemType>] = []

        if let partition = self.store[partitionKey] {
            let sortedPartition = partition.sorted(by: { (left, right) -> Bool in
                return left.key < right.key
            })
            
            sortKeyIteration: for (sortKey, value) in sortedPartition {
                if let currentSortKeyCondition = sortKeyCondition {
                    switch currentSortKeyCondition {
                    case .equals(let value):
                        if !(value == sortKey) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .lessThan(let value):
                        if !(sortKey < value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .lessThanOrEqual(let value):
                        if !(sortKey <= value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .greaterThan(let value):
                        if !(sortKey > value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .greaterThanOrEqual(let value):
                        if !(sortKey >= value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .between(let value1, let value2):
                        if !(sortKey > value1 && sortKey < value2) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .beginsWith(let value):
                        if !(sortKey.hasPrefix(value)) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    }
                }

                if let typedValue = value as? TypedDatabaseItem<AttributesType, ItemType> {
                    items.append(typedValue)
                } else {
                    let description = "Expected type \(TypedDatabaseItem<AttributesType, ItemType>.self), "
                        + " was \(type(of: value))."
                    let context = DecodingError.Context(codingPath: [], debugDescription: description)
                    
                    throw DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
                }
            }
        }

        return items
    }
    
    func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                    sortKeyCondition: AttributeCondition?,
                                                    limit: Int?,
                                                    scanIndexForward: Bool,
                                                    exclusiveStartKey: String?,
                                                    consistentRead: Bool) throws
    -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?) {
        // get all the results
        let rawItems: [TypedDatabaseItem<AttributesType, ItemType>] = try monomorphicQuery(forPartitionKey: partitionKey,
                                                                                           sortKeyCondition: sortKeyCondition,
                                                                                           consistentRead: consistentRead)
        
        let items: [TypedDatabaseItem<AttributesType, ItemType>]
        if !scanIndexForward {
            items = rawItems.reversed()
        } else {
            items = rawItems
        }

        let startIndex: Int
        // if there is an exclusiveStartKey
        if let exclusiveStartKey = exclusiveStartKey {
            guard let storedStartIndex = Int(exclusiveStartKey) else {
                fatalError("Unexpectedly encoded exclusiveStartKey '\(exclusiveStartKey)'")
            }

            startIndex = storedStartIndex
        } else {
            startIndex = 0
        }

        let endIndex: Int
        let lastEvaluatedKey: String?
        if let limit = limit, startIndex + limit < items.count {
            endIndex = startIndex + limit
            lastEvaluatedKey = String(endIndex)
        } else {
            endIndex = items.count
            lastEvaluatedKey = nil
        }
        
        return (Array(items[startIndex..<endIndex]), lastEvaluatedKey)
    }
}
