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
//  InMemoryDynamoDBCompositePrimaryKeyTable+monomorphicQuery.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel
import NIO

public extension InMemoryDynamoDBCompositePrimaryKeyTable {
    
    func monomorphicGetItems<AttributesType, ItemType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>])
    -> EventLoopFuture<[CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>]> {
        let promise = self.eventLoop.makePromise(of: [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>].self)
        
        accessQueue.async {
            var map: [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>] = [:]
            
            keys.forEach { key in
                if let partition = self.store[key.partitionKey] {

                    guard let value = partition[key.sortKey] else {
                        return
                    }
                    
                    guard let item = value as? TypedDatabaseItem<AttributesType, ItemType> else {
                        let foundType = type(of: value)
                        let description = "Expected to decode \(TypedDatabaseItem<AttributesType, ItemType>.self). Instead found \(foundType)."
                        let context = DecodingError.Context(codingPath: [], debugDescription: description)
                        let error = DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
                        
                        promise.fail(error)
                        return
                    }
                    
                    map[key] = item
                }
            }
            
            promise.succeed(map)
        }
        
        return promise.futureResult
    }
    
    func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                    sortKeyCondition: AttributeCondition?)
    -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]>
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        let promise = self.eventLoop.makePromise(of: [TypedDatabaseItem<AttributesType, ItemType>].self)
        
        accessQueue.async {
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
                        let error = DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
                        
                        promise.fail(error)
                        return
                    }
                }
            }

            promise.succeed(items)
        }
        
        return promise.futureResult
    }
    
    func monomorphicQuery<AttributesType, ItemType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?,
            scanIndexForward: Bool,
            exclusiveStartKey: String?)
            -> EventLoopFuture<([TypedDatabaseItem<AttributesType, ItemType>], String?)>
            where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        // get all the results
        return monomorphicQuery(forPartitionKey: partitionKey,
                                sortKeyCondition: sortKeyCondition)
            .map { (rawItems: [TypedDatabaseItem<AttributesType, ItemType>]) in
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
}
