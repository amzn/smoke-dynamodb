// swiftlint:disable cyclomatic_complexity
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
//  InMemoryDynamoDBCompositePrimaryKeyTable+monomorphicQuery.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel

public extension InMemoryDynamoDBCompositePrimaryKeyTable {
    
    func monomorphicQuerySync<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                        sortKeyCondition: AttributeCondition?) throws
    -> [TypedDatabaseItem<AttributesType, ItemType>]
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        var items: [TypedDatabaseItem<AttributesType, ItemType>] = []

            if let partition = store[partitionKey] {
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
                        throw DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self,
                                                         context)
                    }
                }
            }

            return items
        }
    
    func monomorphicQueryAsync<AttributesType, ItemType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            completion: @escaping (SmokeDynamoDBErrorResult<[TypedDatabaseItem<AttributesType, ItemType>]>) -> ()) throws
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        do {
            let items: [TypedDatabaseItem<AttributesType, ItemType>] =
                try monomorphicQuerySync(forPartitionKey: partitionKey,
                                         sortKeyCondition: sortKeyCondition)

            completion(.success(items))
        } catch {
            completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
        }
    }
    
    func monomorphicQuerySync<AttributesType, ItemType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?,
            scanIndexForward: Bool,
            exclusiveStartKey: String?) throws
    -> ([TypedDatabaseItem<AttributesType, ItemType>], String?)
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        // get all the results
        let rawItems: [TypedDatabaseItem<AttributesType, ItemType>] = try monomorphicQuerySync(
            forPartitionKey: partitionKey,
            sortKeyCondition: sortKeyCondition)
        
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
    
    func monomorphicQueryAsync<AttributesType, ItemType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?,
            scanIndexForward: Bool,
            exclusiveStartKey: String?,
            completion: @escaping (SmokeDynamoDBErrorResult<([TypedDatabaseItem<AttributesType, ItemType>], String?)>) -> ()) throws
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        do {
            let result: ([TypedDatabaseItem<AttributesType, ItemType>], String?) =
                try monomorphicQuerySync(forPartitionKey: partitionKey,
                                         sortKeyCondition: sortKeyCondition,
                                         limit: limit,
                                         scanIndexForward: true,
                                         exclusiveStartKey: exclusiveStartKey)

            completion(.success(result))
        } catch {
            completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
        }
    }
}
