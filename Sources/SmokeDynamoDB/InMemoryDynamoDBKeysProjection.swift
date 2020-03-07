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
//  InMemoryDynamoDBKeysProjection.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel

public class InMemoryDynamoDBKeysProjection: DynamoDBKeysProjection {

    public var keys: [Any] = []

    public init(keys: [Any] = []) {
        self.keys = keys
    }

    public func querySync<AttributesType>(forPartitionKey partitionKey: String,
                                          sortKeyCondition: AttributeCondition?) throws
        -> [CompositePrimaryKey<AttributesType>] {
        var items: [CompositePrimaryKey<AttributesType>] = []
            
        let sortedKeys = keys.compactMap { $0 as? CompositePrimaryKey<AttributesType> }.sorted(by: { (left, right) -> Bool in
            return left.sortKey < right.sortKey
        })
            
        sortKeyIteration: for key in sortedKeys {
            let sortKey = key.sortKey

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

            items.append(key)
        }

        return items
    }

    public func queryAsync<AttributesType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            completion: @escaping (SmokeDynamoDBErrorResult<[CompositePrimaryKey<AttributesType>]>) -> ())
        throws where AttributesType: PrimaryKeyAttributes {
            do {
                let items: [CompositePrimaryKey<AttributesType>] =
                    try querySync(forPartitionKey: partitionKey,
                                  sortKeyCondition: sortKeyCondition)

                completion(.success(items))
            } catch {
                completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
            }
    }

    public func querySync<AttributesType>(forPartitionKey partitionKey: String,
                                          sortKeyCondition: AttributeCondition?,
                                          limit: Int?,
                                          exclusiveStartKey: String?) throws
        -> ([CompositePrimaryKey<AttributesType>], String?)
        where AttributesType: PrimaryKeyAttributes {
            // get all the results
            let items: [CompositePrimaryKey<AttributesType>] = try querySync(
                forPartitionKey: partitionKey,
                sortKeyCondition: sortKeyCondition)

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

    public func queryAsync<AttributesType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?,
            exclusiveStartKey: String?,
            completion: @escaping (SmokeDynamoDBErrorResult<([CompositePrimaryKey<AttributesType>], String?)>) -> ())
        throws where AttributesType: PrimaryKeyAttributes {
            do {
                let result: ([CompositePrimaryKey<AttributesType>], String?) =
                    try querySync(forPartitionKey: partitionKey,
                                  sortKeyCondition: sortKeyCondition,
                                  limit: limit,
                                  exclusiveStartKey: exclusiveStartKey)

                completion(.success(result))
            } catch {
                completion(.failure(error.asUnrecognizedSmokeDynamoDBError()))
            }
    }
}
