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
//  DynamoDBCompositePrimaryKeyTable+clobberVersionedItemWithHistoricalRow.swift
//  SmokeDynamoDB
//

import Foundation

public extension DynamoDBCompositePrimaryKeyTable {
    /**
     * This operation provide a mechanism for managing mutable database rows
     * and storing all previous versions of that row in a historical partition.
     * This operation store the primary item under a "version zero" sort key
     * with a payload that replicates the current version of the row. This
     * historical partition contains rows for each version, including the
     * current version under a sort key for that version.
     
     - Parameters:
        - partitionKey: the partition key to use for the primary (v0) item
        - historicalKey: the partition key to use for the historical items
        - item: the payload for the new version of the primary item row
        - AttributesType: the row identity type
        - generateSortKey: generator to provide a sort key for a provided
                           version number.
     - completion: completion handler providing an error that was thrown or nil
     */
    func clobberVersionedItemWithHistoricalRow<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(
        forPrimaryKey partitionKey: String,
        andHistoricalKey historicalKey: String,
        item: ItemType,
        primaryKeyType: AttributesType.Type,
        readableTableOverrides: ReadableTableOverrides? = nil,
        writableTableOverrides: WritableTableOverrides? = nil,
        generateSortKey: @escaping (Int) -> String) async throws {
            func primaryItemProvider(_ existingItem: TypedDatabaseItem<AttributesType, RowWithItemVersion<ItemType>>?)
                -> TypedDatabaseItem<AttributesType, RowWithItemVersion<ItemType>> {
                    if let existingItem = existingItem {
                        // If an item already exists, the inserted item should be created
                        // from that item (to get an accurate version number)
                        // with the payload from the default item.
                        let overWrittenItemRowValue = existingItem.rowValue.createUpdatedItem(
                            withVersion: existingItem.rowValue.itemVersion + 1,
                            withValue: item)
                        return existingItem.createUpdatedItem(withValue: overWrittenItemRowValue)
                    }
                    
                    // If there is no existing item to be overwritten, a new item should be constructed.
                    let newItemRowValue = RowWithItemVersion.newItem(withValue: item)
                    let defaultKey = CompositePrimaryKey<AttributesType>(partitionKey: partitionKey, sortKey: generateSortKey(0))
                    return TypedDatabaseItem.newItem(withKey: defaultKey, andValue: newItemRowValue)
            }
        
            func historicalItemProvider(_ primaryItem: TypedDatabaseItem<AttributesType, RowWithItemVersion<ItemType>>)
                -> TypedDatabaseItem<AttributesType, RowWithItemVersion<ItemType>> {
                    let sortKey = generateSortKey(primaryItem.rowValue.itemVersion)
                    let key = CompositePrimaryKey<AttributesType>(partitionKey: historicalKey,
                                                               sortKey: sortKey)
                    return TypedDatabaseItem.newItem(withKey: key, andValue: primaryItem.rowValue)
            }
        
            return try await clobberItemWithHistoricalRow(readableTableOverrides: readableTableOverrides,
                                                          writableTableOverrides: writableTableOverrides,
                                                          primaryItemProvider: primaryItemProvider,
                                                          historicalItemProvider: historicalItemProvider)
    }
}
