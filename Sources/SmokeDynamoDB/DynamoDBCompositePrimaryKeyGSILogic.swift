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
//  DynamoDBCompositePrimaryKeyGSILogic.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel
import NIO

// Provide a default `PolymorphicWriteEntry` for the `DynamoDBCompositePrimaryKeyGSILogic` for backwards compatibility
public struct NoOpPolymorphicWriteEntry: PolymorphicWriteEntry {
    public func handle<Context>(context: Context) throws -> Context.WriteEntryTransformType where Context : PolymorphicWriteEntryContext {
        fatalError("Unimplemented")
    }
}

/**
  A protocol that simulates the logic of a GSI reacting to events on the main table.
 */
public protocol DynamoDBCompositePrimaryKeyGSILogic {
    associatedtype GSIAttributesType: PrimaryKeyAttributes
    associatedtype WriteEntryType: PolymorphicWriteEntry = NoOpPolymorphicWriteEntry
    /**
     * Called when an item is inserted on the main table. Can be used to transform the provided item to the item that would be made available on the GSI.
     */
    func onInsertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) -> EventLoopFuture<Void>

    /**
     * Called when an item is clobbered on the main table. Can be used to transform the provided item to the item that would be made available on the GSI.
     */
    func onClobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                 gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) -> EventLoopFuture<Void>

    /**
     * Called when an item is updated on the main table. Can be used to transform the provided item to the item that would be made available on the GSI.
     */
    func onUpdateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) -> EventLoopFuture<Void>
 
    /**
     * Called when an item is delete on the main table. Can be used to also delete the corresponding item on the GSI.

     */
    func onDeleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>,
                                      gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) -> EventLoopFuture<Void>
    
#if (os(Linux) && compiler(>=5.5)) || (!os(Linux) && compiler(>=5.5.2)) && canImport(_Concurrency)
    /**
     * Called when an item is inserted on the main table. Can be used to transform the provided item to the item that would be made available on the GSI.
     */
    func onInsertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) async throws

    /**
     * Called when an item is clobbered on the main table. Can be used to transform the provided item to the item that would be made available on the GSI.
     */
    func onClobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                 gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) async throws

    /**
     * Called when an item is updated on the main table. Can be used to transform the provided item to the item that would be made available on the GSI.
     */
    func onUpdateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) async throws
 
    /**
     * Called when an item is delete on the main table. Can be used to also delete the corresponding item on the GSI.

     */
    func onDeleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>,
                                      gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) async throws

    /**
     * Called when an transact write in the main table. Can be used to also transact write the corresponding item on the GSI.

     */
    func onTransactWrite(_ entries: [WriteEntryType],
                         gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) async throws
#endif
}

// For async/await APIs, simply delegate to the EventLoopFuture implementation until support is dropped for Swift <5.5
public extension DynamoDBCompositePrimaryKeyGSILogic {
#if (os(Linux) && compiler(>=5.5)) || (!os(Linux) && compiler(>=5.5.2)) && canImport(_Concurrency)
    func onInsertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) async throws {
        try await onInsertItem(item, gsiDataStore: gsiDataStore).get()
    }

    /**
     * Called when an item is clobbered on the main table. Can be used to transform the provided item to the item that would be made available on the GSI.
     */
    func onClobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>,
                                                 gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) async throws {
        try await onClobberItem(item, gsiDataStore: gsiDataStore).get()
    }

    /**
     * Called when an item is updated on the main table. Can be used to transform the provided item to the item that would be made available on the GSI.
     */
    func onUpdateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                existingItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) async throws {
        try await onUpdateItem(newItem: newItem, existingItem: existingItem, gsiDataStore: gsiDataStore).get()
    }
 
    /**
     * Called when an item is delete on the main table. Can be used to also delete the corresponding item on the GSI.

     */
    func onDeleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>,
                                      gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) async throws {
        try await onDeleteItem(forKey: key, gsiDataStore: gsiDataStore).get()
    }

    // provide default for backwards compatibility
    func onTransactWrite(_ entries: [WriteEntryType],
                         gsiDataStore: InMemoryDynamoDBCompositePrimaryKeyTable) async throws {
        // do nothing
    }
#endif
}
