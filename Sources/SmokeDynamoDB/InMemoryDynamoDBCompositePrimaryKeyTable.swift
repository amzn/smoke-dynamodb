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
//  InMemoryDynamoDBCompositePrimaryKeyTable.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel
import NIO

public protocol PolymorphicOperationReturnTypeConvertable {
    var createDate: Foundation.Date { get }
    var rowStatus: RowStatus { get }
    
    var rowTypeIdentifier: String { get }
}

extension TypedDatabaseItem: PolymorphicOperationReturnTypeConvertable {
    public var rowTypeIdentifier: String {
        return getTypeRowIdentifier(type: RowType.self)
    }
}

public typealias ExecuteItemFilterType = (String, String, String, PolymorphicOperationReturnTypeConvertable)
    -> Bool

public class InMemoryDynamoDBCompositePrimaryKeyTable: DynamoDBCompositePrimaryKeyTable {

    public let eventLoop: EventLoop
    internal let storeWrapper: InMemoryDynamoDBCompositePrimaryKeyTableStore
    
    public var store: [String: [String: PolymorphicOperationReturnTypeConvertable]] {
        do {
            return try storeWrapper.getStore(eventLoop: self.eventLoop).wait()
        } catch {
            fatalError("Unable to retrieve InMemoryDynamoDBCompositePrimaryKeyTable store.")
        }
    }
    
    public init(eventLoop: EventLoop,
                executeItemFilter: ExecuteItemFilterType? = nil) {
        self.eventLoop = eventLoop
        self.storeWrapper = InMemoryDynamoDBCompositePrimaryKeyTableStore(executeItemFilter: executeItemFilter)
    }
    
    internal init(eventLoop: EventLoop,
                  storeWrapper: InMemoryDynamoDBCompositePrimaryKeyTableStore) {
        self.eventLoop = eventLoop
        self.storeWrapper = storeWrapper
    }
    
    public func on(eventLoop: EventLoop) -> InMemoryDynamoDBCompositePrimaryKeyTable {
        return InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop,
                                                        storeWrapper: self.storeWrapper)
    }

    public func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void> {
        return storeWrapper.insertItem(item, eventLoop: self.eventLoop)
    }

    public func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void> {
        return storeWrapper.clobberItem(item, eventLoop: self.eventLoop)
    }

    public func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                     existingItem: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void> {
        return storeWrapper.updateItem(newItem: newItem, existingItem: existingItem, eventLoop: self.eventLoop)
    }
    
    public func bulkWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>])
    -> EventLoopFuture<Void> {
        return storeWrapper.bulkWrite(entries, eventLoop: self.eventLoop)
    }

    public func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>)
    -> EventLoopFuture<TypedDatabaseItem<AttributesType, ItemType>?> {
        return storeWrapper.getItem(forKey: key, eventLoop: self.eventLoop)
    }
    
    public func getItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>])
    -> EventLoopFuture<[CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]> {
        return storeWrapper.getItems(forKeys: keys, eventLoop: self.eventLoop)
    }

    public func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) -> EventLoopFuture<Void> {
        return storeWrapper.deleteItem(forKey: key, eventLoop: self.eventLoop)
    }
    
    public func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) -> EventLoopFuture<Void> {
        return storeWrapper.deleteItems(forKeys: keys, eventLoop: self.eventLoop)
    }
    
    public func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType]) -> EventLoopFuture<Void> {
        return storeWrapper.deleteItems(existingItems: existingItems, eventLoop: self.eventLoop)
    }
    
    public func deleteItem<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) -> EventLoopFuture<Void>
            where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        return storeWrapper.deleteItem(existingItem: existingItem, eventLoop: self.eventLoop)
    }

    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?)
    -> EventLoopFuture<[ReturnedType]> {
        return storeWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition, eventLoop: self.eventLoop)
    }
    
    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    limit: Int?,
                                                                    exclusiveStartKey: String?)
    -> EventLoopFuture<([ReturnedType], String?)> {
        return storeWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                  limit: limit, exclusiveStartKey: exclusiveStartKey, eventLoop: self.eventLoop)
    }

    public func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                                    sortKeyCondition: AttributeCondition?,
                                                                    limit: Int?,
                                                                    scanIndexForward: Bool,
                                                                    exclusiveStartKey: String?)
    -> EventLoopFuture<([ReturnedType], String?)> {
        return storeWrapper.query(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                  limit: limit, scanIndexForward: scanIndexForward,
                                  exclusiveStartKey: exclusiveStartKey, eventLoop: eventLoop)
    }
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?) -> EventLoopFuture<[ReturnedType]> {
        return storeWrapper.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                    additionalWhereClause: additionalWhereClause, eventLoop: self.eventLoop)
    }
    
    public func execute<ReturnedType: PolymorphicOperationReturnType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?,
            nextToken: String?) -> EventLoopFuture<([ReturnedType], String?)> {
        return storeWrapper.execute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                    additionalWhereClause: additionalWhereClause, nextToken: nextToken, eventLoop: self.eventLoop)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?)
    -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]> {
        return storeWrapper.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                               additionalWhereClause: additionalWhereClause, eventLoop: self.eventLoop)
    }
    
    public func monomorphicExecute<AttributesType, ItemType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?,
            nextToken: String?)
    -> EventLoopFuture<([TypedDatabaseItem<AttributesType, ItemType>], String?)> {
        return storeWrapper.monomorphicExecute(partitionKeys: partitionKeys, attributesFilter: attributesFilter,
                                               additionalWhereClause: additionalWhereClause, nextToken: nextToken, eventLoop: self.eventLoop)
    }
    
    public func monomorphicGetItems<AttributesType, ItemType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>])
    -> EventLoopFuture<[CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>]> {
        return storeWrapper.monomorphicGetItems(forKeys: keys, eventLoop: self.eventLoop)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                    sortKeyCondition: AttributeCondition?)
    -> EventLoopFuture<[TypedDatabaseItem<AttributesType, ItemType>]>
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        return storeWrapper.monomorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                             eventLoop: self.eventLoop)
    }
    
    public func monomorphicQuery<AttributesType, ItemType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?,
            scanIndexForward: Bool,
            exclusiveStartKey: String?)
    -> EventLoopFuture<([TypedDatabaseItem<AttributesType, ItemType>], String?)>
    where AttributesType : PrimaryKeyAttributes, ItemType : Decodable, ItemType : Encodable {
        return storeWrapper.monomorphicQuery(forPartitionKey: partitionKey, sortKeyCondition: sortKeyCondition,
                                             limit: limit, scanIndexForward: scanIndexForward,
                                             exclusiveStartKey: exclusiveStartKey, eventLoop: self.eventLoop)
    }
}
