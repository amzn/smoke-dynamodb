// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
//  DynamoDBCompositePrimaryKeyTableV2.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import DynamoDBModel
import NIO

public protocol DynamoDBCompositePrimaryKeyTableV2 {
    // This property doesn't really belong on the protocol but provides
    // access to the property for the protocol's extensions
    var consistentRead: Bool { get }
    
#if (os(Linux) && compiler(>=5.5)) || (!os(Linux) && compiler(>=5.5.2)) && canImport(_Concurrency)
    
    /**
     * Insert item is a non-destructive API. If an item already exists with the specified key this
     * API should fail.
     */
    func insertItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) async throws

    /**
     * Clobber item is destructive API. Regardless of what is present in the database the provided
     * item will be inserted.
     */
    func clobberItem<AttributesType, ItemType>(_ item: TypedDatabaseItem<AttributesType, ItemType>) async throws

    /**
     * Update item requires having gotten an item from the database previously and will not update
     * if the item at the specified key is not the existing item provided.
     */
    func updateItem<AttributesType, ItemType>(newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                              existingItem: TypedDatabaseItem<AttributesType, ItemType>) async throws
    
    /**
     * Provides the ability to bulk write database rows
     */
    func monomorphicBulkWrite<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws

    func monomorphicBulkWriteWithoutThrowing<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws
    -> Set<BatchStatementErrorCodeEnum>
    /**
     * Retrieves an item from the database table. Returns nil if the item doesn't exist.
     */
    func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) async throws -> TypedDatabaseItem<AttributesType, ItemType>?
    
    /**
     * Retrieves items from the database table as a dictionary mapped to the provided key. Missing entries from the provided map indicate that item doesn't exist.
     */
    func getItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>]) async throws
    -> [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]

    /**
     * Removes an item from the database table. Is an idempotent operation; running it multiple times
     * on the same item or attribute does not result in an error response.
     */
    func deleteItem<AttributesType>(forKey key: CompositePrimaryKey<AttributesType>) async throws
    
    /**
     * Removes an item from the database table. Is an idempotent operation; running it multiple times
     * on the same item or attribute does not result in an error response. This operation will not modify the table
     * if the item at the specified key is not the existing item provided.
     */
    func deleteItem<AttributesType, ItemType>(existingItem: TypedDatabaseItem<AttributesType, ItemType>) async throws
    
    /**
     * Removes items from the database table. Is an idempotent operation; running it multiple times
     * on the same item or attribute does not result in an error response.
     */
    func deleteItems<AttributesType>(forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws
    
    /**
     * Removes items from the database table. Is an idempotent operation; running it multiple times
     * on the same item or attribute does not result in an error response. This operation will not modify the table
     * if the item at the specified key is not the existing item provided.
     */
    func deleteItems<ItemType: DatabaseItem>(existingItems: [ItemType]) async throws

    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will potentially make multiple calls to DynamoDB to retrieve all results for
       the query.
     */
    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             consistentRead: Bool) async throws
        -> [ReturnedType]

    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will return paginated results based on the limit and exclusiveStartKey provided.
     */
    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             limit: Int?,
                                                             exclusiveStartKey: String?,
                                                             consistentRead: Bool) async throws
        -> ([ReturnedType], String?)
    
    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will return paginated results based on the limit and exclusiveStartKey provided.
     */
    func query<ReturnedType: PolymorphicOperationReturnType>(forPartitionKey partitionKey: String,
                                                             sortKeyCondition: AttributeCondition?,
                                                             limit: Int?,
                                                             scanIndexForward: Bool,
                                                             exclusiveStartKey: String?,
                                                             consistentRead: Bool) async throws
        -> ([ReturnedType], String?)
    
    /**
     * Uses the ExecuteStatement API to perform batch reads or writes on data stored in DynamoDB, using PartiQL.
     * ExecuteStatement API has a maximum limit on the number of decomposed read operations per request. This function handles pagination internally.
     * This function will potentially make multiple calls to DynamoDB to retrieve all results.
     *
     * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ExecuteStatement.html
     */
    func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws -> [ReturnedType]
    
    /**
     * Uses the ExecuteStatement API to to perform batch reads or writes on data stored in DynamoDB, using PartiQL.
     * ExecuteStatement API has a maximum limit on the number of decomposed read operations per request.
     * Caller of this function needs to handle pagination on their side.
     *
     * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ExecuteStatement.html
     */
    func execute<ReturnedType: PolymorphicOperationReturnType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws -> ([ReturnedType], String?)
    
    // MARK: Monomorphic batch and queries
    
    /**
     * Retrieves items from the database table as a dictionary mapped to the provided key. Missing entries from the provided map indicate that item doesn't exist.
     */
    func monomorphicGetItems<AttributesType, ItemType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws
    -> [CompositePrimaryKey<AttributesType>: TypedDatabaseItem<AttributesType, ItemType>]
    
    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will potentially make multiple calls to DynamoDB to retrieve all results for
       the query.
     */
    func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                    sortKeyCondition: AttributeCondition?,
                                                    consistentRead: Bool) async throws
        -> [TypedDatabaseItem<AttributesType, ItemType>]
    
    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will return paginated results based on the limit and exclusiveStartKey provided.
     */
    func monomorphicQuery<AttributesType, ItemType>(forPartitionKey partitionKey: String,
                                                        sortKeyCondition: AttributeCondition?,
                                                        limit: Int?,
                                                        scanIndexForward: Bool,
                                                        exclusiveStartKey: String?,
                                                        consistentRead: Bool) async throws
        -> ([TypedDatabaseItem<AttributesType, ItemType>], String?)
    
    /**
     * Uses the ExecuteStatement API to to perform batch reads or writes on data stored in DynamoDB, using PartiQL.
     * ExecuteStatement API has a maximum limit on the number of decomposed read operations per request. This function handles pagination internally.
     * This function will potentially make multiple calls to DynamoDB to retrieve all results.
     *
     * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ExecuteStatement.html
     */
    func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?) async throws -> [TypedDatabaseItem<AttributesType, ItemType>]
    
    /**
     * Uses the ExecuteStatement API to to perform batch reads or writes on data stored in DynamoDB, using PartiQL.
     * ExecuteStatement API has a maximum limit on the number of decomposed read operations per request.
     * Caller of this function needs to handle pagination on their side.
     *
     * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ExecuteStatement.html
     */
    func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) async throws -> ([TypedDatabaseItem<AttributesType, ItemType>], String?)
    
#endif
    
    /**
    * This is a helper function to validate WriteEntry
    */
    func validateEntry<AttributesType, ItemType>(entry: WriteEntry<AttributesType, ItemType>) throws 
}

public extension DynamoDBCompositePrimaryKeyTableV2 {
    // provide a default value for the table's `consistentRead`
    // maintains backwards compatibility
    var consistentRead: Bool {
        return true
    }
}
