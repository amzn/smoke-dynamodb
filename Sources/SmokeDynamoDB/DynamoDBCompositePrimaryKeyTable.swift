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
//  DynamoDBCompositePrimaryKeyTable.swift
//  SmokeDynamoDB
//

import Foundation
import AWSDynamoDB
import ClientRuntime

/**
 Enumeration of the errors that can be thrown by a DynamoDBTable.
 */
public enum SmokeDynamoDBError: Error {
    case databaseError(reason: String)
    case unexpectedError(cause: Swift.Error)
    case unexpectedResponse(reason: String)
    case conditionalCheckFailed(partitionKey: String, sortKey: String, message: String?)
    case duplicateItem(partitionKey: String?, sortKey: String?, message: String?)
    case typeMismatch(expected: String, provided: String)
    case unexpectedType(provided: String)
    case concurrencyError(partitionKey: String, sortKey: String, message: String?)
    case unableToUpdateError(reason: String)
    case unrecognizedError(String, String?)
    case multipleUnexpectedErrors(cause: [Swift.Error])
    case batchAPIExceededRetries(retryCount: Int)
    case validationError(reason: String)
    case batchErrorsReturned(errorCount: Int, messageMap: [String: Int])
    case statementLengthExceeded(reason: String)
    case transactionSizeExceeded(attemptedSize: Int, maximumSize: Int)
    case transactionConflict(message: String?)
    case transactionProvisionedThroughputExceeded(message: String?)
    case transactionThrottling(message: String?)
    case transactionConditionalCheckFailed(partitionKey: String?, sortKey: String?, message: String?)
    case transactionValidation(partitionKey: String?, sortKey: String?, message: String?)
    case transactionUnknown(code: String?, partitionKey: String?, sortKey: String?, message: String?)
    case transactionCanceled(reasons: [SmokeDynamoDBError])
}

public typealias SmokeDynamoDBErrorResult<SuccessPayload> = Result<SuccessPayload, SmokeDynamoDBError>

public extension Swift.Error {
    func asUnrecognizedSmokeDynamoDBError() -> SmokeDynamoDBError {
        let errorType = String(describing: type(of: self))
        let errorDescription = String(describing: self)
        return .unrecognizedError(errorType, errorDescription)
    }
}

/**
 Enumeration of the types of conditions that can be specified for an attribute.
 */
public enum AttributeCondition {
    case equals(String)
    case lessThan(String)
    case lessThanOrEqual(String)
    case greaterThan(String)
    case greaterThanOrEqual(String)
    case between(String, String)
    case beginsWith(String)
}

public enum WriteEntry<AttributesType: PrimaryKeyAttributes, ItemType: Codable> {
    case update(new: TypedDatabaseItem<AttributesType, ItemType>, existing: TypedDatabaseItem<AttributesType, ItemType>)
    case insert(new: TypedDatabaseItem<AttributesType, ItemType>)
    case deleteAtKey(key: CompositePrimaryKey<AttributesType>)
    case deleteItem(existing: TypedDatabaseItem<AttributesType, ItemType>)
    
    public var compositePrimaryKey: CompositePrimaryKey<AttributesType> {
        switch self {
        case .update(new: let new, existing: _):
            return new.compositePrimaryKey
        case .insert(new: let new):
            return new.compositePrimaryKey
        case .deleteAtKey(key: let key):
            return key
        case .deleteItem(existing: let existing):
            return existing.compositePrimaryKey
        }
    }
}

public typealias StandardWriteEntry<ItemType: Codable> = WriteEntry<StandardPrimaryKeyAttributes, ItemType>

public protocol DynamoDBCompositePrimaryKeyTable {
    // This property doesn't really belong on the protocol but provides
    // access to the property for the protocol's extensions
    var consistentRead: Bool { get }

    /**
     * PartiQL string attributes cannot contain single quotes. Otherwise, PartiQL statement is consider to be invalid.
     * This property controls if single quotes are escaped while formatting PartiQL statements.
     */
    var escapeSingleQuoteInPartiQL: Bool { get }

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
    
    func monomorphicBulkWriteWithFallback<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws
    
    func monomorphicBulkWriteWithoutThrowing<AttributesType, ItemType>(_ entries: [WriteEntry<AttributesType, ItemType>]) async throws
    -> Set<DynamoDBClientTypes.BatchStatementErrorCodeEnum>

    /**
     * Retrieves an item from the database table. Returns nil if the item doesn't exist.
     */
    func getItem<AttributesType, ItemType>(forKey key: CompositePrimaryKey<AttributesType>) async throws
    -> TypedDatabaseItem<AttributesType, ItemType>?
    
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
    -> (items: [ReturnedType], lastEvaluatedKey: String?)
    
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
    -> (items: [ReturnedType], lastEvaluatedKey: String?)
    
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
        additionalWhereClause: String?) async throws 
    -> [ReturnedType]
    
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
        additionalWhereClause: String?, nextToken: String?) async throws 
    -> (items: [ReturnedType], lastEvaluatedKey: String?)
    
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
    -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?)
    
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
        additionalWhereClause: String?) async throws 
    -> [TypedDatabaseItem<AttributesType, ItemType>]
    
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
        additionalWhereClause: String?, nextToken: String?) async throws
    -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?)

    /**
    * This is a helper function to validate WriteEntry
    */
    func validateEntry<AttributesType, ItemType>(entry: WriteEntry<AttributesType, ItemType>) throws 
        
    /**
     * Provides the ability to bulk write database rows in a transaction.
     * The transaction will comprise of the write entries specified in `entries`.
     * The transaction will fail if the number of entries is greater than 100.
     */
    func transactWrite<WriteEntryType: PolymorphicWriteEntry>(
        _ entries: [WriteEntryType]) async throws
    
    /**
     * Provides the ability to bulk write database rows in a transaction.
     * The transaction will comprise of the write entries specified in `entries`.
     * The transaction will be cancelled if the constraints specified in `constraints` are not met (for example you can specify that an item
     * with a specified version must exist regardless of if it will be written to by the transaction).
     * The transaction will fail if the number of entries and constraints combined is greater than 100.
     */
    func transactWrite<WriteEntryType: PolymorphicWriteEntry, TransactionConstraintEntryType: PolymorphicTransactionConstraintEntry>(
        _ entries: [WriteEntryType], constraints: [TransactionConstraintEntryType]) async throws
    
    func bulkWrite<WriteEntryType: PolymorphicWriteEntry>(_ entries: [WriteEntryType]) async throws
}

public extension DynamoDBCompositePrimaryKeyTable {
    // provide a default value for the table's `consistentRead`
    // maintains backwards compatibility
    var consistentRead: Bool {
        return true
    }

    // provide a default value for the table's `escapeSingleQuoteInPartiQL`
    // maintains backwards compatibility
    var escapeSingleQuoteInPartiQL: Bool {
        return false
    }
}
