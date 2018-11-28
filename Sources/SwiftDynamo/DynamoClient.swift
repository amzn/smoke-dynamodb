//
//  DynamoClient.swift
//  SwiftDynamo
//

import Foundation

/**
 Enumeration of the errors that can be thrown by a DynamoClient.
 */
public enum SwiftDynamoError: Error {
    case databaseError(reason: String)
    case conditionalCheckFailed(paritionKey: String, sortKey: String, message: String?)
    case typeMismatch(expected: String, provided: String)
    case unexpectedType(provided: String)
    case concurrencyError(partitionKey: String, sortKey: String, message: String?)
    case unableToUpdateError(reason: String)
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

public protocol DynamoClient {
    
    /**
     * Insert item is a non-destructive API. If an item already exists with the specified key this
     * API should fail.
     */
    func insertItem<RowIdentity, ItemType>(_ item: TypedDatabaseItem<RowIdentity, ItemType>) throws
    
    /**
     * Clobber item is destructive API. Regardless of what is present in the database the provided
     * item will be inserted.
     */
    func clobberItem<RowIdentity, ItemType>(_ item: TypedDatabaseItem<RowIdentity, ItemType>) throws
    
    /**
     * Update item requires having gotten an item from the database previously and will not update
     * if the item at the specified key is not the existing item provided.
     */
    func updateItem<RowIdentity, ItemType>(newItem: TypedDatabaseItem<RowIdentity, ItemType>, existingItem: TypedDatabaseItem<RowIdentity, ItemType>) throws
    
    /**
     * Retrieves an item from the database table. Returns nil if the item doesn't exist.
     */
    func getItem<RowIdentity, ItemType>(forKey key: CompositePrimaryKey<RowIdentity>) throws -> TypedDatabaseItem<RowIdentity, ItemType>?
    
    /**
     * Removes an item from the database table. Is an idempotent operation; running it multiple times
     * on the same item or attribute does not result in an error response.
     */
    func deleteItem<RowIdentity>(forKey key: CompositePrimaryKey<RowIdentity>) throws
    
    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will potentially make multiple calls to dynamo to retrieve all results for
       the query.
     */
    func query<RowIdentity, PossibleTypes>(forPartitionKey partitionKey: String,
                                           sortKeyCondition: AttributeCondition?) throws
        -> [PolymorphicDatabaseItem<RowIdentity, PossibleTypes>]
    
    /**
     * Queries a partition in the database table and optionally a sort key condition. If the
       partition doesn't exist, this operation will return an empty list as a response. This
       function will return paginated results based on the limit and exclusiveStartKey provided.
     */
    func query<RowIdentity, PossibleTypes>(forPartitionKey partitionKey: String,
                                           sortKeyCondition: AttributeCondition?,
                                           limit: Int,
                                           exclusiveStartKey: String?) throws
        -> ([PolymorphicDatabaseItem<RowIdentity, PossibleTypes>], String?)
}
