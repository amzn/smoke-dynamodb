//
//  SimulateConcurrencyDynamoClient.swift
//  SwiftDynamo
//

import Foundation

/**
 Implementation of the DynamoClient protocol that simulates concurrent access
 to a database by incrementing a row's version every time it is added for
 a specified number of requests.
 */
public class SimulateConcurrencyDynamoClient: DynamoClient {
    let wrappedDynamoClient: DynamoClient
    let simulateConcurrencyModifications: Int
    var previousConcurrencyModifications: Int
    let simulateOnInsertItem: Bool
    let simulateOnUpdateItem: Bool
    
    /**
     Initializer.
 
     - Parameters:
        - wrappedDynamoClient: The underlying DynamoClient used by this implementation.
        - simulateConcurrencyModifications: the number of get requests to simulate concurrency for.
        - simulateOnInsertItem: if this instance should simulate concurrency on insertItem.
        - simulateOnUpdateItem: if this instance should simulate concurrency on updateItem.
     */
    public init(wrappedDynamoClient: DynamoClient, simulateConcurrencyModifications: Int,
                simulateOnInsertItem: Bool = true, simulateOnUpdateItem: Bool = true) {
        self.wrappedDynamoClient = wrappedDynamoClient
        self.simulateConcurrencyModifications = simulateConcurrencyModifications
        self.previousConcurrencyModifications = 0
        self.simulateOnInsertItem = simulateOnInsertItem
        self.simulateOnUpdateItem = simulateOnUpdateItem
    }
    
    public func insertItem<RowIdentity, ItemType>(_ item: TypedDatabaseItem<RowIdentity, ItemType>) throws {
        // if there are still modifications to be made and there is an existing row
        if simulateOnInsertItem && previousConcurrencyModifications < simulateConcurrencyModifications {
            // insert an item so the conditional check will fail
            try wrappedDynamoClient.insertItem(item)
            previousConcurrencyModifications += 1
        }
        
        // then delegate to the wrapped implementation
        try wrappedDynamoClient.insertItem(item)
    }
    
    public func clobberItem<RowIdentity, ItemType>(_ item: TypedDatabaseItem<RowIdentity, ItemType>) throws {
        try wrappedDynamoClient.clobberItem(item)
    }
    
    public func updateItem<RowIdentity, ItemType>(newItem: TypedDatabaseItem<RowIdentity, ItemType>, existingItem: TypedDatabaseItem<RowIdentity, ItemType>) throws {
        
        // if there are still modifications to be made and there is an existing row
        if simulateOnUpdateItem && previousConcurrencyModifications < simulateConcurrencyModifications {
            try wrappedDynamoClient.updateItem(newItem: existingItem.createUpdatedItem(withValue: existingItem.rowValue), existingItem: existingItem)
            previousConcurrencyModifications += 1
        }
        
        // then delegate to the wrapped implementation
        try wrappedDynamoClient.updateItem(newItem: newItem, existingItem: existingItem)
    }
    
    public func getItem<RowIdentity, ItemType>(forKey key: CompositePrimaryKey<RowIdentity>) throws
        -> TypedDatabaseItem<RowIdentity, ItemType>? {
            // simply delegate to the wrapped implementation
            return try wrappedDynamoClient.getItem(forKey: key)
    }
    
    public func deleteItem<RowIdentity>(forKey key: CompositePrimaryKey<RowIdentity>) throws {
        // simply delegate to the wrapped implementation
        try wrappedDynamoClient.deleteItem(forKey: key)
    }
    
    public func query<RowIdentity, PossibleTypes>(forPartitionKey partitionKey: String,
                                                  sortKeyCondition: AttributeCondition?) throws
        -> [PolymorphicDatabaseItem<RowIdentity, PossibleTypes>]
        where RowIdentity: DynamoRowIdentity, PossibleTypes: PossibleItemTypes {
            // simply delegate to the wrapped implementation
            return try wrappedDynamoClient.query(forPartitionKey: partitionKey,
                                                 sortKeyCondition: sortKeyCondition)
    }
    
    public func query<RowIdentity, PossibleTypes>(forPartitionKey partitionKey: String,
                                                  sortKeyCondition: AttributeCondition?,
                                                  limit: Int,
                                                  exclusiveStartKey: String?) throws
        -> ([PolymorphicDatabaseItem<RowIdentity, PossibleTypes>], String?)
        where RowIdentity : DynamoRowIdentity, PossibleTypes : PossibleItemTypes {
            // simply delegate to the wrapped implementation
            return try wrappedDynamoClient.query(forPartitionKey: partitionKey,
                                                 sortKeyCondition: sortKeyCondition,
                                                 limit: limit,
                                                 exclusiveStartKey: exclusiveStartKey)
    }
}
