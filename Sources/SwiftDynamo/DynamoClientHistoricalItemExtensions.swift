//
//  DynamoClientHistoricalItemExtensions
//  Extensions which enable historical item multi-row update usecases.
//
//  Created by Van Pelt, Samuel on 3/8/18.
//

import Foundation
import LoggerAPI

public extension DynamoClient {
    
    /**
     * Historical items exist across multiple rows. This method provides an interface to record all
     * rows in a single call.
     */
    public func insertItemWithHistoricalRow<RowIdentity, ItemType>(primaryItem: TypedDatabaseItem<RowIdentity, ItemType>,
                                                                   historicalItem: TypedDatabaseItem<RowIdentity, ItemType>) throws {
        try insertItem(primaryItem)
        try insertItem(historicalItem)
    }
    
    public func updateItemWithHistoricalRow<RowIdentity, ItemType>(primaryItem: TypedDatabaseItem<RowIdentity, ItemType>,
                                                                   existingItem: TypedDatabaseItem<RowIdentity, ItemType>,
                                                                   historicalItem: TypedDatabaseItem<RowIdentity, ItemType>) throws {
        try updateItem(newItem: primaryItem, existingItem: existingItem)
        try insertItem(historicalItem)
    }
    
    
    
    /**
     * Clobbering a historical item requires knowledge of existing rows to accurately record
     * historical data.
     */
    public func clobberItemWithHistoricalRow<RowIdentity, ItemType>(primaryItemProvider: (TypedDatabaseItem<RowIdentity, ItemType>?) -> TypedDatabaseItem<RowIdentity, ItemType>,
                                                                    historicalItemProvider: (TypedDatabaseItem<RowIdentity, ItemType>) -> TypedDatabaseItem<RowIdentity, ItemType>,
                                                                    withRetries retries: Int = 10
        ) throws {
        
        let primaryItem = primaryItemProvider(nil)
        
        guard retries > 0 else {
            throw SwiftDynamoError.concurrencyError(partitionKey: primaryItem.compositePrimaryKey.partitionKey, sortKey: primaryItem.compositePrimaryKey.sortKey,
                                                    message: "Unable to complete request to clobber versioned item in specified number of attempts")
        }
        
        if let existingItem: TypedDatabaseItem<RowIdentity, ItemType> = try getItem(forKey: primaryItem.compositePrimaryKey) {
            
            let newItem : TypedDatabaseItem<RowIdentity, ItemType> = primaryItemProvider(existingItem)
            
            do {
                try updateItemWithHistoricalRow(primaryItem: newItem, existingItem: existingItem, historicalItem: historicalItemProvider(newItem))
            } catch SwiftDynamoError.conditionalCheckFailed(_) {
                try clobberItemWithHistoricalRow(primaryItemProvider: primaryItemProvider,
                                                 historicalItemProvider: historicalItemProvider,
                                                 withRetries: retries - 1)
            }
        } else {
            do {
                try insertItemWithHistoricalRow(primaryItem: primaryItem,
                                                historicalItem: historicalItemProvider(primaryItem))
            } catch SwiftDynamoError.conditionalCheckFailed(_) {
                try clobberItemWithHistoricalRow(primaryItemProvider: primaryItemProvider,
                                                 historicalItemProvider: historicalItemProvider,
                                                 withRetries: retries - 1)
            }
        }
    }
    
    /**
     Helper function to update a versioned row.
 
     - Parameters:
        - compositePrimaryKey: The composite key for the version to update.
        - primaryItemProvider: Function to provide the updated item or throw if the current item can't be updated.
        - historicalItemProvider: Function tp provide the historical item for the primary item.
     */
    public func conditionallyUpdateItemWithHistoricalRow<RowIdentity, ItemType>(
        compositePrimaryKey: CompositePrimaryKey<RowIdentity>,
        primaryItemProvider: (TypedDatabaseItem<RowIdentity, ItemType>) throws -> TypedDatabaseItem<RowIdentity, ItemType>,
        historicalItemProvider: (TypedDatabaseItem<RowIdentity, ItemType>) -> TypedDatabaseItem<RowIdentity, ItemType>,
        withRetries retries: Int = 10) throws {
        
        guard retries > 0 else {
            throw SwiftDynamoError.concurrencyError(partitionKey: compositePrimaryKey.partitionKey,
                                                    sortKey: compositePrimaryKey.sortKey,
                                                    message: "Unable to complete request to update versioned item in specified number of attempts")
        }
        
        // get the existing item
        guard let existingItem: TypedDatabaseItem<RowIdentity, ItemType> =
            try getItem(forKey: compositePrimaryKey) else {
                throw SwiftDynamoError.conditionalCheckFailed(paritionKey: compositePrimaryKey.partitionKey,
                                                              sortKey: compositePrimaryKey.sortKey,
                                                              message: "Item not present in database.")
        }
        
        let updatedItem = try primaryItemProvider(existingItem)
        let historicalItem = historicalItemProvider(updatedItem)
        
        do {
            try updateItemWithHistoricalRow(primaryItem: updatedItem,
                                            existingItem: existingItem,
                                            historicalItem: historicalItem)
        } catch SwiftDynamoError.conditionalCheckFailed {
            // try again
            return try conditionallyUpdateItemWithHistoricalRow(compositePrimaryKey: compositePrimaryKey,
                                                                primaryItemProvider: primaryItemProvider,
                                                                historicalItemProvider: historicalItemProvider,
                                                                withRetries: retries - 1)
        }
    }
}
