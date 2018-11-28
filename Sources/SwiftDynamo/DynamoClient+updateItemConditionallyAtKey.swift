//
//  DynamoClient+updateItemConditionallyAtKey.swift
//  SwiftDynamo
//

import Foundation

import LoggerAPI

public extension DynamoClient {
    /**
     Method to conditionally update an item at the specified key for a number of retries.
     This method is useful for database rows that may be updated simultaneously by different clients
     and each client will only attempt to update based on the current row value.
     On each attempt, the updatedPayloadProvider will be passed the current row value. It can either
     generate an updated payload or fail with an error if an updated payload is not valid. If an updated
     payload is returned, this method will attempt to update the row. This update may fail due to
     concurrency, in which case the process will repeat until the retry limit has been reached.
 
     - Parameters:
         _: the key of the item to update
         withRetries: the number of times to attempt to retry the update before failing.
         updatedPayloadProvider: the provider that will return updated payloads.
     */
    public func updateItemConditionallyAtKey<RowIdentity, ItemType: Codable>(
        _ key: CompositePrimaryKey<RowIdentity>,
        withRetries retries: Int = 10,
        updatedPayloadProvider: (ItemType) throws -> ItemType) throws {
        
        guard retries > 0 else {
            throw SwiftDynamoError.concurrencyError(partitionKey: key.partitionKey,
                                                    sortKey: key.sortKey,
                                                    message: "Unable to complete request to update versioned item in specified number of attempts")
        }
        
        guard let databaseItem: TypedDatabaseItem<RowIdentity, ItemType> = try getItem(forKey: key) else {
            throw SwiftDynamoError.conditionalCheckFailed(paritionKey: key.partitionKey,
                                                          sortKey: key.sortKey,
                                                          message: "Item not present in database.")
        }
        
        let updatedPayload = try updatedPayloadProvider(databaseItem.rowValue)
        
        let updatedDatabaseItem = databaseItem.createUpdatedItem(withValue: updatedPayload)
        
        do {
            try updateItem(newItem: updatedDatabaseItem, existingItem: databaseItem)
        } catch SwiftDynamoError.conditionalCheckFailed(_) {
            return try updateItemConditionallyAtKey(key,
                                                    withRetries: retries - 1,
                                                    updatedPayloadProvider: updatedPayloadProvider)
        }
    }
}
