//
//  TypedDatabaseItem+RowWithItemVersionProtocol.swift
//  SwiftDynamo
//

import Foundation

/// An extension for TypedDatabaseItem that is constrained by the RowType conforming
/// to RowWithItemVersionProtocol
extension TypedDatabaseItem where RowType: RowWithItemVersionProtocol {
    /// Helper function wrapping createUpdatedItem that will verify if
    /// conditionalStatusVersion is provided that it matches the version
    /// of the current item
    public func createUpdatedRowWithItemVersion(withValue value: RowType.RowType,
                                                conditionalStatusVersion: Int?) throws
        -> TypedDatabaseItem<RowIdentity, RowType> {
            // if we can only update a particular version
            if let overwriteVersion = conditionalStatusVersion,
                rowValue.itemVersion != overwriteVersion {
                    throw SwiftDynamoError.concurrencyError(partitionKey: compositePrimaryKey.partitionKey,
                                                            sortKey: compositePrimaryKey.sortKey,
                                                            message: "Current row did not have the required version '\(overwriteVersion)'")
            }
        
            let updatedPayloadWithVersion: RowType = rowValue.createUpdatedItem(withValue: value)
            return createUpdatedItem(withValue: updatedPayloadWithVersion)
    }
}
