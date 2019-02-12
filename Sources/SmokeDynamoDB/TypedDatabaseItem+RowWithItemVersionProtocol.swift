// Copyright 2018-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
//  TypedDatabaseItem+RowWithItemVersionProtocol.swift
//  SmokeDynamoDB
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
        -> TypedDatabaseItem<AttributesType, RowType> {
            // if we can only update a particular version
            if let overwriteVersion = conditionalStatusVersion,
                rowValue.itemVersion != overwriteVersion {
                    throw SmokeDynamoDBError.concurrencyError(partitionKey: compositePrimaryKey.partitionKey,
                                                            sortKey: compositePrimaryKey.sortKey,
                                                            message: "Current row did not have the required version '\(overwriteVersion)'")
            }
        
            let updatedPayloadWithVersion: RowType = rowValue.createUpdatedItem(withValue: value)
            return createUpdatedItem(withValue: updatedPayloadWithVersion)
    }
}
