// Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
//  RowWithItemVersionProtocol.swift
//  SmokeDynamoDB
//

import Foundation

/**
 Protocol for a item payload wrapper that declares an item version.
 Primarily required to allow the constrained extension below.
 */
public protocol RowWithItemVersionProtocol {
    associatedtype RowType: Codable
    
    /// The item version number
    var itemVersion: Int { get }
    /// The item payload
    var rowValue: RowType { get }
    
    /// Function that accepts a version and an updated row version and returns
    /// an instance of the implementing type
    func createUpdatedItem(withVersion itemVersion: Int?,
                           withValue newRowValue: RowType) -> Self
    
    /// Function that accepts an updated row version and returns
    /// an instance of the implementing type
    func createUpdatedItem(withValue newRowValue: RowType) -> Self
}

public extension RowWithItemVersionProtocol {
    /// Default implementation that delegates to createUpdatedItem(withVersion:withValue:)
    func createUpdatedItem(withValue newRowValue: RowType) -> Self {
        return createUpdatedItem(withVersion: nil, withValue: newRowValue)
    }
}

/// Declare conformance of RowWithItemVersion to RowWithItemVersionProtocol
extension RowWithItemVersion: RowWithItemVersionProtocol {
    
}
