//
//  RowWithItemVersionProtocol.swift
//  SwiftDynamo
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
extension RowWithItemVersion : RowWithItemVersionProtocol {
    
}
