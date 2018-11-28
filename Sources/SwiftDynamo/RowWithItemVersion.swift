//
//  RowWithItemVersion.swift
//  SwiftDynamo
//

import Foundation

public struct RowWithItemVersion<RowType: Codable> : Codable, DynamoDbCustomRowIdentity {

    public static var identity: String? {
        let rowTypeIdentity = getTypeRowIdentity(type: RowType.self)
        
        return "\(rowTypeIdentity)WithItemVersion"
    }
    
    enum CodingKeys: String, CodingKey {
        case itemVersion = "ItemVersion"
    }
    
    public let itemVersion: Int
    public let rowValue: RowType
    
    public static func newItem(withVersion itemVersion: Int = 1,
                               withValue rowValue: RowType) -> RowWithItemVersion<RowType> {
        return RowWithItemVersion<RowType>(itemVersion: itemVersion,
                                           rowValue: rowValue)
    }
    
    public func createUpdatedItem(withVersion itemVersion: Int? = nil,
                                  withValue newRowValue: RowType) -> RowWithItemVersion<RowType> {
        return RowWithItemVersion<RowType>(itemVersion: itemVersion != nil ? itemVersion! : self.itemVersion + 1,
                                           rowValue: newRowValue)
    }
    
    init(itemVersion: Int,
         rowValue: RowType) {
        self.itemVersion = itemVersion
        self.rowValue = rowValue
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        self.itemVersion = try values.decode(Int.self, forKey: .itemVersion)
        
        self.rowValue = try RowType(from: decoder)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(itemVersion, forKey: .itemVersion)
        
        try rowValue.encode(to: encoder)
    }
}


