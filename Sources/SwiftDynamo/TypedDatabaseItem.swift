//
//  DatabaseItem.swift
//  SwiftDynamo
//

import Foundation

public struct RowStatus: Codable {
    public let rowVersion: Int
    public let lastUpdatedDate: Date
    
    public init(rowVersion: Int, lastUpdatedDate: Date) {
        self.rowVersion = rowVersion
        self.lastUpdatedDate = lastUpdatedDate
    }
    
    enum CodingKeys: String, CodingKey {
        case rowVersion = "RowVersion"
        case lastUpdatedDate = "LastUpdatedDate"
    }
}

public struct TypedDatabaseItem<RowIdentity: DynamoRowIdentity, RowType: Codable>: Codable {
    public let compositePrimaryKey: CompositePrimaryKey<RowIdentity>
    public let createDate: Date
    public let rowStatus: RowStatus
    public let rowValue: RowType
    
    enum CodingKeys: String, CodingKey {
        case rowType = "RowType"
        case createDate = "CreateDate"
    }
    
    public static func newItem(withKey key: CompositePrimaryKey<RowIdentity>,
                               andValue value: RowType) -> TypedDatabaseItem<RowIdentity, RowType> {
        return TypedDatabaseItem<RowIdentity, RowType>(compositePrimaryKey: key,
                                     createDate: Date(),
                                     rowStatus: RowStatus(rowVersion: 1, lastUpdatedDate: Date()),
                                     rowValue: value)
    }
    
    public func createUpdatedItem(withValue value: RowType) -> TypedDatabaseItem<RowIdentity, RowType> {
        return TypedDatabaseItem<RowIdentity, RowType>(compositePrimaryKey: compositePrimaryKey,
                                     createDate: createDate,
                                     rowStatus: RowStatus(rowVersion: rowStatus.rowVersion + 1,
                                                          lastUpdatedDate: Date()),
                                     rowValue: value)
    }
    
    init(compositePrimaryKey: CompositePrimaryKey<RowIdentity>,
         createDate: Date,
         rowStatus: RowStatus,
         rowValue: RowType) {
        self.compositePrimaryKey = compositePrimaryKey
        self.createDate = createDate
        self.rowStatus = rowStatus
        self.rowValue = rowValue
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        let storedRowTypeName = try values.decode(String.self, forKey: .rowType)
        self.createDate = try values.decode(Date.self, forKey: .createDate)
        
        // get the type that is being requested to be decoded into
        let requestedRowTypeName = getTypeRowIdentity(type: RowType.self)
        
        // if the stored rowType is not what we should attempt to decode into
        guard storedRowTypeName == requestedRowTypeName else {
            // throw an exception to avoid accidentally decoding into the incorrect type
            throw SwiftDynamoError.typeMismatch(expected: storedRowTypeName, provided: requestedRowTypeName)
        }
        
        self.compositePrimaryKey = try CompositePrimaryKey(from: decoder)
        self.rowStatus = try RowStatus(from: decoder)
        self.rowValue = try RowType(from: decoder)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(getTypeRowIdentity(type: RowType.self), forKey: .rowType)
        try container.encode(createDate, forKey: .createDate)
        
        try compositePrimaryKey.encode(to: encoder)
        try rowStatus.encode(to: encoder)
        try rowValue.encode(to: encoder)
    }
}
