//
//  PolymorphicDatabaseItem.swift
//  SwiftDynamo
//
//  Created by Pilkington, Simon on 1/2/18.
//

import Foundation

public protocol PossibleItemTypes {
    static var types: [Codable.Type] { get }
}

public struct PolymorphicDatabaseItem<RowIdentity: DynamoRowIdentity, PossibleTypes: PossibleItemTypes> : Decodable {
    public let compositePrimaryKey: CompositePrimaryKey<RowIdentity>
    public let createDate: Date
    public let rowStatus: RowStatus
    public let rowValue: Codable
    
    enum CodingKeys: String, CodingKey {
        case rowType = "RowType"
        case createDate = "CreateDate"
    }
    
    init(compositePrimaryKey: CompositePrimaryKey<RowIdentity>,
         createDate: Date,
         rowStatus: RowStatus,
         rowValue: Codable) {
        self.compositePrimaryKey = compositePrimaryKey
        self.createDate = createDate
        self.rowStatus = rowStatus
        self.rowValue = rowValue
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        let storedRowTypeName = try values.decode(String.self, forKey: .rowType)
        self.createDate = try values.decode(Date.self, forKey: .createDate)
        
        var possibleTypes: [String: Codable.Type] = [:]
        PossibleTypes.types.forEach { type in
            possibleTypes[getTypeRowIdentity(type: type)] = type
            
        }
        
        self.compositePrimaryKey = try CompositePrimaryKey(from: decoder)
        self.rowStatus = try RowStatus(from: decoder)
        
        if let type = possibleTypes[storedRowTypeName] {
            self.rowValue = try type.init(from: decoder)
        } else {
            // throw an exception, we don't what this type is
            throw SwiftDynamoError.unexpectedType(provided: storedRowTypeName)
        }
    }
    
    public func createUpdatedItem<RowType: Codable>(withValue value: RowType,
                                                    canOverwriteExistingRow: Bool = true,
                                                    ignoreVersionNumberWhenOverwriting: Bool = false) throws
        -> TypedDatabaseItem<RowIdentity, RowType> {
        if rowValue is RowType {
            return TypedDatabaseItem<RowIdentity, RowType>(compositePrimaryKey: compositePrimaryKey,
                                                           createDate: createDate,
                                                           rowStatus: RowStatus(rowVersion: rowStatus.rowVersion + 1,
                                                                                lastUpdatedDate: Date()),
                                                           rowValue: value)
        }
        
        throw SwiftDynamoError.typeMismatch(expected: String(describing: type(of: rowValue)),
                                              provided: String(describing: RowType.self))
    }
}
