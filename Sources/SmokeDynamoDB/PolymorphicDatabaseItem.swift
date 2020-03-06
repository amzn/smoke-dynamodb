// Copyright 2018-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
//  PolymorphicDatabaseItem.swift
//  SmokeDynamoDB
//

import Foundation

public protocol PossibleItemTypes {
    static var types: [Codable.Type] { get }
}

public struct PolymorphicDatabaseItem<AttributesType: PrimaryKeyAttributes, PossibleTypes: PossibleItemTypes>: Decodable {
    public let compositePrimaryKey: CompositePrimaryKey<AttributesType>
    public let createDate: Date
    public let rowStatus: RowStatus
    public let rowValue: Codable
    
    enum CodingKeys: String, CodingKey {
        case rowType = "RowType"
        case createDate = "CreateDate"
    }
    
    init(compositePrimaryKey: CompositePrimaryKey<AttributesType>,
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
            possibleTypes[getTypeRowIdentifier(type: type)] = type
            
        }
        
        self.compositePrimaryKey = try CompositePrimaryKey(from: decoder)
        self.rowStatus = try RowStatus(from: decoder)
        
        if let type = possibleTypes[storedRowTypeName] {
            self.rowValue = try type.init(from: decoder)
        } else {
            // throw an exception, we don't what this type is
            throw SmokeDynamoDBError.unexpectedType(provided: storedRowTypeName)
        }
    }
    
    public func createUpdatedItem<RowType: Codable>(withValue value: RowType,
                                                    canOverwriteExistingRow: Bool = true,
                                                    ignoreVersionNumberWhenOverwriting: Bool = false) throws
        -> TypedDatabaseItem<AttributesType, RowType> {
        if rowValue is RowType {
            return TypedDatabaseItem<AttributesType, RowType>(compositePrimaryKey: compositePrimaryKey,
                                                           createDate: createDate,
                                                           rowStatus: RowStatus(rowVersion: rowStatus.rowVersion + 1,
                                                                                lastUpdatedDate: Date()),
                                                           rowValue: value)
        }
        
        throw SmokeDynamoDBError.typeMismatch(expected: String(describing: type(of: rowValue)),
                                              provided: String(describing: RowType.self))
    }
}
