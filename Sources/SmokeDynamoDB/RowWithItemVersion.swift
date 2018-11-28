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
//  RowWithItemVersion.swift
//  SmokeDynamoDB
//

import Foundation

public struct RowWithItemVersion<RowType: Codable> : Codable, CustomRowTypeIdentifier {

    public static var rowTypeIdentifier: String? {
        let rowTypeIdentity = getTypeRowIdentifier(type: RowType.self)
        
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
