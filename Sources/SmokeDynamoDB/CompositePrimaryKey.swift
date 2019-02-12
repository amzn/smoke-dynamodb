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
//  CompositePrimaryKey.swift
//  SmokeDynamoDB
//

import Foundation

public protocol PrimaryKeyAttributes {
    static var paritionKeyAttributeName: String { get }
    static var sortKeyAttributeName: String { get }
}

public struct StandardPrimaryKeyAttributes: PrimaryKeyAttributes {
    public static var paritionKeyAttributeName: String {
        return "PK"
    }
    public static var sortKeyAttributeName: String {
        return "SK"
    }
}

public typealias StandardTypedDatabaseItem<RowType: Codable> = TypedDatabaseItem<StandardPrimaryKeyAttributes, RowType>
public typealias StandardPolymorphicDatabaseItem<PossibleTypes: PossibleItemTypes>
    = PolymorphicDatabaseItem<StandardPrimaryKeyAttributes, PossibleTypes>
public typealias StandardCompositePrimaryKey = CompositePrimaryKey<StandardPrimaryKeyAttributes>

struct DynamoDBAttributesTypeCodingKey: CodingKey {
    var stringValue: String
    var intValue: Int?

    init?(stringValue: String) {
        self.stringValue = stringValue
        self.intValue = nil
    }

    init?(intValue: Int) {
        self.stringValue = "\(intValue)"
        self.intValue = intValue
    }
}

public struct CompositePrimaryKey<AttributesType: PrimaryKeyAttributes>: Codable, CustomStringConvertible {
    public var description: String {
        return "CompositePrimaryKey(partitionKey: \(partitionKey), sortKey: \(sortKey))"
    }
    
    public let partitionKey: String
    public let sortKey: String
    
    public init(partitionKey: String, sortKey: String) {
        self.partitionKey = partitionKey
        self.sortKey = sortKey
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: DynamoDBAttributesTypeCodingKey.self)
        partitionKey = try values.decode(String.self, forKey: DynamoDBAttributesTypeCodingKey(stringValue: AttributesType.paritionKeyAttributeName)!)
        sortKey = try values.decode(String.self, forKey: DynamoDBAttributesTypeCodingKey(stringValue: AttributesType.sortKeyAttributeName)!)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: DynamoDBAttributesTypeCodingKey.self)
        try container.encode(partitionKey, forKey: DynamoDBAttributesTypeCodingKey(stringValue: AttributesType.paritionKeyAttributeName)!)
        try container.encode(sortKey, forKey: DynamoDBAttributesTypeCodingKey(stringValue: AttributesType.sortKeyAttributeName)!)
    }
}
