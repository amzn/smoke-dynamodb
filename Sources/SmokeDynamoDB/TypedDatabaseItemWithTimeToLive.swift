// Copyright 2018-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
//  TypedDatabaseItemWithTimeToLive.swift
//  SmokeDynamoDB
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

public protocol DatabaseItem {
    associatedtype AttributesType: PrimaryKeyAttributes
    // Default to StandardTimeToLiveAttributes for backwards compatibility
    associatedtype TimeToLiveAttributesType: TimeToLiveAttributes = StandardTimeToLiveAttributes
    
    var compositePrimaryKey: CompositePrimaryKey<AttributesType> { get }
    var createDate: Date { get }
    var rowStatus: RowStatus { get }
    var timeToLive: TimeToLive<TimeToLiveAttributesType>? { get }
}

public extension DatabaseItem {
    var timeToLive: TimeToLive<TimeToLiveAttributesType>? {
        return nil
    }
}

public protocol StandardDatabaseItem: DatabaseItem where AttributesType == StandardPrimaryKeyAttributes {
    
}

// Default to StandardTimeToLiveAttributes for backwards compatibility
public typealias TypedDatabaseItem<AttributesType: PrimaryKeyAttributes, RowType: Codable> = TypedDatabaseItemWithTimeToLive<AttributesType, RowType, StandardTimeToLiveAttributes>

public struct TypedDatabaseItemWithTimeToLive<AttributesType: PrimaryKeyAttributes,
                                              RowType: Codable,
                                              TimeToLiveAttributesType: TimeToLiveAttributes>: DatabaseItem, Codable {
    public let compositePrimaryKey: CompositePrimaryKey<AttributesType>
    public let createDate: Date
    public let rowStatus: RowStatus
    public let timeToLive: TimeToLive<TimeToLiveAttributesType>?
    public let rowValue: RowType
    
    enum CodingKeys: String, CodingKey {
        case rowType = "RowType"
        case createDate = "CreateDate"
    }
    
    public static func newItem(withKey key: CompositePrimaryKey<AttributesType>,
                               andValue value: RowType,
                               andTimeToLive timeToLive: TimeToLive<TimeToLiveAttributesType>? = nil)
    -> TypedDatabaseItemWithTimeToLive<AttributesType, RowType, TimeToLiveAttributesType> {
        return TypedDatabaseItemWithTimeToLive<AttributesType, RowType, TimeToLiveAttributesType>(
            compositePrimaryKey: key,
            createDate: Date(),
            rowStatus: RowStatus(rowVersion: 1, lastUpdatedDate: Date()),
            rowValue: value,
            timeToLive: timeToLive)
    }
    
    public func createUpdatedItem(withValue value: RowType,
                                  andTimeToLive timeToLive: TimeToLive<TimeToLiveAttributesType>? = nil)
    -> TypedDatabaseItemWithTimeToLive<AttributesType, RowType, TimeToLiveAttributesType> {
        return TypedDatabaseItemWithTimeToLive<AttributesType, RowType, TimeToLiveAttributesType>(
            compositePrimaryKey: compositePrimaryKey,
            createDate: createDate,
            rowStatus: RowStatus(rowVersion: rowStatus.rowVersion + 1,
                                 lastUpdatedDate: Date()),
            rowValue: value,
            timeToLive: timeToLive)
    }
    
    init(compositePrimaryKey: CompositePrimaryKey<AttributesType>,
         createDate: Date,
         rowStatus: RowStatus,
         rowValue: RowType,
         timeToLive: TimeToLive<TimeToLiveAttributesType>? = nil) {
        self.compositePrimaryKey = compositePrimaryKey
        self.createDate = createDate
        self.rowStatus = rowStatus
        self.rowValue = rowValue
        self.timeToLive = timeToLive
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        let storedRowTypeName = try values.decode(String.self, forKey: .rowType)
        self.createDate = try values.decode(Date.self, forKey: .createDate)
        
        // get the type that is being requested to be decoded into
        let requestedRowTypeName = getTypeRowIdentifier(type: RowType.self)
        
        // if the stored rowType is not what we should attempt to decode into
        guard storedRowTypeName == requestedRowTypeName else {
            // throw an exception to avoid accidentally decoding into the incorrect type
            throw SmokeDynamoDBError.typeMismatch(expected: storedRowTypeName, provided: requestedRowTypeName)
        }
        
        self.compositePrimaryKey = try CompositePrimaryKey(from: decoder)
        self.rowStatus = try RowStatus(from: decoder)
        
        do {
            self.timeToLive = try TimeToLive(from: decoder)
        } catch DecodingError.keyNotFound {
            self.timeToLive = nil
        }
        
        self.rowValue = try RowType(from: decoder)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(getTypeRowIdentifier(type: RowType.self), forKey: .rowType)
        try container.encode(createDate, forKey: .createDate)
        
        try compositePrimaryKey.encode(to: encoder)
        try rowStatus.encode(to: encoder)
        try timeToLive?.encode(to: encoder)
        try rowValue.encode(to: encoder)
    }
}
