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
//  TimeToLive.swift
//  SmokeDynamoDB
//

import Foundation

public protocol TimeToLiveAttributes {
    static var timeToLiveAttributeName: String { get }
}

public struct StandardTimeToLiveAttributes: TimeToLiveAttributes {
    public static var timeToLiveAttributeName: String {
        return "ExpireDate"
    }
}

public typealias StandardTimeToLive = TimeToLive<StandardTimeToLiveAttributes>

public struct TimeToLive<AttributesType: TimeToLiveAttributes>: Codable, CustomStringConvertible, Hashable {
    public var description: String {
        return "TimeToLive(timeToLiveTimestamp: \(timeToLiveTimestamp)"
    }
    
    public let timeToLiveTimestamp: Int64
    
    public init(timeToLiveTimestamp: Int64) {
        self.timeToLiveTimestamp = timeToLiveTimestamp
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: DynamoDBAttributesTypeCodingKey.self)
        self.timeToLiveTimestamp = try values.decode(Int64.self, forKey: DynamoDBAttributesTypeCodingKey(stringValue: AttributesType.timeToLiveAttributeName)!)
    }
    
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: DynamoDBAttributesTypeCodingKey.self)
        try container.encode(self.timeToLiveTimestamp, forKey: DynamoDBAttributesTypeCodingKey(stringValue: AttributesType.timeToLiveAttributeName)!)
    }
}
