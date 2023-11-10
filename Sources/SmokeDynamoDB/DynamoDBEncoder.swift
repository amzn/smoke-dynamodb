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
//  DynamoDBEncoder.swift
//  SmokeDynamoDB
//

import Foundation
import AWSDynamoDB

public class DynamoDBEncoder {
    private let attributeNameTransform: ((String) -> String)?

    public init(attributeNameTransform: ((String) -> String)? = nil) {
        self.attributeNameTransform = attributeNameTransform
    }
    
    public func encode<T: Swift.Encodable>(_ value: T, userInfo: [CodingUserInfoKey: Any] = [:]) throws -> DynamoDBClientTypes.AttributeValue {
        let container = InternalSingleValueEncodingContainer(userInfo: userInfo,
                                                             codingPath: [],
                                                             attributeNameTransform: attributeNameTransform,
                                                             defaultValue: nil)
        try value.encode(to: container)
        
        return container.attributeValue
    }
}

internal protocol AttributeValueConvertable {
    var attributeValue: DynamoDBClientTypes.AttributeValue { get }
}

extension DynamoDBClientTypes.AttributeValue: AttributeValueConvertable {
    var attributeValue: DynamoDBClientTypes.AttributeValue {
        return self
    }
}

internal enum ContainerValueType {
    case singleValue(AttributeValueConvertable)
    case unkeyedContainer([AttributeValueConvertable])
    case keyedContainer([String: AttributeValueConvertable])
}
