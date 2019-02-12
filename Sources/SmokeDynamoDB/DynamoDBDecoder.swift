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
//  DynamoDBDecoder.swift
//  SmokeDynamoDB
//

import Foundation
import DynamoDBModel

public class DynamoDBDecoder {
    internal let attributeNameTransform: ((String) -> String)?

    public init(attributeNameTransform: ((String) -> String)? = nil) {
        self.attributeNameTransform = attributeNameTransform
    }
    
    public func decode<T: Swift.Decodable>(_ value: DynamoDBModel.AttributeValue, userInfo: [CodingUserInfoKey: Any] = [:]) throws -> T {
        let container = InternalSingleValueDecodingContainer(attributeValue: value,
                                                             codingPath: [],
                                                             userInfo: userInfo,
                                                             attributeNameTransform: attributeNameTransform)
        
        return try T(from: container)
    }
}
