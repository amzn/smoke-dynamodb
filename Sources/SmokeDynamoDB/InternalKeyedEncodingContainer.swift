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
//  InternalKeyedEncodingContainer.swift
//  SmokeDynamoDB
//

import Foundation
import DynamoDBModel

internal struct InternalKeyedEncodingContainer<K: CodingKey>: KeyedEncodingContainerProtocol {
    typealias Key = K

    private let enclosingContainer: InternalSingleValueEncodingContainer
    
    init(enclosingContainer: InternalSingleValueEncodingContainer) {
        self.enclosingContainer = enclosingContainer
    }

    // MARK: - Swift.KeyedEncodingContainerProtocol Methods
    
    var codingPath: [CodingKey] {
        return enclosingContainer.codingPath
    }

    func encodeNil(forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBModel.AttributeValue(NULL: true))
    }
    
    func encode(_ value: Bool, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBModel.AttributeValue(BOOL: value))
    }
    
    func encode(_ value: Int, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Int8, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Int16, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Int32, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Int64, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: UInt, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: UInt8, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: UInt16, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: UInt32, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: UInt64, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Float, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Double, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: String, forKey key: Key) throws {
        enclosingContainer.addToKeyedContainer(key: key, value: DynamoDBModel.AttributeValue(S: value)) }
    
    func encode<T>(_ value: T, forKey key: Key)   throws where T: Encodable {
        let nestedContainer = createNestedContainer(for: key)
        
        try nestedContainer.encode(value)
    }

    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type,
                                    forKey key: Key) -> KeyedEncodingContainer<NestedKey> {
        let nestedContainer = createNestedContainer(for: key, defaultValue: .keyedContainer([:]))
        
        let nestedKeyContainer = InternalKeyedEncodingContainer<NestedKey>(enclosingContainer: nestedContainer)
        
        return KeyedEncodingContainer<NestedKey>(nestedKeyContainer)
    }

    func nestedUnkeyedContainer(forKey key: Key) -> UnkeyedEncodingContainer {
        let nestedContainer = createNestedContainer(for: key, defaultValue: .unkeyedContainer([]))
        
        let nestedKeyContainer = InternalUnkeyedEncodingContainer(enclosingContainer: nestedContainer)
        
        return nestedKeyContainer
    }

    func superEncoder() -> Encoder { return createNestedContainer(for: InternalDynamoDBCodingKey.super) }
    func superEncoder(forKey key: Key) -> Encoder { return createNestedContainer(for: key) }

    // MARK: -

    private func createNestedContainer<NestedKey: CodingKey>(for key: NestedKey,
                                                             defaultValue: ContainerValueType? = nil)
        -> InternalSingleValueEncodingContainer {
        let nestedContainer = InternalSingleValueEncodingContainer(userInfo: enclosingContainer.userInfo,
                                                                   codingPath: enclosingContainer.codingPath + [key],
                                                                   attributeNameTransform: enclosingContainer.attributeNameTransform,
                                                                   defaultValue: defaultValue)
        enclosingContainer.addToKeyedContainer(key: key, value: nestedContainer)
        
        return nestedContainer
    }
}
