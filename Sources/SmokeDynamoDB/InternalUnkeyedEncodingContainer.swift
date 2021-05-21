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
//  InternalUnkeyedEncodingContainer.swift
//  SmokeDynamoDB
//

import Foundation
import DynamoDBModel

internal struct InternalUnkeyedEncodingContainer: UnkeyedEncodingContainer {
    private let enclosingContainer: InternalSingleValueEncodingContainer

    init(enclosingContainer: InternalSingleValueEncodingContainer) {
        self.enclosingContainer = enclosingContainer
    }

    // MARK: - Swift.UnkeyedEncodingContainer Methods

    var codingPath: [CodingKey] {
        return enclosingContainer.codingPath
    }
    
    var count: Int { return enclosingContainer.unkeyedContainerCount }
    
    func encodeNil() throws {
        enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue(NULL: true)) }
    
    func encode(_ value: Bool) throws {
        enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue(BOOL: value)) }
    
    func encode(_ value: Int) throws {
        enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Int8) throws {
        enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Int16) throws {
        enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Int32) throws {
        enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Int64) throws {
        enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: UInt) throws {
        enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: UInt8) throws {
        enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: UInt16) throws {
        enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: UInt32) throws {
        enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: UInt64) throws {
        enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Float) throws {
        enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Double) throws {
        enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: String) throws {
        enclosingContainer.addToUnkeyedContainer(value: DynamoDBModel.AttributeValue(S: value))
    }
    
    func encode<T>(_ value: T) throws where T: Encodable {
        try createNestedContainer().encode(value)
    }

    func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type) -> KeyedEncodingContainer<NestedKey> {
        let nestedContainer = createNestedContainer(defaultValue: .keyedContainer([:]))
        
        let nestedKeyContainer = InternalKeyedEncodingContainer<NestedKey>(enclosingContainer: nestedContainer)
        
        return KeyedEncodingContainer<NestedKey>(nestedKeyContainer)
    }

    func nestedUnkeyedContainer() -> UnkeyedEncodingContainer {
        let nestedContainer = createNestedContainer(defaultValue: .unkeyedContainer([]))
        
        let nestedKeyContainer = InternalUnkeyedEncodingContainer(enclosingContainer: nestedContainer)
        
        return nestedKeyContainer
    }
    
    func superEncoder() -> Encoder { return createNestedContainer() }

    // MARK: -
    
    private func createNestedContainer(defaultValue: ContainerValueType? = nil)
        -> InternalSingleValueEncodingContainer {
        let index = enclosingContainer.unkeyedContainerCount
        
        let nestedContainer = InternalSingleValueEncodingContainer(userInfo: enclosingContainer.userInfo,
                                                    codingPath: enclosingContainer.codingPath + [InternalDynamoDBCodingKey(index: index)],
                                                    attributeNameTransform: enclosingContainer.attributeNameTransform,
                                                    defaultValue: defaultValue)
        enclosingContainer.addToUnkeyedContainer(value: nestedContainer)
        
        return nestedContainer
    }
}
