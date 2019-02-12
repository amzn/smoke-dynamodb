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
//  InternalSingleValueEncodingContainer.swift
//  SmokeDynamoDB
//

import Foundation
import DynamoDBModel
import LoggerAPI

internal class InternalSingleValueEncodingContainer: SingleValueEncodingContainer {
    internal private(set) var containerValue: ContainerValueType?
    internal let attributeNameTransform: ((String) -> String)?
    
    let codingPath: [CodingKey]
    let userInfo: [CodingUserInfoKey: Any]
    
    init(userInfo: [CodingUserInfoKey: Any],
         codingPath: [CodingKey],
         attributeNameTransform: ((String) -> String)?,
         defaultValue: ContainerValueType?) {
        self.containerValue = defaultValue
        self.userInfo = userInfo
        self.codingPath = codingPath
        self.attributeNameTransform = attributeNameTransform
    }
    
    func encodeNil() throws {
        containerValue = .singleValue(DynamoDBModel.AttributeValue(NULL: true))
    }
    
    func encode(_ value: Bool) throws {
        containerValue = .singleValue(DynamoDBModel.AttributeValue(BOOL: value))
    }
    
    func encode(_ value: Int) throws {
        containerValue = .singleValue(DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Int8) throws {
        containerValue = .singleValue(DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Int16) throws {
        containerValue = .singleValue(DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Int32) throws {
        containerValue = .singleValue(DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Int64) throws {
        containerValue = .singleValue(DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: UInt) throws {
        containerValue = .singleValue(DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: UInt8) throws {
        containerValue = .singleValue(DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: UInt16) throws {
        containerValue = .singleValue(DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: UInt32) throws {
        containerValue = .singleValue(DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: UInt64) throws {
        containerValue = .singleValue(DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Float) throws {
        containerValue = .singleValue(DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: Double) throws {
        containerValue = .singleValue(DynamoDBModel.AttributeValue(N: String(value)))
    }
    
    func encode(_ value: String) throws {
        containerValue = .singleValue(DynamoDBModel.AttributeValue(S: value))
    }

    func encode<T>(_ value: T) throws where T: Encodable {
        if let date = value as? Foundation.Date {
            let dateAsString = date.iso8601
            
            containerValue = .singleValue(DynamoDBModel.AttributeValue(S: dateAsString))
            return
        }
        
        try value.encode(to: self)
    }
    
    func addToKeyedContainer<KeyType: CodingKey>(key: KeyType, value: AttributeValueConvertable) {
        guard let currentContainerValue = containerValue else {
            fatalError("Attempted to add a keyed item to an unitinialized container.")
        }
        
        guard case .keyedContainer(var values) = currentContainerValue else {
            fatalError("Expected keyed container and there wasn't one.")
        }
        
        let attributeName = getAttributeName(key: key)
        
        values[attributeName] = value
        
        containerValue = .keyedContainer(values)
    }
    
    func addToUnkeyedContainer(value: AttributeValueConvertable) {
        guard let currentContainerValue = containerValue else {
            fatalError("Attempted to ad an unkeyed item to an uninitialized container.")
        }
        
        guard case .unkeyedContainer(var values) = currentContainerValue else {
            fatalError("Expected unkeyed container and there wasn't one.")
        }
        
        values.append(value)
        
        containerValue = .unkeyedContainer(values)
    }
    
    private func getAttributeName(key: CodingKey) -> String {
        let attributeName: String
        if let attributeNameTransform = attributeNameTransform {
            attributeName = attributeNameTransform(key.stringValue)
        } else {
            attributeName = key.stringValue
        }
        
        return attributeName
    }
}

extension InternalSingleValueEncodingContainer: AttributeValueConvertable {
    var attributeValue: DynamoDBModel.AttributeValue {
        guard let containerValue = containerValue else {
            fatalError("Attempted to access uninitialized container.")
        }
        
        switch containerValue {
        case .singleValue(let value):
            return value.attributeValue
        case .unkeyedContainer(let values):
            let mappedValues = values.map { value in value.attributeValue }
            
            return DynamoDBModel.AttributeValue(L: mappedValues)
        case .keyedContainer(let values):
            let mappedValues = values.mapValues { value in value.attributeValue }
        
            return DynamoDBModel.AttributeValue(M: mappedValues)
        }
    }
}

extension InternalSingleValueEncodingContainer: Swift.Encoder {
    var unkeyedContainerCount: Int {
        guard let containerValue = containerValue else {
            fatalError("Attempted to access unitialized container.")
        }
        
        guard case .unkeyedContainer(let values) = containerValue else {
            fatalError("Expected unkeyed container and there wasn't one.")
        }
        
        return values.count
    }
    
    func container<Key>(keyedBy type: Key.Type) -> KeyedEncodingContainer<Key> where Key: CodingKey {
        
        // if there container is already initialized
        if let currentContainerValue = containerValue {
            guard case .keyedContainer = currentContainerValue else {
                fatalError("Trying to use an already initialized container as a keyed container.")
            }
        } else {
            containerValue = .keyedContainer([:])
        }
        
        let container = InternalKeyedEncodingContainer<Key>(enclosingContainer: self)
        
        return KeyedEncodingContainer<Key>(container)
    }
    
    func unkeyedContainer() -> UnkeyedEncodingContainer {
        
        // if there container is already initialized
        if let currentContainerValue = containerValue {
            guard case .unkeyedContainer = currentContainerValue else {
                fatalError("Trying to use an already initialized container as an unkeyed container.")
            }
        } else {
            containerValue = .unkeyedContainer([])
        }
        
        let container = InternalUnkeyedEncodingContainer(enclosingContainer: self)
        
        return container
    }
    
    func singleValueContainer() -> SingleValueEncodingContainer {
        return self
    }
}
