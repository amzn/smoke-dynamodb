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
//  InternalSingleValueDecodingContainer.swift
//  SmokeDynamoDB
//

import Foundation
import DynamoDBModel

internal struct InternalSingleValueDecodingContainer {
    internal let codingPath: [CodingKey]
    internal let userInfo: [CodingUserInfoKey: Any]
    internal let attributeValue: DynamoDBModel.AttributeValue
    internal let attributeNameTransform: ((String) -> String)?
    
    init(attributeValue: DynamoDBModel.AttributeValue,
         codingPath: [CodingKey],
         userInfo: [CodingUserInfoKey: Any],
         attributeNameTransform: ((String) -> String)?) {
        self.attributeValue = attributeValue
        self.codingPath = codingPath
        self.userInfo = userInfo
        self.attributeNameTransform = attributeNameTransform
    }
}

extension InternalSingleValueDecodingContainer: SingleValueDecodingContainer {
    func decodeNil() -> Bool {
        return attributeValue.NULL ?? false
    }
    
    func decode(_ type: Bool.Type)   throws -> Bool {
        guard let value = attributeValue.BOOL else {
            throw getTypeMismatchError(expectation: Bool.self)
        }
        
        return value
    }
    
    func decode(_ type: Int.Type)    throws -> Int {
        guard let valueAsString = attributeValue.N,
            let value = Int(valueAsString) else {
            throw getTypeMismatchError(expectation: Int.self)
        }
        
        return value
    }
    
    func decode(_ type: Int8.Type)   throws -> Int8 {
        guard let valueAsString = attributeValue.N,
            let value = Int8(valueAsString) else {
            throw getTypeMismatchError(expectation: Int8.self)
        }
        
        return value
    }
    
    func decode(_ type: Int16.Type)  throws -> Int16 {
        guard let valueAsString = attributeValue.N,
            let value = Int16(valueAsString) else {
            throw getTypeMismatchError(expectation: Int16.self)
        }
        
        return value
    }
    
    func decode(_ type: Int32.Type)  throws -> Int32 {
        guard let valueAsString = attributeValue.N,
            let value = Int32(valueAsString) else {
            throw getTypeMismatchError(expectation: Int32.self)
        }
        
        return value
    }
    
    func decode(_ type: Int64.Type)  throws -> Int64 {
        guard let valueAsString = attributeValue.N,
            let value = Int64(valueAsString) else {
            throw getTypeMismatchError(expectation: Int64.self)
        }
        
        return value
    }
    
    func decode(_ type: UInt.Type)   throws -> UInt {
        guard let valueAsString = attributeValue.N,
            let value = UInt(valueAsString) else {
            throw getTypeMismatchError(expectation: UInt.self)
        }
        
        return value
    }
    
    func decode(_ type: UInt8.Type)  throws -> UInt8 {
        guard let valueAsString = attributeValue.N,
            let value = UInt8(valueAsString) else {
            throw getTypeMismatchError(expectation: UInt8.self)
        }
        
        return value
    }
    
    func decode(_ type: UInt16.Type) throws -> UInt16 {
        guard let valueAsString = attributeValue.N,
            let value = UInt16(valueAsString) else {
            throw getTypeMismatchError(expectation: UInt16.self)
        }
        
        return value
    }
    
    func decode(_ type: UInt32.Type) throws -> UInt32 {
        guard let valueAsString = attributeValue.N,
            let value = UInt32(valueAsString) else {
            throw getTypeMismatchError(expectation: UInt32.self)
        }
        
        return value
    }
    
    func decode(_ type: UInt64.Type) throws -> UInt64 {
        guard let valueAsString = attributeValue.N,
            let value = UInt64(valueAsString) else {
            throw getTypeMismatchError(expectation: UInt64.self)
        }
        
        return value
    }
    
    func decode(_ type: Float.Type)  throws -> Float {
        guard let valueAsString = attributeValue.N,
            let value = Float(valueAsString) else {
            throw getTypeMismatchError(expectation: Float.self)
        }
        
        return value
    }
    
    func decode(_ type: Double.Type) throws -> Double {
        guard let valueAsString = attributeValue.N,
            let value = Double(valueAsString) else {
            throw getTypeMismatchError(expectation: Double.self)
        }
        
        return value
    }
    
    func decode(_ type: String.Type) throws -> String {
        guard let value = attributeValue.S else {
            throw getTypeMismatchError(expectation: String.self)
        }
        
        return value
    }
    
    func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        if type == Date.self {
            let dateAsString = try String(from: self)
            
            guard let date = dateAsString.dateFromISO8601 as? T else {
                throw getTypeMismatchError(expectation: Date.self)
            }
            
            return date
        }
        
        return try T(from: self)
    }
    
    private func getTypeMismatchError(expectation: Any.Type) -> DecodingError {
        let description = "Expected to decode \(expectation)."
        let context = DecodingError.Context(codingPath: codingPath, debugDescription: description)
        
        return DecodingError.typeMismatch(expectation, context)
    }
}

extension InternalSingleValueDecodingContainer: Swift.Decoder {
    func container<Key>(keyedBy type: Key.Type) throws -> KeyedDecodingContainer<Key> where Key: CodingKey {
        let container = InternalKeyedDecodingContainer<Key>(decodingContainer: self)
        
        return KeyedDecodingContainer<Key>(container)
    }
    
    func unkeyedContainer() throws -> UnkeyedDecodingContainer {
        let container = InternalUnkeyedDecodingContainer(decodingContainer: self)
        
        return container
    }
    
    func singleValueContainer() throws -> SingleValueDecodingContainer {
        return self
    }
}
