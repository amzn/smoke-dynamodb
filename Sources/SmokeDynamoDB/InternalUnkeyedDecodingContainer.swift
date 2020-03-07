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
//  InternalUnkeyedDecodingContainer.swift
//  SmokeDynamoDB
//

import Foundation

internal struct InternalUnkeyedDecodingContainer: UnkeyedDecodingContainer {
    private let decodingContainer: InternalSingleValueDecodingContainer
    internal private(set) var currentIndex: Int
    
    init(decodingContainer: InternalSingleValueDecodingContainer) {
        self.decodingContainer = decodingContainer
        self.currentIndex = 0
    }

    // MARK: - Swift.UnkeyedEncodingContainer Methods

    var codingPath: [CodingKey] {
        return decodingContainer.codingPath
    }
    
    mutating func decodeNil() throws -> Bool {
        return try createNestedContainer().decodeNil()
    }

    mutating func decode(_ type: Bool.Type)   throws -> Bool {
        return try createNestedContainer().decode(Bool.self)
    }
    
    mutating func decode(_ type: Int.Type)    throws -> Int {
        return try createNestedContainer().decode(Int.self)
    }
    
    mutating func decode(_ type: Int8.Type)   throws -> Int8 {
        return try createNestedContainer().decode(Int8.self)
    }
    
    mutating func decode(_ type: Int16.Type)  throws -> Int16 {
        return try createNestedContainer().decode(Int16.self)
    }
    
    mutating func decode(_ type: Int32.Type)  throws -> Int32 {
        return try createNestedContainer().decode(Int32.self)
    }
    
    mutating func decode(_ type: Int64.Type)  throws -> Int64 {
        return try createNestedContainer().decode(Int64.self)
    }
    
    mutating func decode(_ type: UInt.Type)   throws -> UInt {
        return try createNestedContainer().decode(UInt.self)
    }
    
    mutating func decode(_ type: UInt8.Type)  throws -> UInt8 {
        return try createNestedContainer().decode(UInt8.self)
    }
    
    mutating func decode(_ type: UInt16.Type) throws -> UInt16 {
        return try createNestedContainer().decode(UInt16.self)
    }
    
    mutating func decode(_ type: UInt32.Type) throws -> UInt32 {
        return try createNestedContainer().decode(UInt32.self)
    }
    
    mutating func decode(_ type: UInt64.Type) throws -> UInt64 {
        return try createNestedContainer().decode(UInt64.self)
    }
    
    mutating func decode(_ type: Float.Type)  throws -> Float {
        return try createNestedContainer().decode(Float.self)
    }
    
    mutating func decode(_ type: Double.Type) throws -> Double {
        return try createNestedContainer().decode(Double.self)
    }
    
    mutating func decode(_ type: String.Type) throws -> String {
        return try createNestedContainer().decode(String.self)
    }
    
    mutating func decode<T>(_ type: T.Type) throws -> T where T: Decodable {
        return try createNestedContainer().decode(type)
    }
    
    var count: Int? {
        guard let values = decodingContainer.attributeValue.L else {
            return nil
        }
        
        return values.count
    }
    
    var isAtEnd: Bool {
        guard let values = decodingContainer.attributeValue.L else {
            return true
        }
        
        return currentIndex >= values.count
    }
    
    mutating func nestedContainer<NestedKey>(keyedBy type: NestedKey.Type) throws
        -> KeyedDecodingContainer<NestedKey> where NestedKey: CodingKey {
        return try createNestedContainer().container(keyedBy: type)
    }
    
    mutating func nestedUnkeyedContainer() throws -> UnkeyedDecodingContainer {
        return try createNestedContainer().unkeyedContainer()
    }
    
    mutating func superDecoder() throws -> Decoder {
        return try createNestedContainer()
    }

    // MARK: -
    
    private mutating func createNestedContainer() throws -> InternalSingleValueDecodingContainer {
        let index = currentIndex
        currentIndex += 1
        
        guard let values = decodingContainer.attributeValue.L else {
            let description = "Expected to decode a list."
            let context = DecodingError.Context(codingPath: codingPath, debugDescription: description)
            throw DecodingError.dataCorrupted(context)
        }
        
        guard index < values.count else {
            let description = "Could not find key for index \(index)."
            let context = DecodingError.Context(codingPath: codingPath, debugDescription: description)
            throw DecodingError.valueNotFound(Any.self, context)
        }
        
        let value = values[index]
        
        return InternalSingleValueDecodingContainer(attributeValue: value,
                                                    codingPath: decodingContainer.codingPath
                                                        + [InternalDynamoDBCodingKey(index: index)],
                                                    userInfo: decodingContainer.userInfo,
                                                    attributeNameTransform: decodingContainer.attributeNameTransform)
    }
}

private let iso8601DateFormatter: DateFormatter = {
     let formatter = DateFormatter()
     formatter.calendar = Calendar(identifier: .iso8601)
     formatter.locale = Locale(identifier: "en_US_POSIX")
     formatter.timeZone = TimeZone(secondsFromGMT: 0)
     formatter.dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSXXXXX"
     return formatter
 }()

 extension Date {
     var iso8601: String {
         return iso8601DateFormatter.string(from: self)
     }
 }

 extension String {
     var dateFromISO8601: Date? {
         return iso8601DateFormatter.date(from: self)   // "Mar 22, 2017, 10:22 AM"
     }
 }
