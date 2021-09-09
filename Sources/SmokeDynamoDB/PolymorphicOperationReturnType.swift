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
//  PolymorphicOperationReturnType.swift
//  SmokeDynamoDB
//
import Foundation
import SmokeHTTPClient
import DynamoDBModel

public protocol BatchCapableReturnType {
    associatedtype AttributesType: PrimaryKeyAttributes
    
    func getItemKey() -> CompositePrimaryKey<AttributesType>
}

public protocol PolymorphicOperationReturnType {
    associatedtype AttributesType: PrimaryKeyAttributes
        
    static var types: [(Codable.Type, PolymorphicOperationReturnOption<AttributesType, Self>)] { get }
}

public struct PolymorphicOperationReturnOption<AttributesType: PrimaryKeyAttributes, ReturnType> {
    private let decodingPayloadHandler: (Decoder) throws -> ReturnType
    private let typeConvertingPayloadHander: (Any) throws -> ReturnType
    
    public init<RowType: Codable>(
        _ payloadHandler: @escaping (TypedDatabaseItem<AttributesType, RowType>) -> ReturnType) {
        func newDecodingPayloadHandler(decoder: Decoder) throws -> ReturnType {
            let typedDatabaseItem: TypedDatabaseItem<AttributesType, RowType> = try TypedDatabaseItem(from: decoder)
            
            return payloadHandler(typedDatabaseItem)
        }
        
        func newTypeConvertingPayloadHandler(input: Any) throws -> ReturnType {
            guard let typedDatabaseItem = input as? TypedDatabaseItem<AttributesType, RowType> else {
                let description = "Expected to use item type \(TypedDatabaseItem<AttributesType, RowType>.self)."
                let context = DecodingError.Context(codingPath: [], debugDescription: description)
                throw DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, RowType>.self, context)
            }
                        
            return payloadHandler(typedDatabaseItem)
        }
        
        self.decodingPayloadHandler = newDecodingPayloadHandler
        self.typeConvertingPayloadHander = newTypeConvertingPayloadHandler
    }
    
    internal func getReturnType(from decoder: Decoder) throws -> ReturnType {
        return try self.decodingPayloadHandler(decoder)
    }
    
    internal func getReturnType(input: Any) throws -> ReturnType {
        return try self.typeConvertingPayloadHander(input)
    }
}

internal struct ReturnTypeDecodable<ReturnType: PolymorphicOperationReturnType>: Decodable {
    public let decodedValue: ReturnType

    enum CodingKeys: String, CodingKey {
        case rowType = "RowType"
    }

    init(decodedValue: ReturnType) {
        self.decodedValue = decodedValue
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: CodingKeys.self)
        let storedRowTypeName = try values.decode(String.self, forKey: .rowType)
        
        var queryableTypeProviders: [String: PolymorphicOperationReturnOption<ReturnType.AttributesType, ReturnType>] = [:]
        ReturnType.types.forEach { (type, provider) in
            queryableTypeProviders[getTypeRowIdentifier(type: type)] = provider
        }

        if let provider = queryableTypeProviders[storedRowTypeName] {
            self.decodedValue = try provider.getReturnType(from: decoder)
        } else {
            // throw an exception, we don't know what this type is
            throw SmokeDynamoDBError.unexpectedType(provided: storedRowTypeName)
        }
    }
}
