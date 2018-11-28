//
//  InternalKeyedEncodingContainer.swift
//  SwiftDynamo
//

import Foundation
import DynamoDBModel

internal struct InternalKeyedEncodingContainer<K: CodingKey> : KeyedEncodingContainerProtocol {
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

    func superEncoder() -> Encoder { return createNestedContainer(for: InternalDynamoCodingKey.super) }
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
