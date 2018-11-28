//
//  DynamoEncoder.swift
//  SwiftDynamo
//

import Foundation
import DynamoDBModel

public class DynamoEncoder {
    private let attributeNameTransform: ((String) -> String)?

    public init(attributeNameTransform: ((String) -> String)? = nil) {
        self.attributeNameTransform = attributeNameTransform
    }
    
    public func encode<T: Swift.Encodable>(_ value: T, userInfo: [CodingUserInfoKey: Any] = [:]) throws -> DynamoDBModel.AttributeValue {
        let container = InternalSingleValueEncodingContainer(userInfo: userInfo,
                                                             codingPath: [],
                                                             attributeNameTransform: attributeNameTransform,
                                                             defaultValue: nil)
        try value.encode(to: container)
        
        return container.attributeValue
    }
}

internal protocol AttributeValueConvertable {
    var attributeValue: DynamoDBModel.AttributeValue { get }
}

extension DynamoDBModel.AttributeValue: AttributeValueConvertable {
    var attributeValue: DynamoDBModel.AttributeValue {
        return self
    }
}

internal enum ContainerValueType {
    case singleValue(AttributeValueConvertable)
    case unkeyedContainer([AttributeValueConvertable])
    case keyedContainer([String: AttributeValueConvertable])
}
