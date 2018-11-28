//
//  DynamoDecoder.swift
//  SwiftDynamo
//

import Foundation
import DynamoDBModel

public class DynamoDecoder {
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
