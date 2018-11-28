//
//  CompositePrimaryKey.swift
//  SwiftDynamo
//

import Foundation

public protocol DynamoRowIdentity {
    static var paritionKeyAttributeName: String { get }
    static var sortKeyAttributeName: String { get }
}

public struct DefaultDynamoRowIdentity: DynamoRowIdentity {
    public static var paritionKeyAttributeName: String {
        return "PK"
    }
    public static var sortKeyAttributeName: String {
        return "SK"
    }
}

public typealias DefaultIdentityTypedDatabaseItem<RowType : Codable> = TypedDatabaseItem<DefaultDynamoRowIdentity, RowType>
public typealias DefaultIdentityPolymorphicDatabaseItem<PossibleTypes : PossibleItemTypes>
    = PolymorphicDatabaseItem<DefaultDynamoRowIdentity, PossibleTypes>
public typealias DefaultIdentityCompositePrimaryKey = CompositePrimaryKey<DefaultDynamoRowIdentity>

struct DynamoRowIdentityCodingKey: CodingKey {
    var stringValue: String
    var intValue: Int?

    init?(stringValue: String) {
        self.stringValue = stringValue
        self.intValue = nil
    }

    init?(intValue: Int) {
        self.stringValue = "\(intValue)"
        self.intValue = intValue
    }
}

public struct CompositePrimaryKey<RowIdentity: DynamoRowIdentity>: Codable, CustomStringConvertible {
    public var description: String {
        return "CompositePrimaryKey(partitionKey: \(partitionKey), sortKey: \(sortKey))"
    }
    
    public let partitionKey: String
    public let sortKey: String
    
    public init(partitionKey: String, sortKey: String) {
        self.partitionKey = partitionKey
        self.sortKey = sortKey
    }
    
    public init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: DynamoRowIdentityCodingKey.self)
        partitionKey = try values.decode(String.self, forKey: DynamoRowIdentityCodingKey(stringValue: RowIdentity.paritionKeyAttributeName)!)
        sortKey = try values.decode(String.self, forKey: DynamoRowIdentityCodingKey(stringValue: RowIdentity.sortKeyAttributeName)!)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: DynamoRowIdentityCodingKey.self)
        try container.encode(partitionKey, forKey: DynamoRowIdentityCodingKey(stringValue: RowIdentity.paritionKeyAttributeName)!)
        try container.encode(sortKey, forKey: DynamoRowIdentityCodingKey(stringValue: RowIdentity.sortKeyAttributeName)!)
    }
}
