//
//  DynamoDbCustomRowIdentity.swift
//  SwiftDynamo
//

import Foundation

public protocol DynamoDbCustomRowIdentity {
    static var identity: String? { get }
}

func getTypeRowIdentity(type: Any.Type) -> String {
    let typeRowIdentity: String
    // if this type has a custom row identity
    if let customRowIdentityType = type as? DynamoDbCustomRowIdentity.Type,
        let identity = customRowIdentityType.identity {
        typeRowIdentity = identity
    } else {
        typeRowIdentity = String(describing: type)
    }
    
    return typeRowIdentity
}
