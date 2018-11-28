//
//  TestConfiguration.swift
//  SwiftDynamo
//

import Foundation
@testable import SwiftDynamo

struct TestTypeA: Codable {
    let firstly: String
    let secondly: String
}

struct TestTypeB: Codable, DynamoDbCustomRowIdentity {
    static var identity: String? = "TypeBCustom"
    
    let thirdly: String
    let fourthly: String
}
