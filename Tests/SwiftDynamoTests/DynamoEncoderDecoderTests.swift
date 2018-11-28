//
//  DynamoEncoderTests.swift
//  SwiftDynamoTests
//

import XCTest
@testable import SwiftDynamo

private let dynamoEncoder = DynamoEncoder()
private let dynamoDecoder = DynamoDecoder()

struct CoreAccountAttributes: Codable {
    var description: String
    var mappedValues: [String: String]
    var notificationTargets: NotificationTargets
}

extension CoreAccountAttributes: Equatable {
    static func ==(lhs: CoreAccountAttributes, rhs: CoreAccountAttributes) -> Bool {
        return lhs.description == rhs.description && lhs.notificationTargets == rhs.notificationTargets
            && lhs.mappedValues == rhs.mappedValues
    }
}

struct NotificationTargets: Codable {
    var currentIDs: [String]
    var maximum: Int
}

extension NotificationTargets: Equatable {
    static func ==(lhs: NotificationTargets, rhs: NotificationTargets) -> Bool {
        return lhs.currentIDs == rhs.currentIDs && lhs.maximum == rhs.maximum
    }
}

typealias DatabaseItemType = DefaultIdentityTypedDatabaseItem<CoreAccountAttributes>

class DynamoEncoderDecoderTests: XCTestCase {
    
    func testEncoderDecoder() {
        let notificationTargets = NotificationTargets(currentIDs: [], maximum: 20)
        let attributes = CoreAccountAttributes(description: "Description",
                                               mappedValues: ["A": "one", "B": "two"],
                                               notificationTargets: notificationTargets)
        
        // create key and database item to create
        let key = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionKey", sortKey: "sortKey")
        let newDatabaseItem: DatabaseItemType = DefaultIdentityTypedDatabaseItem.newItem(withKey: key, andValue: attributes)
        
        let encodedAttributeValue = try! dynamoEncoder.encode(newDatabaseItem)
        
        let output: DatabaseItemType = try! dynamoDecoder.decode(encodedAttributeValue)
        
        XCTAssertEqual(newDatabaseItem.rowValue, output.rowValue)
        XCTAssertEqual("partitionKey", output.compositePrimaryKey.partitionKey)
        XCTAssertEqual("sortKey", output.compositePrimaryKey.sortKey)
    }
    
    static var allTests = [
        ("testEncoderDecoder", testEncoderDecoder),
    ]
}
