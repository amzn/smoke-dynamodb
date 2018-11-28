//
//  TypedDatabaseItem+RowWithItemVersionProtocolTests.swift
//  SwiftDynamo
//

import Foundation

import XCTest
@testable import SwiftDynamo

private let ORIGINAL_PAYLOAD = "Payload"
private let UPDATED_PAYLOAD = "Updated"

class TypedDatabaseItemRowWithItemVersionProtocolTests: XCTestCase {

    func testCreateUpdatedRowWithItemVersion() throws {
        let compositeKey = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: rowWithItemVersion)
        
        let updatedItem = try databaseItem.createUpdatedRowWithItemVersion(withValue: "Updated",
                                                                            conditionalStatusVersion: nil)
        
        XCTAssertEqual(1, databaseItem.rowStatus.rowVersion)
        XCTAssertEqual(1, databaseItem.rowValue.itemVersion)
        XCTAssertEqual(ORIGINAL_PAYLOAD, databaseItem.rowValue.rowValue)
        XCTAssertEqual(2, updatedItem.rowStatus.rowVersion)
        XCTAssertEqual(2, updatedItem.rowValue.itemVersion)
        XCTAssertEqual(UPDATED_PAYLOAD, updatedItem.rowValue.rowValue)
    }
    
    func testCreateUpdatedRowWithItemVersionWithCorrectConditionalVersion() throws {
        let compositeKey = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: rowWithItemVersion)
        
        let updatedItem = try databaseItem.createUpdatedRowWithItemVersion(withValue: "Updated",
                                                                            conditionalStatusVersion: 1)
        
        XCTAssertEqual(1, databaseItem.rowStatus.rowVersion)
        XCTAssertEqual(1, databaseItem.rowValue.itemVersion)
        XCTAssertEqual(ORIGINAL_PAYLOAD, databaseItem.rowValue.rowValue)
        XCTAssertEqual(2, updatedItem.rowStatus.rowVersion)
        XCTAssertEqual(2, updatedItem.rowValue.itemVersion)
        XCTAssertEqual(UPDATED_PAYLOAD, updatedItem.rowValue.rowValue)
    }
    
    func testCreateUpdatedRowWithItemVersionWithIncorrectConditionalVersion() {
        let compositeKey = DefaultIdentityCompositePrimaryKey(partitionKey: "partitionKey",
                                                                 sortKey: "sortKey")
        let rowWithItemVersion = RowWithItemVersion.newItem(withValue: "Payload")
        let databaseItem = TypedDatabaseItem.newItem(withKey: compositeKey, andValue: rowWithItemVersion)
        
        do {
            _ = try databaseItem.createUpdatedRowWithItemVersion(withValue: "Updated",
                                                                 conditionalStatusVersion: 8)
            
            XCTFail("Expected error not thrown.")
        } catch SwiftDynamoError.concurrencyError {
            return
        } catch {
            XCTFail("Unexpected error thrown: '\(error)'.")
        }
    }

    static var allTests = [
        ("testCreateUpdatedRowWithItemVersion", testCreateUpdatedRowWithItemVersion),
        ("testCreateUpdatedRowWithItemVersionWithCorrectConditionalVersion",
         testCreateUpdatedRowWithItemVersionWithCorrectConditionalVersion),
        ("testCreateUpdatedRowWithItemVersionWithIncorrectConditionalVersion",
         testCreateUpdatedRowWithItemVersionWithIncorrectConditionalVersion)
    ]
}
