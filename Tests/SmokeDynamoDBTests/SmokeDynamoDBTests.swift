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
// SmokeDynamoDBTests.swift
// SmokeDynamoDBTests
//

import XCTest
@testable import SmokeDynamoDB
import DynamoDBModel

fileprivate let dynamodbEncoder = DynamoDBEncoder()
fileprivate let dynamodbDecoder = DynamoDBDecoder()

fileprivate func createDecoder() -> JSONDecoder {
    let jsonDecoder = JSONDecoder()
    #if os (Linux)
        jsonDecoder.dateDecodingStrategy = .iso8601
    #elseif os (OSX)
        if #available(OSX 10.12, *) {
            jsonDecoder.dateDecodingStrategy = .iso8601
        }
    #endif
    
    return jsonDecoder
}

fileprivate let jsonDecoder = createDecoder()

fileprivate func assertNoThrow<T>(_ body: @autoclosure () throws -> T) -> T? {
    do {
        return try body()
    } catch {
        XCTFail(error.localizedDescription)
    }
    
    return nil
}

class SmokeDynamoDBTests: XCTestCase {
    
    func testEncodeTypedItem() {
        let inputData = serializedTypeADatabaseItem.data(using: .utf8)!
        
        guard let jsonAttributeValue = assertNoThrow(
            try jsonDecoder.decode(DynamoDBModel.AttributeValue.self, from: inputData)) else {
                return
        }
        
        guard let databaseItem: StandardTypedDatabaseItem<TypeA> = assertNoThrow(
            try dynamodbDecoder.decode(jsonAttributeValue)) else {
                return
        }
        
        guard let decodeAttributeValue = assertNoThrow(
            try dynamodbEncoder.encode(databaseItem)) else {
                return
        }
        
        XCTAssertEqual(decodeAttributeValue.M!.count, jsonAttributeValue.M!.count)
    }

    func testTypedDatabaseItem() {
        let inputData = serializedTypeADatabaseItem.data(using: .utf8)!
        
        guard let attributeValue = assertNoThrow(
                try jsonDecoder.decode(DynamoDBModel.AttributeValue.self, from: inputData)) else {
            return
        }
        
        guard let databaseItem: StandardTypedDatabaseItem<TypeA> = assertNoThrow(
                try dynamodbDecoder.decode(attributeValue)) else {
            return
        }
        
        XCTAssertEqual(databaseItem.rowValue.firstly, "aaa")
        XCTAssertEqual(databaseItem.rowValue.secondly, "bbb")
        XCTAssertEqual(databaseItem.rowStatus.rowVersion, 5)
        
        // create an updated item from the decoded one
        let newItem = TypeA(firstly: "hello", secondly: "world!!")
        let updatedItem = databaseItem.createUpdatedItem(withValue: newItem)
        XCTAssertEqual(updatedItem.rowStatus.rowVersion, 6)
    }
  
    func testPolymorphicDatabaseItemList() {
        let inputData = serializedPolymorphicDatabaseItemList.data(using: .utf8)!
        
        guard let attributeValues = assertNoThrow(
                try jsonDecoder.decode([DynamoDBModel.AttributeValue].self, from: inputData)) else {
            return
        }
        
        let itemsOptional: [StandardPolymorphicDatabaseItem<AllCodableTypes>]? = assertNoThrow(
            try attributeValues.map { value in
            return try dynamodbDecoder.decode(value)
        })
        
        guard let items = itemsOptional else {
            return
        }
        
        XCTAssertEqual(items.count, 2)
        
        let first = items[0].rowValue as! TypeA
        let second = items[1].rowValue as! TypeB
        
        XCTAssertEqual(first.firstly, "aaa")
        XCTAssertEqual(first.secondly, "bbb")
        XCTAssertEqual(items[0].rowStatus.rowVersion, 5)
        
        XCTAssertEqual(second.thirdly, "ccc")
        XCTAssertEqual(second.fourthly, "ddd")
        XCTAssertEqual(items[1].rowStatus.rowVersion, 12)
        
        // try to create up updated item with the correct type
        let newItem = TypeA(firstly: "hello", secondly: "world!!")
        
        guard let updatedItem = assertNoThrow(
                try items[0].createUpdatedItem(withValue: newItem)) else {
            return
        }
        
        XCTAssertEqual(updatedItem.rowStatus.rowVersion, 6)
        
        do {
            let updatedItem = try items[1].createUpdatedItem(withValue: newItem)
            XCTAssertEqual(updatedItem.rowStatus.rowVersion, 6)
        } catch SmokeDynamoDBError.typeMismatch(expected: let expected, provided: let provided) {
            XCTAssertEqual(expected, String(describing: TypeB.self))
            XCTAssertEqual(provided, String(describing: TypeA.self))
            
            return
        } catch {
            XCTFail("Incorrect error thrown.")
        }
        
        XCTFail("Decoding error expected.")
    }
    
    func testPolymorphicDatabaseItemListUnknownType() {
        let inputData = serializedPolymorphicDatabaseItemList.data(using: .utf8)!
        
        guard let attributeValues = assertNoThrow(
                try jsonDecoder.decode([DynamoDBModel.AttributeValue].self, from: inputData)) else {
            return
        }
        
        do {
            let _: [StandardPolymorphicDatabaseItem<SomeCodableTypes>] = try attributeValues.map { value in
                return try dynamodbDecoder.decode(value)
            }
        } catch SmokeDynamoDBError.unexpectedType(provided: let provided) {
            XCTAssertEqual(provided, "TypeBCustom")
            
            return
        } catch {
            XCTFail("Incorrect error thrown.")
        }
        
        XCTFail("Decoding error expected.")
    }
    
    func testPolymorphicDatabaseItemListWithIndex() {
        let inputData = serializedPolymorphicDatabaseItemListWithIndex.data(using: .utf8)!
        
        guard let attributeValues = assertNoThrow(
                try jsonDecoder.decode([DynamoDBModel.AttributeValue].self, from: inputData)) else {
            return
        }
        
        let itemsOptional: [StandardPolymorphicDatabaseItem<AllCodableTypesWithIndex>]? = assertNoThrow(
            try attributeValues.map { value in
            return try dynamodbDecoder.decode(value)
        })
        
        guard let items = itemsOptional else {
            return
        }
        
        XCTAssertEqual(items.count, 2)
        
        let first = items[0].rowValue as! RowWithIndex<TypeA, GSI1PKIndexIdentity>
        let second = items[1].rowValue as! TypeB
        
        XCTAssertEqual(first.rowValue.firstly, "aaa")
        XCTAssertEqual(first.rowValue.secondly, "bbb")
        XCTAssertEqual(first.indexValue, "gsi-index")
        XCTAssertEqual(items[0].rowStatus.rowVersion, 5)
        
        XCTAssertEqual(second.thirdly, "ccc")
        XCTAssertEqual(second.fourthly, "ddd")
        XCTAssertEqual(items[1].rowStatus.rowVersion, 12)
        
        // try to create up updated item with the correct type
        let newItem = TypeA(firstly: "hello", secondly: "world!!")
        let newRowWithIndex = first.createUpdatedItem(withValue: newItem)
        
        guard let updatedItem = assertNoThrow(
                try items[0].createUpdatedItem(withValue: newRowWithIndex)) else {
            return
        }
        
        XCTAssertEqual(updatedItem.rowStatus.rowVersion, 6)
    }

    static var allTests = [
        ("testTypedDatabaseItem", testTypedDatabaseItem),
        ("testPolymorphicDatabaseItemList", testPolymorphicDatabaseItemList),
        ("testPolymorphicDatabaseItemListUnknownType", testPolymorphicDatabaseItemListUnknownType),
        ("testPolymorphicDatabaseItemListWithIndex", testPolymorphicDatabaseItemListWithIndex),
    ]
}
