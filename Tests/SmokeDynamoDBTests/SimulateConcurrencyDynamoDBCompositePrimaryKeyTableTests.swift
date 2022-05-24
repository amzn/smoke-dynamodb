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
//  SimulateConcurrencyDynamoDBCompositePrimaryKeyTableTests.swift
//  SmokeDynamoDBTests
//
import XCTest
@testable import SmokeDynamoDB
import SmokeHTTPClient
import DynamoDBModel

private typealias DatabaseRowType = StandardTypedDatabaseItem<TestTypeA>

enum ExpectedQueryableTypes: PolymorphicOperationReturnType {
    typealias AttributesType = StandardPrimaryKeyAttributes
    
    static var types: [(Codable.Type, PolymorphicOperationReturnOption<StandardPrimaryKeyAttributes, Self>)] = [
        (TestTypeA.self, .init( {.testTypeA($0)} )),
        ]
    
    case testTypeA(StandardTypedDatabaseItem<TestTypeA>)
}

class SimulateConcurrencyDynamoDBCompositePrimaryKeyTableTests: XCTestCase {
    
    func testSimulateConcurrencyOnInsert() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: InMemoryDynamoDBCompositePrimaryKeyTable(),
                                                                        simulateConcurrencyModifications: 5)
        
        do {
            try await table.insertItem(databaseItem)
            XCTFail()
        } catch {
            // expected error thrown
        }
    }

    fileprivate func verifyWithUpdate(table: SimulateConcurrencyDynamoDBCompositePrimaryKeyTable,
                                      databaseItem: TypedDatabaseItem<StandardPrimaryKeyAttributes, TestTypeA>,
                                      key: StandardCompositePrimaryKey,
                                      expectedFailureCount: Int) async throws {
        try await table.insertItem(databaseItem)
        var errorCount = 0
        
        for _ in 0..<10 {
            guard let item: DatabaseRowType = try! await table.getItem(forKey: key) else {
                return XCTFail("Expected to retrieve item and there was none")
            }
            
            do {
                try await table.updateItem(newItem: item.createUpdatedItem(withValue: item.rowValue), existingItem: item)
            } catch {
                errorCount += 1
            }
        }
        
        // should fail the expected number of times
        XCTAssertEqual(expectedFailureCount, errorCount)
        
        try await table.deleteItem(forKey: key)
        
        let nowDeletedItem: DatabaseRowType? = try! await table.getItem(forKey: key)
        XCTAssertNil(nowDeletedItem)
    }
    
    func testSimulateConcurrencyWithUpdate() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: InMemoryDynamoDBCompositePrimaryKeyTable(),
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false)
        
        try await verifyWithUpdate(table: table, databaseItem: databaseItem, key: key, expectedFailureCount: 5)
    }
   
    func testSimulateWithNoConcurrency() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: InMemoryDynamoDBCompositePrimaryKeyTable(),
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false,
                                                                        simulateOnUpdateItem: false)
        
        try await verifyWithUpdate(table: table, databaseItem: databaseItem, key: key, expectedFailureCount: 0)
    }
    
    func testSimulateConcurrencyWithQuery() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: InMemoryDynamoDBCompositePrimaryKeyTable(),
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false)
        
        try await table.insertItem(databaseItem)
        var errorCount = 0
        
        for _ in 0..<10 {
            let query: [ExpectedQueryableTypes] = try! await table.query(forPartitionKey: "partitionId",
                                                                         sortKeyCondition: .equals("sortId"))
            
            guard query.count == 1, case let .testTypeA(firstDatabaseItem) = query[0] else {
                return XCTFail("Expected to retrieve item and there wasn't the correct number or type.")
            }
            
            let firstValue = firstDatabaseItem.rowValue
            
            let existingItem = DatabaseRowType(compositePrimaryKey: firstDatabaseItem.compositePrimaryKey,
                                               createDate: firstDatabaseItem.createDate,
                                               rowStatus: firstDatabaseItem.rowStatus,
                                               rowValue: firstValue)
            let item = firstDatabaseItem.createUpdatedItem(withValue: firstValue)
            
            do {
                try await table.updateItem(newItem: item, existingItem: existingItem)
            } catch {
                errorCount += 1
            }
        }
        
        // should only fail five times
        XCTAssertEqual(5, errorCount)
        
        try await table.deleteItem(forKey: key)
        
        let nowDeletedItem: DatabaseRowType? = try! await table.getItem(forKey: key)
        XCTAssertNil(nowDeletedItem)
    }
    
    func testSimulateConcurrencyWithMonomorphicQuery() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: InMemoryDynamoDBCompositePrimaryKeyTable(),
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false)
        
        try await table.insertItem(databaseItem)
        var errorCount = 0
        
        for _ in 0..<10 {
            let query: [DatabaseRowType] = try! await table.monomorphicQuery(forPartitionKey: "partitionId",
                                                                             sortKeyCondition: .equals("sortId"))
            
            guard query.count == 1, let firstQuery = query.first else {
                return XCTFail("Expected to retrieve item and there wasn't the correct number or type.")
            }
            
            let existingItem = DatabaseRowType(compositePrimaryKey: firstQuery.compositePrimaryKey,
                                               createDate: firstQuery.createDate,
                                               rowStatus: firstQuery.rowStatus,
                                               rowValue: firstQuery.rowValue)
            let item = firstQuery.createUpdatedItem(withValue: firstQuery.rowValue)
            
            do {
                try await table.updateItem(newItem: item, existingItem: existingItem)
            } catch {
                errorCount += 1
            }
        }
        
        // should only fail five times
        XCTAssertEqual(5, errorCount)
        
        try await table.deleteItem(forKey: key)
        
        let nowDeletedItem: DatabaseRowType? = try! await table.getItem(forKey: key)
        XCTAssertNil(nowDeletedItem)
    }
    
    func testSimulateClobberConcurrencyWithGet() async throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 5)
        
        try await wrappedTable.insertItem(databaseItem)
        
        for _ in 0..<10 {
            guard let item: DatabaseRowType = try await table.getItem(forKey: key) else {
                return XCTFail("Expected to retrieve item and there was none")
            }
            
            XCTAssertEqual(databaseItem.rowStatus.rowVersion, item.rowStatus.rowVersion)
            
            try await wrappedTable.updateItem(newItem: item.createUpdatedItem(withValue: item.rowValue), existingItem: item)
            try await table.clobberItem(item)
        }
        
        try await table.deleteItem(forKey: key)
        
        let nowDeletedItem: DatabaseRowType? = try! await table.getItem(forKey: key)
        XCTAssertNil(nowDeletedItem)
    }
    
    func testSimulateConcurrencyWithMonomorphicBulkWriteWithoutThrowing() async throws {

        typealias TestObject = TestTypeA
        typealias TestObjectDatabaseItem = StandardTypedDatabaseItem<TestObject>
        typealias TestObjectWriteEntry = StandardWriteEntry<TestObject>
    
        
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable()
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 5)
    
        var entryList: [TestObjectWriteEntry] = []
        var i = 0
        while(i < 26) {
            let key = StandardCompositePrimaryKey(partitionKey: "partitionId\(i%25)", sortKey: "sortId\(i%25)")
            let test = TestObject(firstly: "firstly", secondly: "secondly")
            let testItem: TestObjectDatabaseItem = TestObjectDatabaseItem.newItem(withKey: key, andValue: test)
            entryList.append(TestObjectWriteEntry.insert(new: testItem))
            i += 1
        }
        do {
            let result = try await table.monomorphicBulkWriteWithoutThrowing(entryList)
            
            XCTAssertEqual(result.count, 1)
            if let _ = result[25] {
                return
            } else {
                XCTFail("should return duplicated index = 25")
            }

            entryList = []
            i = 0
            while(i < 30) {
                let key = StandardCompositePrimaryKey(partitionKey: "partitionId\(i)", sortKey: "sortId\(i)")
                let test = TestObject(firstly: "firstly", secondly: "secondly")
                let testItem: TestObjectDatabaseItem = TestObjectDatabaseItem.newItem(withKey: key, andValue: test)
                entryList.append(TestObjectWriteEntry.insert(new: testItem))
                i += 1
            }

            let result25 = try await table.monomorphicBulkWriteWithoutThrowing(entryList)
            XCTAssertEqual(result25.count, 25)

            let result30 = try await table.monomorphicBulkWriteWithoutThrowing(entryList)
            XCTAssertEqual(result30.count, 30)
        } catch {
            XCTFail("monomorphicBulkWriteWithoutThrowing should not throw error")
        }
    }
   
    static var allTests = [
        ("testSimulateConcurrencyOnInsert", testSimulateConcurrencyOnInsert),
        ("testSimulateConcurrencyWithUpdate", testSimulateConcurrencyWithUpdate),
        ("testSimulateWithNoConcurrency", testSimulateWithNoConcurrency),
        ("testSimulateConcurrencyWithQuery", testSimulateConcurrencyWithQuery),
        ("testSimulateClobberConcurrencyWithGet", testSimulateClobberConcurrencyWithGet),
        ("testSimulateConcurrencyWithMonomorphicBulkWriteWithoutThrowing", testSimulateConcurrencyWithMonomorphicBulkWriteWithoutThrowing)
    ]
}
