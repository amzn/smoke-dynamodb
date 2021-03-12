// Copyright 2018-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import NIO

private typealias DatabaseRowType = StandardTypedDatabaseItem<TestTypeA>

enum ExpectedQueryableTypes: PolymorphicOperationReturnType {
    typealias AttributesType = StandardPrimaryKeyAttributes
    
    static var types: [(Codable.Type, PolymorphicOperationReturnOption<StandardPrimaryKeyAttributes, Self>)] = [
        (TestTypeA.self, .init( {.testTypeA($0)} )),
        ]
    
    case testTypeA(StandardTypedDatabaseItem<TestTypeA>)
}

class SimulateConcurrencyDynamoDBCompositePrimaryKeyTableTests: XCTestCase {
    var eventLoopGroup: EventLoopGroup?
    var eventLoop: EventLoop!
    
    override func setUp() {
        super.setUp()
        
        let newEventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        eventLoop = newEventLoopGroup.next()
        eventLoopGroup = newEventLoopGroup
    }

    override func tearDown() {
        super.tearDown()
        
        try? eventLoopGroup?.syncShutdownGracefully()
        eventLoop = nil
    }
    
    func testSimulateConcurrencyOnInsert() {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop),
                                                                        eventLoop: eventLoop,
                                                                        simulateConcurrencyModifications: 5)
        
        XCTAssertThrowsError(try table.insertItem(databaseItem).wait())
    }

    fileprivate func verifyWithUpdate(table: SimulateConcurrencyDynamoDBCompositePrimaryKeyTable,
                                      databaseItem: TypedDatabaseItem<StandardPrimaryKeyAttributes, TestTypeA>,
                                      key: StandardCompositePrimaryKey,
                                      expectedFailureCount: Int) throws {
        try table.insertItem(databaseItem).wait()
        var errorCount = 0
        
        for _ in 0..<10 {
            guard let item: DatabaseRowType = try! table.getItem(forKey: key).wait() else {
                return XCTFail("Expected to retrieve item and there was none")
            }
            
            do {
                try table.updateItem(newItem: item.createUpdatedItem(withValue: item.rowValue), existingItem: item).wait()
            } catch {
                errorCount += 1
            }
        }
        
        // should fail the expected number of times
        XCTAssertEqual(expectedFailureCount, errorCount)
        
        try table.deleteItem(forKey: key).wait()
        
        let nowDeletedItem: DatabaseRowType? = try! table.getItem(forKey: key).wait()
        XCTAssertNil(nowDeletedItem)
    }
    
    func testSimulateConcurrencyWithUpdate() {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop),
                                                                        eventLoop: eventLoop,
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false)
        
        XCTAssertNoThrow(try verifyWithUpdate(table: table, databaseItem: databaseItem, key: key, expectedFailureCount: 5))
    }
   
    func testSimulateWithNoConcurrency() {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop),
                                                                        eventLoop: eventLoop,
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false,
                                                                        simulateOnUpdateItem: false)
        
        XCTAssertNoThrow(try verifyWithUpdate(table: table, databaseItem: databaseItem, key: key, expectedFailureCount: 0))
    }
    
    func testSimulateConcurrencyWithQuery() {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop),
                                                                        eventLoop: eventLoop,
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        var errorCount = 0
        
        for _ in 0..<10 {
            let query: [ExpectedQueryableTypes] = try! table.query(forPartitionKey: "partitionId",
                                                                   sortKeyCondition: .equals("sortId")).wait()
            
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
                try table.updateItem(newItem: item, existingItem: existingItem).wait()
            } catch {
                errorCount += 1
            }
        }
        
        // should only fail five times
        XCTAssertEqual(5, errorCount)
        
        XCTAssertNoThrow(try table.deleteItem(forKey: key).wait())
        
        let nowDeletedItem: DatabaseRowType? = try! table.getItem(forKey: key).wait()
        XCTAssertNil(nowDeletedItem)
    }
    
    func testSimulateConcurrencyWithMonomorphicQuery() {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop),
                                                                        eventLoop: eventLoop,
                                                                        simulateConcurrencyModifications: 5,
                                                                        simulateOnInsertItem: false)
        
        XCTAssertNoThrow(try table.insertItem(databaseItem).wait())
        var errorCount = 0
        
        for _ in 0..<10 {
            let query: [DatabaseRowType] = try! table.monomorphicQuery(forPartitionKey: "partitionId",
                                                                       sortKeyCondition: .equals("sortId")).wait()
            
            guard query.count == 1, let firstQuery = query.first else {
                return XCTFail("Expected to retrieve item and there wasn't the correct number or type.")
            }
            
            let existingItem = DatabaseRowType(compositePrimaryKey: firstQuery.compositePrimaryKey,
                                               createDate: firstQuery.createDate,
                                               rowStatus: firstQuery.rowStatus,
                                               rowValue: firstQuery.rowValue)
            let item = firstQuery.createUpdatedItem(withValue: firstQuery.rowValue)
            
            do {
                try table.updateItem(newItem: item, existingItem: existingItem).wait()
            } catch {
                errorCount += 1
            }
        }
        
        // should only fail five times
        XCTAssertEqual(5, errorCount)
        
        XCTAssertNoThrow(try table.deleteItem(forKey: key).wait())
        
        let nowDeletedItem: DatabaseRowType? = try! table.getItem(forKey: key).wait()
        XCTAssertNil(nowDeletedItem)
    }
    
    func testSimulateClobberConcurrencyWithGet() throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let wrappedTable = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        let table = SimulateConcurrencyDynamoDBCompositePrimaryKeyTable(wrappedDynamoDBTable: wrappedTable, eventLoop: eventLoop,
                                                     simulateConcurrencyModifications: 5)
        
        XCTAssertNoThrow(try wrappedTable.insertItem(databaseItem).wait())
        
        for _ in 0..<10 {
            guard let item: DatabaseRowType = try table.getItem(forKey: key).wait() else {
                return XCTFail("Expected to retrieve item and there was none")
            }
            
            XCTAssertEqual(databaseItem.rowStatus.rowVersion, item.rowStatus.rowVersion)
            
            try wrappedTable.updateItem(newItem: item.createUpdatedItem(withValue: item.rowValue), existingItem: item).wait()
            XCTAssertNoThrow(try table.clobberItem(item).wait())
        }
        
        XCTAssertNoThrow(try table.deleteItem(forKey: key).wait())
        
        let nowDeletedItem: DatabaseRowType? = try! table.getItem(forKey: key).wait()
        XCTAssertNil(nowDeletedItem)
    }
  
    static var allTests = [
        ("testSimulateConcurrencyOnInsert", testSimulateConcurrencyOnInsert),
        ("testSimulateConcurrencyWithUpdate", testSimulateConcurrencyWithUpdate),
        ("testSimulateWithNoConcurrency", testSimulateWithNoConcurrency),
        ("testSimulateConcurrencyWithQuery", testSimulateConcurrencyWithQuery),
        ("testSimulateClobberConcurrencyWithGet", testSimulateClobberConcurrencyWithGet),
    ]
}
