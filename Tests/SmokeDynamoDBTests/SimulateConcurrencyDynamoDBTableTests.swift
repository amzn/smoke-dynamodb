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
//  SimulateConcurrencyDynamoDBTableTests.swift
//  SmokeDynamoDBTests
//

import XCTest
@testable import SmokeDynamoDB
import SmokeHTTPClient
import DynamoDBModel

private typealias DatabaseRowType = StandardTypedDatabaseItem<TestTypeA>
private typealias QueryRowType = StandardPolymorphicDatabaseItem<ExpectedTypes>

class SimulateConcurrencyDynamoDBTableTests: XCTestCase {
    
    func testSimulateConcurrencyOnInsertSync() {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: InMemoryDynamoDBTable(),
                                                     simulateConcurrencyModifications: 5)
        
        XCTAssertThrowsError(try table.insertItemSync(databaseItem))
    }
    
    func testSimulateConcurrencyOnInsertAsync() throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: InMemoryDynamoDBTable(),
                                                     simulateConcurrencyModifications: 5)
        
        var errorThrown = false
        try table.insertItemAsync(databaseItem) { error in
            errorThrown = errorThrown || error != nil
        }
        
        XCTAssertTrue(errorThrown)
    }

    fileprivate func verifyWithUpdateSync(table: SimulateConcurrencyDynamoDBTable,
                                          databaseItem: TypedDatabaseItem<StandardPrimaryKeyAttributes, TestTypeA>,
                                          key: StandardCompositePrimaryKey,
                                          expectedFailureCount: Int) throws {
        try table.insertItemSync(databaseItem)
        var errorCount = 0
        
        for _ in 0..<10 {
            guard let item: DatabaseRowType = try! table.getItemSync(forKey: key) else {
                return XCTFail("Expected to retrieve item and there was none")
            }
            
            do {
                try table.updateItemSync(newItem: item.createUpdatedItem(withValue: item.rowValue), existingItem: item)
            } catch {
                errorCount += 1
            }
        }
        
        // should fail the expected number of times
        XCTAssertEqual(expectedFailureCount, errorCount)
        
        try table.deleteItemSync(forKey: key)
        
        let nowDeletedItem: DatabaseRowType? = try! table.getItemSync(forKey: key)
        XCTAssertNil(nowDeletedItem)
    }
    
    fileprivate func verifyWithUpdateAsync(table: SimulateConcurrencyDynamoDBTable,
                                           databaseItem: TypedDatabaseItem<StandardPrimaryKeyAttributes, TestTypeA>,
                                           key: StandardCompositePrimaryKey,
                                           expectedFailureCount: Int) throws {
        var isInsertCompleted = false
        func insertCompletionHandler(error: Error?) {
            if error != nil {
                XCTFail()
            }
            
            isInsertCompleted = true
        }
        
        try table.insertItemAsync(databaseItem, completion: insertCompletionHandler)
        var errorCount = 0
        
        func updateItemCompletion(error: Error?) {
            if error != nil {
                errorCount += 1
            }
        }
        
        var getItemCompletionCount = 0
        for _ in 0..<10 {
            try table.getItemAsync(forKey: key) { (result: SmokeDynamoDBErrorResult<DatabaseRowType?>) in
                switch result {
                case .success(let itemOptional):
                    guard let item = itemOptional else {
                        return XCTFail("Expected to retrieve item and there was none")
                    }
                    
                    try! table.updateItemAsync(newItem: item.createUpdatedItem(withValue: item.rowValue),
                                                existingItem: item, completion: updateItemCompletion)
                case .failure(_):
                    XCTFail()
                }
                
                getItemCompletionCount += 1
            }
        }
        
        // should fail the expected number of times
        XCTAssertEqual(expectedFailureCount, errorCount)
        
        var isDeleteCompleted = false
        func deletionCompletionHandler(error: Error?) {
            if error != nil {
                XCTFail()
            }
            
            isDeleteCompleted = true
        }
        
        try table.deleteItemAsync(forKey: key, completion: deletionCompletionHandler)
        
        let nowDeletedItem: DatabaseRowType? = try! table.getItemSync(forKey: key)
        XCTAssertNil(nowDeletedItem)
        XCTAssertTrue(isInsertCompleted)
        XCTAssertEqual(10, getItemCompletionCount)
        XCTAssertTrue(isDeleteCompleted)
    }
    
    func testSimulateConcurrencyWithUpdateSync() {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: InMemoryDynamoDBTable(),
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)
        
        XCTAssertNoThrow(try verifyWithUpdateAsync(table: table, databaseItem: databaseItem, key: key, expectedFailureCount: 5))
    }
    
    func testSimulateConcurrencyWithUpdateAsync() {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: InMemoryDynamoDBTable(),
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)
        
        XCTAssertNoThrow(try verifyWithUpdateAsync(table: table, databaseItem: databaseItem, key: key, expectedFailureCount: 5))
    }
   
    func testSimulateWithNoConcurrencySync() {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: InMemoryDynamoDBTable(),
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false,
                                                     simulateOnUpdateItem: false)
        
        XCTAssertNoThrow(try verifyWithUpdateSync(table: table, databaseItem: databaseItem, key: key, expectedFailureCount: 0))
    }
    
    func testSimulateWithNoConcurrencyAsync() {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: InMemoryDynamoDBTable(),
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false,
                                                     simulateOnUpdateItem: false)
        
        XCTAssertNoThrow(try verifyWithUpdateAsync(table: table, databaseItem: databaseItem, key: key, expectedFailureCount: 0))
    }
    
    func testSimulateConcurrencyWithQuerySync() {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: InMemoryDynamoDBTable(),
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)
        
        XCTAssertNoThrow(try table.insertItemSync(databaseItem))
        var errorCount = 0
        
        for _ in 0..<10 {
            let query: [QueryRowType] = try! table.querySync(forPartitionKey: "partitionId",
                                                      sortKeyCondition: .equals("sortId"))
            
            guard query.count == 1, let firstValue = query[0].rowValue as? TestTypeA else {
                return XCTFail("Expected to retrieve item and there wasn't the correct number or type.")
            }
            
            let firstQuery = query[0]
            let existingItem = DatabaseRowType(compositePrimaryKey: firstQuery.compositePrimaryKey,
                                               createDate: firstQuery.createDate,
                                               rowStatus: firstQuery.rowStatus,
                                               rowValue: firstQuery.rowValue as! TestTypeA)
            let item: DatabaseRowType
            
            do {
                item = try firstQuery.createUpdatedItem(withValue: firstValue)
            } catch {
                return XCTFail("Unexpectedly failed to convert types \(error)")
            }
            
            do {
                try table.updateItemSync(newItem: item, existingItem: existingItem)
            } catch {
                errorCount += 1
            }
        }
        
        // should only fail five times
        XCTAssertEqual(5, errorCount)
        
        XCTAssertNoThrow(try table.deleteItemSync(forKey: key))
        
        let nowDeletedItem: DatabaseRowType? = try! table.getItemSync(forKey: key)
        XCTAssertNil(nowDeletedItem)
    }
    
    func testSimulateConcurrencyWithQueryAsync() throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: InMemoryDynamoDBTable(),
                                                     simulateConcurrencyModifications: 5,
                                                     simulateOnInsertItem: false)
        
        XCTAssertNoThrow(try table.insertItemSync(databaseItem))
        var errorCount = 0
        
        func updateItemCompletion(error: Error?) {
            if error != nil {
                errorCount += 1
            }
        }
        
        var queryCompletionCount = 0
        for _ in 0..<10 {
            func handleQueryResult(result: SmokeDynamoDBErrorResult<[QueryRowType]>) {
                switch result {
                case .success(let query):
                    guard query.count == 1, let firstValue = query[0].rowValue as? TestTypeA else {
                        return XCTFail("Expected to retrieve item and there wasn't the correct number or type.")
                    }
                    
                    let firstQuery = query[0]
                    let existingItem = DatabaseRowType(compositePrimaryKey: firstQuery.compositePrimaryKey,
                                                       createDate: firstQuery.createDate,
                                                       rowStatus: firstQuery.rowStatus,
                                                       rowValue: firstQuery.rowValue as! TestTypeA)
                    let item: DatabaseRowType
                    
                    do {
                        item = try firstQuery.createUpdatedItem(withValue: firstValue)
                    } catch {
                        return XCTFail("Unexpectedly failed to convert types \(error)")
                    }
                    
                    do {
                        try table.updateItemAsync(newItem: item, existingItem: existingItem,
                                                   completion: updateItemCompletion)
                    } catch {
                        errorCount += 1
                    }
                case .failure:
                    XCTFail()
                }
                
                queryCompletionCount += 1
            }
            try table.queryAsync(forPartitionKey: "partitionId",
                                  sortKeyCondition: .equals("sortId"), completion: handleQueryResult)
        }
        
        // should only fail five times
        XCTAssertEqual(5, errorCount)
        
        var isDeleteCompleted = false
        func deleteCompletionHandler(error: Error?) {
            if error != nil {
                XCTFail()
            }
            
            isDeleteCompleted = true
        }
        
        XCTAssertNoThrow(try table.deleteItemAsync(forKey: key, completion: deleteCompletionHandler))
        
        let nowDeletedItem: DatabaseRowType? = try! table.getItemSync(forKey: key)
        XCTAssertNil(nowDeletedItem)
        XCTAssertEqual(10, queryCompletionCount)
        XCTAssertTrue(isDeleteCompleted)
    }
    
    func testSimulateClobberConcurrencyWithGetSync() throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let wrappedTable = InMemoryDynamoDBTable()
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 5)
        
        XCTAssertNoThrow(try wrappedTable.insertItemSync(databaseItem))
        
        for _ in 0..<10 {
            guard let item: DatabaseRowType = try table.getItemSync(forKey: key) else {
                return XCTFail("Expected to retrieve item and there was none")
            }
            
            XCTAssertEqual(databaseItem.rowStatus.rowVersion, item.rowStatus.rowVersion)
            
            try wrappedTable.updateItemSync(newItem: item.createUpdatedItem(withValue: item.rowValue), existingItem: item)
            XCTAssertNoThrow(try table.clobberItemSync(item))
        }
        
        XCTAssertNoThrow(try table.deleteItemSync(forKey: key))
        
        let nowDeletedItem: DatabaseRowType? = try! table.getItemSync(forKey: key)
        XCTAssertNil(nowDeletedItem)
    }
    
    func testSimulateClobberConcurrencyWithGetAsync() throws {
        let key = StandardCompositePrimaryKey(partitionKey: "partitionId", sortKey: "sortId")
        let payload = TestTypeA(firstly: "firstly", secondly: "secondly")
        
        let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
        let wrappedTable = InMemoryDynamoDBTable()
        let table = SimulateConcurrencyDynamoDBTable(wrappedDynamoDBTable: wrappedTable,
                                                     simulateConcurrencyModifications: 5)
        
        XCTAssertNoThrow(try wrappedTable.insertItemSync(databaseItem))
        
        var getItemCompletionCount = 0
        var updateCompletionCount = 0
        var clobberCompletionCount = 0
        for _ in 0..<10 {
            func handleGetItemResult(result: SmokeDynamoDBErrorResult<DatabaseRowType?>) {
                switch result {
                case .success(let itemOptional):
                    guard let item = itemOptional else {
                        return XCTFail("Expected to retrieve item and there was none")
                    }
                    
                    XCTAssertEqual(databaseItem.rowStatus.rowVersion, item.rowStatus.rowVersion)
            
                    try! wrappedTable.updateItemAsync(newItem: item.createUpdatedItem(withValue: item.rowValue),
                                                       existingItem: item) { _ in updateCompletionCount += 1 }
                    XCTAssertNoThrow(try table.clobberItemAsync(item) { _ in clobberCompletionCount += 1 })
                case .failure(_):
                    XCTFail()
                }
                
                getItemCompletionCount += 1
            }
            
            try table.getItemAsync(forKey: key, completion: handleGetItemResult)
        }
        
        var isDeleteCompleted = false
        func deleteCompletionHandler(error: Error?) {
            if error != nil {
                XCTFail()
            }
            
            isDeleteCompleted = true
        }
        
        XCTAssertNoThrow(try table.deleteItemAsync(forKey: key, completion: deleteCompletionHandler))
        
        let nowDeletedItem: DatabaseRowType? = try! table.getItemSync(forKey: key)
        XCTAssertNil(nowDeletedItem)
        XCTAssertEqual(10, getItemCompletionCount)
        XCTAssertEqual(10, updateCompletionCount)
        XCTAssertEqual(10, clobberCompletionCount)
        XCTAssertTrue(isDeleteCompleted)
    }
  
    static var allTests = [
        ("testSimulateConcurrencyOnInsertSync", testSimulateConcurrencyOnInsertSync),
        ("testSimulateConcurrencyOnInsertAsync", testSimulateConcurrencyOnInsertAsync),
        ("testSimulateConcurrencyWithUpdateSync", testSimulateConcurrencyWithUpdateSync),
        ("testSimulateConcurrencyWithUpdateAsync", testSimulateConcurrencyWithUpdateAsync),
        ("testSimulateWithNoConcurrencySync", testSimulateWithNoConcurrencySync),
        ("testSimulateWithNoConcurrencyAsync", testSimulateWithNoConcurrencyAsync),
        ("testSimulateConcurrencyWithQuerySync", testSimulateConcurrencyWithQuerySync),
        ("testSimulateConcurrencyWithQueryAsync", testSimulateConcurrencyWithQueryAsync),
        ("testSimulateClobberConcurrencyWithGetSync", testSimulateClobberConcurrencyWithGetSync),
        ("testSimulateClobberConcurrencyWithGetAsync", testSimulateClobberConcurrencyWithGetAsync)
    ]
}
