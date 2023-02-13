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
//  DynamoDBCompositePrimaryKeyTableClobberVersionedItemWithHistoricalRowTests.swift
//  SmokeDynamoDBTests
//

import Foundation
import XCTest
@testable import SmokeDynamoDB
import NIO

class DynamoDBCompositePrimaryKeyTableClobberVersionedItemWithHistoricalRowTests: XCTestCase {
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
    
    func testClobberVersionedItemWithHistoricalRow() throws {
        let payload1 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let partitionKey = "partitionId"
        let historicalPartitionPrefix = "historical"
        let historicalPartitionKey = "\(historicalPartitionPrefix).\(partitionKey)"
                
        func generateSortKey(withVersion version: Int) -> String {
            let prefix = String(format: "v%05d", version)
            return [prefix, "sortId"].dynamodbKey
        }
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        
        try table.clobberVersionedItemWithHistoricalRow(forPrimaryKey: partitionKey,
                                                        andHistoricalKey: historicalPartitionKey,
                                                        item: payload1,
                                                        primaryKeyType: StandardPrimaryKeyAttributes.self,
                                                        generateSortKey: generateSortKey).wait()
        
        // the v0 row, copy of version 1
        let key1 = StandardCompositePrimaryKey(partitionKey: partitionKey, sortKey: generateSortKey(withVersion: 0))
        let item1: StandardTypedDatabaseItem<RowWithItemVersion<TestTypeA>> = try table.getItem(forKey: key1).wait()!
        XCTAssertEqual(1, item1.rowValue.itemVersion)
        XCTAssertEqual(1, item1.rowStatus.rowVersion)
        XCTAssertEqual(payload1, item1.rowValue.rowValue)
        
        // the v1 row, has version 1
        let key2 = StandardCompositePrimaryKey(partitionKey: historicalPartitionKey, sortKey: generateSortKey(withVersion: 1))
        let item2: StandardTypedDatabaseItem<RowWithItemVersion<TestTypeA>> = try table.getItem(forKey: key2).wait()!
        XCTAssertEqual(1, item2.rowValue.itemVersion)
        XCTAssertEqual(1, item2.rowStatus.rowVersion)
        XCTAssertEqual(payload1, item2.rowValue.rowValue)
        
        let payload2 = TestTypeA(firstly: "thirdly", secondly: "fourthly")
        
        try table.clobberVersionedItemWithHistoricalRow(forPrimaryKey: partitionKey,
                                                        andHistoricalKey: historicalPartitionKey,
                                                        item: payload2,
                                                        primaryKeyType: StandardPrimaryKeyAttributes.self,
                                                        generateSortKey: generateSortKey).wait()
        
        // the v0 row, copy of version 2
        let key3 = StandardCompositePrimaryKey(partitionKey: partitionKey, sortKey: generateSortKey(withVersion: 0))
        let item3: StandardTypedDatabaseItem<RowWithItemVersion<TestTypeA>> = try table.getItem(forKey: key3).wait()!
        XCTAssertEqual(2, item3.rowValue.itemVersion)
        XCTAssertEqual(2, item3.rowStatus.rowVersion)
        XCTAssertEqual(payload2, item3.rowValue.rowValue)
        
        // the v1 row, still has version 1
        let key4 = StandardCompositePrimaryKey(partitionKey: historicalPartitionKey, sortKey: generateSortKey(withVersion: 1))
        let item4: StandardTypedDatabaseItem<RowWithItemVersion<TestTypeA>> = try table.getItem(forKey: key4).wait()!
        XCTAssertEqual(1, item4.rowValue.itemVersion)
        XCTAssertEqual(1, item4.rowStatus.rowVersion)
        XCTAssertEqual(payload1, item4.rowValue.rowValue)
        
        // the v2 row, has version 2
        let key5 = StandardCompositePrimaryKey(partitionKey: historicalPartitionKey, sortKey: generateSortKey(withVersion: 2))
        let item5: StandardTypedDatabaseItem<RowWithItemVersion<TestTypeA>> = try table.getItem(forKey: key5).wait()!
        XCTAssertEqual(2, item5.rowValue.itemVersion)
        XCTAssertEqual(1, item5.rowStatus.rowVersion)
        XCTAssertEqual(payload2, item5.rowValue.rowValue)
    }
}
