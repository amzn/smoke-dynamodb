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
//  DynamoDBCompositePrimaryKeyTableTests.swift
//  SmokeDynamoDBTests
//

import XCTest
@testable import SmokeDynamoDB
import NIO

class DynamoDBCompositePrimaryKeyTableTests: XCTestCase {
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
    
    public func testmonomorphicBulkWriteWithFallback() throws {
        // Length of insert statements of payload1 is larger than the limitation
        let payload1 = TestTypeA(
            firstly: "firstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstlyfirstly",
            secondly: "secondly")
        let partitionKey1 = "partitionId1"
        let key1 = StandardCompositePrimaryKey(partitionKey: partitionKey1,
                                               sortKey: "sortId")
        let payload2 = TestTypeA(firstly: "firstly", secondly: "secondly")
        let partitionKey2 = "partitionId2"
        let key2 = StandardCompositePrimaryKey(partitionKey: partitionKey2,
                                              sortKey: "sortId")
        
        var nodeEntries: [(key: String, entry: TestTypeAWriteEntry)] = []
        nodeEntries.append((partitionKey1, TestTypeAWriteEntry.insert(new: StandardTypedDatabaseItem.newItem(withKey: key1, andValue: payload1))))
        nodeEntries.append((partitionKey2, TestTypeAWriteEntry.insert(new: StandardTypedDatabaseItem.newItem(withKey: key2, andValue: payload2))))
        
        let table = InMemoryDynamoDBCompositePrimaryKeyTable(eventLoop: eventLoop)
        do {
            try table.validateEntry(entry: nodeEntries.map(\.entry)[0])
            XCTFail("Expect error doesn't throw")
        } catch SmokeDynamoDBError.statementLengthExceeded {
            //expect error. Entry1's statement length should exceed the limitation
        } catch {
            XCTFail("Unexpect error \(error)")
        }
        
        _ = try table.monomorphicBulkWriteWithFallback(nodeEntries.map(\.entry)).wait()
        
        // verify the item has inserted
        let inserted1: StandardTypedDatabaseItem<TestTypeA> = (try table.getItem(forKey: key1).wait())!
        // verify item1 which exceed statements limitation inserted
        XCTAssertEqual(inserted1.rowValue.firstly, payload1.firstly)
        XCTAssertEqual(inserted1.rowValue.secondly, payload1.secondly)
        let inserted2: StandardTypedDatabaseItem<TestTypeA> = (try table.getItem(forKey: key2).wait())!
        XCTAssertEqual(inserted2.rowValue.firstly, payload2.firstly)
        XCTAssertEqual(inserted2.rowValue.secondly, payload2.secondly)
    }
}