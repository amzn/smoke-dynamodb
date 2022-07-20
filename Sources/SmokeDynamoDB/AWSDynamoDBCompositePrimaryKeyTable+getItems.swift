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
//  AWSDynamoDBCompositePrimaryKeyTable+getItems.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeAWSCore
import DynamoDBModel
import SmokeHTTPClient
import Logging
import NIO

// BatchGetItem has a maximum of 100 of items per request
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
private let maximumKeysPerGetItemBatch = 100

/// DynamoDBTable conformance getItems function
public extension AWSDynamoDBCompositePrimaryKeyTable {
    /**
     Helper type that manages the state of a getItems request.
     
     As suggested here - https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html - this helper type
     monitors the unprocessed items returned in the response from DynamoDB and uses an exponential backoff algorithm to retry those items using
     the same retry configuration as the underlying DynamoDB client.
     */
    private class GetItemsRetriable<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType> {
        typealias OutputType = [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]
        
        let dynamodb: _AWSDynamoDBClient<InvocationReportingType>
        let eventLoop: EventLoop
        
        private let retryQueue = DispatchQueue.global()
        
        var retriesRemaining: Int
        var input: BatchGetItemInput
        var outputItems: OutputType = [:]
        
        init(initialInput: BatchGetItemInput,
             dynamodb: _AWSDynamoDBClient<InvocationReportingType>,
             eventLoopOverride eventLoop: EventLoop) {
            self.dynamodb = dynamodb
            self.eventLoop = eventLoop
            self.retriesRemaining = dynamodb.retryConfiguration.numRetries
            self.input = initialInput
        }
        
        func batchGetItem() -> EventLoopFuture<OutputType> {
            // submit the asynchronous request
            return self.dynamodb.batchGetItem(input: self.input).flatMap { output -> EventLoopFuture<OutputType> in
                let errors = output.responses?.flatMap({ (tableName, itemList) -> [Error] in
                    return itemList.compactMap { values -> Error? in
                        do {
                            let attributeValue = DynamoDBModel.AttributeValue(M: values)
                            
                            let decodedItem: ReturnTypeDecodable<ReturnedType> = try DynamoDBDecoder().decode(attributeValue)
                            let decodedValue = decodedItem.decodedValue
                            let key = decodedValue.getItemKey()
                                                            
                            self.outputItems[key] = decodedValue
                            return nil
                        } catch {
                            return error
                        }
                    }
                }) ?? []
                
                if !errors.isEmpty {
                    let promise = self.eventLoop.makePromise(of: OutputType.self)
                    let error = SmokeDynamoDBError.multipleUnexpectedErrors(cause: errors)
                    promise.fail(error)
                    return promise.futureResult
                }
                
                if let requestItems = output.unprocessedKeys, !requestItems.isEmpty {
                    self.input = BatchGetItemInput(requestItems: requestItems)
                    
                    return self.getNextFuture()
                }
                
                let promise = self.eventLoop.makePromise(of: OutputType.self)
                promise.succeed(self.outputItems)
                return promise.futureResult
            }
        }
        
        func getNextFuture() -> EventLoopFuture<OutputType> {
            let promise = self.eventLoop.makePromise(of: OutputType.self)
            let logger = self.dynamodb.reporting.logger
            
            // if there are retries remaining
            if retriesRemaining > 0 {
                // determine the required interval
                let retryInterval = Int(self.dynamodb.retryConfiguration.getRetryInterval(retriesRemaining: retriesRemaining))
                
                let currentRetriesRemaining = retriesRemaining
                retriesRemaining -= 1
                
                let remainingKeysCount = self.input.requestItems.count
                
                logger.warning(
                    "Request retried for remaining items: \(remainingKeysCount). Remaining retries: \(currentRetriesRemaining). Retrying in \(retryInterval) ms.")
                let deadline = DispatchTime.now() + .milliseconds(retryInterval)
                retryQueue.asyncAfter(deadline: deadline) {
                    logger.trace("Reattempting request due to remaining retries: \(currentRetriesRemaining)")
                    
                    let nextFuture = self.batchGetItem()
                    
                    promise.completeWith(nextFuture)
                }
                
                // return the future that will be completed with the future retry.
                return promise.futureResult
            }
            
            let error = SmokeDynamoDBError.batchAPIExceededRetries(retryCount: self.dynamodb.retryConfiguration.numRetries)
            promise.fail(error)
            
            return promise.futureResult
        }
    }
    
    func getItems<ReturnedType: PolymorphicOperationReturnType & BatchCapableReturnType>(
        forKeys keys: [CompositePrimaryKey<ReturnedType.AttributesType>])
    -> EventLoopFuture<[CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]> {
        let chunkedList = keys.chunked(by: maximumKeysPerGetItemBatch)
        
        let futures = chunkedList.map { chunk -> EventLoopFuture<[CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]> in
            let input: BatchGetItemInput
            do {
                input = try getInputForBatchGetItem(forKeys: chunk)
            } catch {
                let promise = self.eventLoop.makePromise(of: [CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType].self)
                promise.fail(error)
                return promise.futureResult
            }
            
            let retriable = GetItemsRetriable<ReturnedType>(
                initialInput: input,
                dynamodb: self.dynamodb,
                eventLoopOverride: self.eventLoop)
            
            return retriable.batchGetItem()
        }
        
        // maps is of type [[CompositePrimaryKey<ReturnedType.AttributesType>: ReturnedType]]
        // with each map coming from each chunk of the original key list
        return EventLoopFuture.whenAllSucceed(futures, on: self.eventLoop) .map { maps in
            return maps.reduce([:]) { (partialMap, chunkMap) in
                // reduce the maps from the chunks into a single map
                return partialMap.merging(chunkMap) { (_, new) in new }
            }
        }
    }
}
