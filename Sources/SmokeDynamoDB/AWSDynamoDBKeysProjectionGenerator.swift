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
//  AWSDynamoDBKeysProjectionGenerator.swift
//  SmokeDynamoDB
//

import Foundation
import Logging
import DynamoDBClient
import DynamoDBModel
import SmokeAWSCore
import SmokeHTTPClient

public class AWSDynamoDBKeysProjectionGenerator {
    internal let dynamodbGenerator: AWSDynamoDBClientGenerator
    internal let targetTableName: String

    public init(accessKeyId: String, secretAccessKey: String,
                region: AWSRegion,
                endpointHostName: String, tableName: String,
                eventLoopProvider: HTTPClient.EventLoopProvider = .spawnNewThreads) {
        let staticCredentials = StaticCredentials(accessKeyId: accessKeyId,
                                                  secretAccessKey: secretAccessKey,
                                                  sessionToken: nil)

        self.dynamodbGenerator = AWSDynamoDBClientGenerator(credentialsProvider: staticCredentials,
                                                            awsRegion: region,
                                                            endpointHostName: endpointHostName,
                                                            eventLoopProvider: eventLoopProvider)
        self.targetTableName = tableName
    }

    public init(credentialsProvider: CredentialsProvider,
                region: AWSRegion,
                endpointHostName: String, tableName: String,
                eventLoopProvider: HTTPClient.EventLoopProvider = .spawnNewThreads) {
        self.dynamodbGenerator = AWSDynamoDBClientGenerator(credentialsProvider: credentialsProvider,
                                                            awsRegion: region,
                                                            endpointHostName: endpointHostName,
                                                            eventLoopProvider: eventLoopProvider)
        self.targetTableName = tableName
    }

    /**
     Gracefully shuts down the client behind this table. This function is idempotent and
     will handle being called multiple times.
     */
    public func close() {
        dynamodbGenerator.close()
    }

    /**
     Waits for the client behind this table to be closed. If close() is not called,
     this will block forever.
     */
    public func wait() {
        dynamodbGenerator.wait()
    }
    
    public func with<NewInvocationReportingType: HTTPClientCoreInvocationReporting>(
            reporting: NewInvocationReportingType) -> AWSDynamoDBKeysProjection<NewInvocationReportingType> {
        return AWSDynamoDBKeysProjection<NewInvocationReportingType>(
            dynamodb: self.dynamodbGenerator.with(reporting: reporting),
            targetTableName: self.targetTableName,
            logger: reporting.logger)
    }
}
