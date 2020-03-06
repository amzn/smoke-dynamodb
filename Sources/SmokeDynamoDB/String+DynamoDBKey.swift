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
//  String+DynamoDBKey.swift
//  SmokeDynamoDB
//

import Foundation

/// Extension for Arrays of Strings
public extension Array where Element == String {
    // Transforms the Array into a Dynamo key - putting dots between each element.
    var dynamodbKey: String {
        // return all elements joined with dots
        return self.joined(separator: ".")
    }

    // Transforms an Array into a DynamoDB key prefix - a DynamoDB key with a dot on the end.
    var dynamodbKeyPrefix: String {
        
        let dynamodbKey = self.dynamodbKey
        if dynamodbKey.count == 0 {
            return ""
        }
        return dynamodbKey + "."
    }
    
    /**
     Returns the provided string with the DynamoDB key (with the trailing
     dot) corresponding to this array dropped as a prefix. Returns nil
     if the provided string doesn't have the prefix.
     */
    func dropAsDynamoDBKeyPrefix(from string: String) -> String? {
        let prefix = self.dynamodbKeyPrefix

        guard string.hasPrefix(prefix) else {
          return nil
        }

        return String(string.dropFirst(prefix.count))
    }
    
    /**
     Transforms the Array into a DynamoDB key - putting dots between each element - with a prefix
     element specifying the version.
 
     - Parameters:
        - versionNumber: The version number to prefix.
        - minimumFieldWidth: the minimum field width of the version field. Leading
        zeros will be padded if required.
     */
    func dynamodbKeyWithPrefixedVersion(_ versionNumber: Int, minimumFieldWidth: Int) -> String {
        let versionAsString = String(format: "%0\(minimumFieldWidth)d", versionNumber)
        return (["v\(versionAsString)"] + self).dynamodbKey
    }
}
