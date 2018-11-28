//
//  String+DynamoKey.swift
//  SwiftDynamo
//

import Foundation

/// Extension for Arrays of Strings
public extension Array where Element == String {
    // Transforms the Array into a Dynamo key - putting dots between each element.
    public var dynamoKey: String {
        // return all elements joined with dots
        return self.joined(separator: ".")
    }

    // Transforms an Array into a Dynamo key prefix - a Dynamo key with a dot on the end.
    public var dynamoKeyPrefix: String {
        
        let dynamoKey = self.dynamoKey
        if dynamoKey.count == 0 {
            return ""
        }
        return dynamoKey + "."
    }
    
    /**
     Returns the provided string with the dynamo key (with the trailing
     dot) corresponding to this array dropped as a prefix. Returns nil
     if the provided string doesn't have the prefix.
     */
    func dropAsDynamoKeyPrefix(from string: String) -> String? {
        let prefix = self.dynamoKeyPrefix

        guard string.hasPrefix(prefix) else {
          return nil
        }

        return String(string.dropFirst(prefix.count))
    }
    
    /**
     Transforms the Array into a Dynamo key - putting dots between each element - with a prefix
     element specifying the version.
 
     - Parameters:
        - versionNumber: The version number to prefix.
        - minimumFieldWidth: the minimum field width of the version field. Leading
        zeros will be padded if required.
     */
    func dynamoKeyWithPrefixedVersion(_ versionNumber: Int, minimumFieldWidth: Int) -> String {
        let versionAsString = String(format: "%0\(minimumFieldWidth)d", versionNumber)
        return (["v\(versionAsString)"] + self).dynamoKey
    }
}
