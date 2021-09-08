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
//  DynamoDBCompositePrimaryKeyTable+bulkUpdateSupport.swift.swift
//  SmokeDynamoDB
//

import Foundation
import SmokeHTTPClient
import Logging
import DynamoDBModel
import NIO

internal enum AttributeDifference: Equatable {
    case update(path: String, value: String)
    case remove(path: String)
    case listAppend(path: String, value: String)
    
    var path: String {
        switch self {
        case .update(path: let path, value: _):
            return path
        case .remove(path: let path):
            return path
        case .listAppend(path: let path, value: _):
            return path
        }
    }
}

extension DynamoDBCompositePrimaryKeyTable {
    
    func getAttributes<AttributesType, ItemType>(forItem item: TypedDatabaseItem<AttributesType, ItemType>) throws
        -> [String: DynamoDBModel.AttributeValue] {
            let attributeValue = try DynamoDBEncoder().encode(item)

            let attributes: [String: DynamoDBModel.AttributeValue]
            if let itemAttributes = attributeValue.M {
                attributes = itemAttributes
            } else {
                throw SmokeDynamoDBError.unexpectedResponse(reason: "Expected a map.")
            }

            return attributes
    }
    
    func getUpdateExpression<AttributesType, ItemType>(tableName: String,
                                                       newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                       existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws -> String {
        let attributeDifferences = try diffItems(newItem: newItem,
                                                 existingItem: existingItem)
        
        // according to https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.update.html
        let elements = attributeDifferences.map { attributeDifference -> String in
            switch attributeDifference {
            case .update(path: let path, value: let value):
                return "SET \(path)=\(value)"
            case .remove(path: let path):
                return "REMOVE \(path)"
            case .listAppend(path: let path, value: let value):
                return "SET \(path)=list_append(\(path),\(value)"
            }
        }
        
        let combinedElements = elements.joined(separator: " ")
        
        return "UPDATE \"\(tableName)\" \(combinedElements) "
            + "WHERE \(AttributesType.partitionKeyAttributeName)='\(newItem.compositePrimaryKey.partitionKey)' "
            + "AND \(AttributesType.sortKeyAttributeName)='\(newItem.compositePrimaryKey.sortKey)' "
            + "AND \(RowStatus.CodingKeys.rowVersion.rawValue)=\(existingItem.rowStatus.rowVersion)"
    }
    
    func getInsertExpression<AttributesType, ItemType>(tableName: String,
                                                       newItem: TypedDatabaseItem<AttributesType, ItemType>) throws -> String {
        let newAttributes = try getAttributes(forItem: newItem)
        let flattenedAttribute = try getFlattenedMapAttribute(attribute: newAttributes)
        
        // according to https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.insert.html
        return "INSERT INTO \"\(tableName)\" value \(flattenedAttribute)"
    }
    
    func getDeleteExpression<ItemType: DatabaseItem>(tableName: String,
                                                     existingItem: ItemType) throws -> String {
        return "DELETE FROM \"\(tableName)\" "
            + "WHERE \(ItemType.AttributesType.partitionKeyAttributeName)='\(existingItem.compositePrimaryKey.partitionKey)' "
            + "AND \(ItemType.AttributesType.sortKeyAttributeName)='\(existingItem.compositePrimaryKey.sortKey)' "
            + "AND \(RowStatus.CodingKeys.rowVersion.rawValue)=\(existingItem.rowStatus.rowVersion)"
    }
    
    func getDeleteExpression<AttributesType>(tableName: String,
                                             existingKey: CompositePrimaryKey<AttributesType>) throws -> String {
        return "DELETE FROM \"\(tableName)\" "
            + "WHERE \(AttributesType.partitionKeyAttributeName)='\(existingKey.partitionKey)' "
            + "AND \(AttributesType.sortKeyAttributeName)='\(existingKey.sortKey)'"
    }
    
    func diffItems<AttributesType, ItemType>(
                newItem: TypedDatabaseItem<AttributesType, ItemType>,
                existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws -> [AttributeDifference] {
        let newAttributes = try getAttributes(forItem: newItem)
        let existingAttributes = try getAttributes(forItem: existingItem)
        
        return try diffMapAttribute(path: nil, newAttribute: newAttributes, existingAttribute: existingAttributes)
    }
    
    private func diffAttribute(path: String,
                               newAttribute: DynamoDBModel.AttributeValue,
                               existingAttribute: DynamoDBModel.AttributeValue) throws -> [AttributeDifference] {
        if newAttribute.B != nil || existingAttribute.B != nil {
            throw SmokeDynamoDBError.unableToUpdateError(reason: "Unable to hande Binary types.")
        } else if let newTypedAttribute = newAttribute.BOOL, let existingTypedAttribute = existingAttribute.BOOL {
            if newTypedAttribute != existingTypedAttribute {
                return [.update(path: path, value: String(newTypedAttribute))]
            }
        } else if newAttribute.BS != nil || existingAttribute.BS != nil {
            throw SmokeDynamoDBError.unableToUpdateError(reason: "Unable to hande Binary Set types.")
        } else if let newTypedAttribute = newAttribute.L, let existingTypedAttribute = existingAttribute.L {
            return try diffListAttribute(path: path, newAttribute: newTypedAttribute, existingAttribute: existingTypedAttribute)
        } else if let newTypedAttribute = newAttribute.M, let existingTypedAttribute = existingAttribute.M {
            return try diffMapAttribute(path: path, newAttribute: newTypedAttribute, existingAttribute: existingTypedAttribute)
        } else if let newTypedAttribute = newAttribute.N, let existingTypedAttribute = existingAttribute.N {
            if newTypedAttribute != existingTypedAttribute {
                return [.update(path: path, value: String(newTypedAttribute))]
            }
        } else if newAttribute.NS != nil || existingAttribute.NS  != nil {
            throw SmokeDynamoDBError.unableToUpdateError(reason: "Unable to hande Number Set types.")
        } else if newAttribute.NULL != nil && existingAttribute.NULL != nil {
            // always equal
            return []
        } else if let newTypedAttribute = newAttribute.S, let existingTypedAttribute = existingAttribute.S {
            if newTypedAttribute != existingTypedAttribute {
                return [.update(path: path, value: "'\(newTypedAttribute)'")]
            }
        } else if newAttribute.SS != nil || existingAttribute.SS  != nil {
            throw SmokeDynamoDBError.unableToUpdateError(reason: "Unable to hande String Set types.")
        } else {
            // new value is a different type and could be replaced
            return try updateAttribute(newPath: path, attribute: newAttribute)
        }
        
        // no change
        return []
    }
    
    private func diffListAttribute(path: String,
                                   newAttribute: [DynamoDBModel.AttributeValue],
                                   existingAttribute: [DynamoDBModel.AttributeValue]) throws -> [AttributeDifference] {
        let maxIndex = max(newAttribute.count, existingAttribute.count)
        var haveAppendedAdditionalValues = false
        
        return try (0..<maxIndex).flatMap { index -> [AttributeDifference] in
            let newPath = "\(path)[\(index)]"
            
            // if both new and existing attributes are present
            if index < newAttribute.count && index < existingAttribute.count {
                return try diffAttribute(path: newPath, newAttribute: newAttribute[index], existingAttribute: existingAttribute[index])
            } else if index < existingAttribute.count {
                return [.remove(path: newPath)]
            } else if index < newAttribute.count {
                let additionalAttributes = Array(newAttribute[index...])
                let newValue = try getFlattenedListAttribute(attribute: additionalAttributes)
                
                if !haveAppendedAdditionalValues {
                    haveAppendedAdditionalValues = true
                    
                    return [.listAppend(path: path, value: newValue)]
                } else {
                    // values have already been appended to the list
                    return []
                }
            }
            
            return []
        }
    }
    
    private func diffMapAttribute(path: String?,
                                  newAttribute: [String: DynamoDBModel.AttributeValue],
                                  existingAttribute: [String: DynamoDBModel.AttributeValue]) throws -> [AttributeDifference] {
        var combinedMap: [String: (new: DynamoDBModel.AttributeValue?, existing: DynamoDBModel.AttributeValue?)] = [:]
        
        newAttribute.forEach { (key, attribute) in
            var existingEntry = combinedMap[key] ?? (nil, nil)
            existingEntry.new = attribute
            combinedMap[key] = existingEntry
        }
        
        existingAttribute.forEach { (key, attribute) in
            var existingEntry = combinedMap[key] ?? (nil, nil)
            existingEntry.existing = attribute
            combinedMap[key] = existingEntry
        }
        
        return try combinedMap.flatMap { (key, attribute) -> [AttributeDifference] in
            let newPath = combinePath(basePath: path, newComponent: key)
            
            // if both new and existing attributes are present
            if let new = attribute.new, let existing = attribute.existing {
                return try diffAttribute(path: newPath, newAttribute: new, existingAttribute: existing)
            } else if attribute.existing != nil {
                return [.remove(path: newPath)]
            } else if let new = attribute.new {
                return try updateAttribute(newPath: newPath, attribute: new)
            } else {
                return []
            }
        }
    }
    
    private func combinePath(basePath: String?, newComponent: String) -> String {
        if let basePath = basePath {
            return "\(basePath).\(newComponent)"
        } else {
            return newComponent
        }
    }
    
    private func updateAttribute(newPath: String, attribute: DynamoDBModel.AttributeValue) throws -> [AttributeDifference] {
        if let newValue = try getFlattenedAttribute(attribute: attribute) {
            return [.update(path: newPath, value: newValue)]
        } else {
            return [.remove(path: newPath)]
        }
    }
    
    func getFlattenedAttribute(attribute: DynamoDBModel.AttributeValue) throws -> String? {
        if attribute.B != nil {
            throw SmokeDynamoDBError.unableToUpdateError(reason: "Unable to hande Binary types.")
        } else if let typedAttribute = attribute.BOOL {
            return String(typedAttribute)
        } else if attribute.BS != nil {
            throw SmokeDynamoDBError.unableToUpdateError(reason: "Unable to hande Binary Set types.")
        } else if let typedAttribute = attribute.L {
            return try getFlattenedListAttribute(attribute: typedAttribute)
        } else if let typedAttribute = attribute.M {
            return try getFlattenedMapAttribute(attribute: typedAttribute)
        } else if let typedAttribute = attribute.N {
            return String(typedAttribute)
        } else if attribute.NS != nil {
            throw SmokeDynamoDBError.unableToUpdateError(reason: "Unable to hande Number Set types.")
        } else if attribute.NULL != nil {
            return nil
        } else if let typedAttribute = attribute.S {
            return "'\(typedAttribute)'"
        } else if attribute.SS != nil {
            throw SmokeDynamoDBError.unableToUpdateError(reason: "Unable to hande String Set types.")
        }
        
        return nil
    }
    
    private func getFlattenedListAttribute(attribute: [DynamoDBModel.AttributeValue]) throws -> String {
        let elements: [String] = try attribute.compactMap { nestedAttribute in
            return try getFlattenedAttribute(attribute: nestedAttribute)
        }
        
        let joinedElements = elements.joined(separator: ", ")
        return "[\(joinedElements)]"
    }
    
    private func getFlattenedMapAttribute(attribute: [String: DynamoDBModel.AttributeValue]) throws -> String {
        let elements: [String] = try attribute.compactMap { (key, nestedAttribute) in
            guard let flattenedNestedAttribute = try getFlattenedAttribute(attribute: nestedAttribute) else {
                return nil
            }
            
            return "'\(key)': \(flattenedNestedAttribute)"
        }
        
        let joinedElements = elements.joined(separator: ", ")
        return "{\(joinedElements)}"
    }
}
