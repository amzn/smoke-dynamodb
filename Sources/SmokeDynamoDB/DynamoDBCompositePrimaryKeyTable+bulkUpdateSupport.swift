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
import Logging
import AWSDynamoDB

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
        -> [String: DynamoDBClientTypes.AttributeValue] {
            let attributeValue = try DynamoDBEncoder().encode(item)

            let attributes: [String: DynamoDBClientTypes.AttributeValue]
            if case .m(let itemAttributes) = attributeValue {
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
                return "SET \(path)=list_append(\(path),\(value))"
            }
        }
        
        let combinedElements = elements.joined(separator: " ")
        
        return "UPDATE \"\(tableName)\" \(combinedElements) "
            + "WHERE \(AttributesType.partitionKeyAttributeName)='\(sanitizeString(newItem.compositePrimaryKey.partitionKey))' "
            + "AND \(AttributesType.sortKeyAttributeName)='\(sanitizeString(newItem.compositePrimaryKey.sortKey))' "
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
            + "WHERE \(ItemType.AttributesType.partitionKeyAttributeName)='\(sanitizeString(existingItem.compositePrimaryKey.partitionKey))' "
            + "AND \(ItemType.AttributesType.sortKeyAttributeName)='\(sanitizeString(existingItem.compositePrimaryKey.sortKey))' "
            + "AND \(RowStatus.CodingKeys.rowVersion.rawValue)=\(existingItem.rowStatus.rowVersion)"
    }
    
    func getDeleteExpression<AttributesType>(tableName: String,
                                             existingKey: CompositePrimaryKey<AttributesType>) throws -> String {
        return "DELETE FROM \"\(tableName)\" "
            + "WHERE \(AttributesType.partitionKeyAttributeName)='\(sanitizeString(existingKey.partitionKey))' "
            + "AND \(AttributesType.sortKeyAttributeName)='\(sanitizeString(existingKey.sortKey))'"
    }
    
    func getExistsExpression<AttributesType, ItemType>(tableName: String,
                                                       existingItem: TypedDatabaseItem<AttributesType, ItemType>) -> String {
        // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-functions.exists.html
        return "EXISTS("
            + "SELECT * FROM \"\(tableName)\" "
            + "WHERE \(AttributesType.partitionKeyAttributeName)='\(sanitizeString(existingItem.compositePrimaryKey.partitionKey))' "
            + "AND \(AttributesType.sortKeyAttributeName)='\(sanitizeString(existingItem.compositePrimaryKey.sortKey))' "
            + "AND \(RowStatus.CodingKeys.rowVersion.rawValue)=\(existingItem.rowStatus.rowVersion)"
            + ")"
    }
    
    /*
     Function to return the differences between two items. This is used to then create an UPDATE
     query that just specifies the values that are changing.
     */
    func diffItems<AttributesType, ItemType>(
                newItem: TypedDatabaseItem<AttributesType, ItemType>,
                existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws -> [AttributeDifference] {
        let newAttributes = try getAttributes(forItem: newItem)
        let existingAttributes = try getAttributes(forItem: existingItem)
        
        return try diffMapAttribute(path: nil, newAttribute: newAttributes, existingAttribute: existingAttributes)
    }
    
    private func diffAttribute(path: String,
                               newAttribute: DynamoDBClientTypes.AttributeValue,
                               existingAttribute: DynamoDBClientTypes.AttributeValue) throws -> [AttributeDifference] {
        switch (newAttribute, existingAttribute) {
        case (.b, .b):
            throw SmokeDynamoDBError.unableToUpdateError(reason: "Unable to handle Binary types.")
        case (.bool(let newTypedAttribute), .bool(let existingTypedAttribute)):
            if newTypedAttribute != existingTypedAttribute {
                return [.update(path: path, value: String(newTypedAttribute))]
            }
        case (.bs, .bs):
            throw SmokeDynamoDBError.unableToUpdateError(reason: "Unable to handle Binary Set types.")
        case (.l(let newTypedAttribute), .l(let existingTypedAttribute)):
            return try diffListAttribute(path: path, newAttribute: newTypedAttribute, existingAttribute: existingTypedAttribute)
        case (.m(let newTypedAttribute), .m(let existingTypedAttribute)):
            return try diffMapAttribute(path: path, newAttribute: newTypedAttribute, existingAttribute: existingTypedAttribute)
        case (.n(let newTypedAttribute), .n(let existingTypedAttribute)):
            if newTypedAttribute != existingTypedAttribute {
                return [.update(path: path, value: String(newTypedAttribute))]
            }
        case (.ns, .ns):
            throw SmokeDynamoDBError.unableToUpdateError(reason: "Unable to handle Number Set types.")
        case (.null, .null):
            // always equal
            return []
        case (.s(let newTypedAttribute), .s(let existingTypedAttribute)):
            if newTypedAttribute != existingTypedAttribute {
                return [.update(path: path, value: "'\(sanitizeString(newTypedAttribute))'")]
            }
        case (.ss, .ss):
            throw SmokeDynamoDBError.unableToUpdateError(reason: "Unable to handle String Set types.")
        default:
            // new value is a different type and could be replaced
            return try updateAttribute(newPath: path, attribute: newAttribute)
        }
        
        // no change
        return []
    }
    
    private func diffListAttribute(path: String,
                                   newAttribute: [DynamoDBClientTypes.AttributeValue],
                                   existingAttribute: [DynamoDBClientTypes.AttributeValue]) throws -> [AttributeDifference] {
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
                                  newAttribute: [String: DynamoDBClientTypes.AttributeValue],
                                  existingAttribute: [String: DynamoDBClientTypes.AttributeValue]) throws -> [AttributeDifference] {
        var combinedMap: [String: (new: DynamoDBClientTypes.AttributeValue?, existing: DynamoDBClientTypes.AttributeValue?)] = [:]
        
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
            return "\(basePath).\"\(newComponent)\""
        } else {
            return "\"\(newComponent)\""
        }
    }
    
    private func updateAttribute(newPath: String, attribute: DynamoDBClientTypes.AttributeValue) throws -> [AttributeDifference] {
        if let newValue = try getFlattenedAttribute(attribute: attribute) {
            return [.update(path: newPath, value: newValue)]
        } else {
            return [.remove(path: newPath)]
        }
    }
    
    func getFlattenedAttribute(attribute: DynamoDBClientTypes.AttributeValue) throws -> String? {
            switch attribute {
            case .b:
                throw SmokeDynamoDBError.unableToUpdateError(reason: "Unable to handle Binary types.")
            case .bool(let typedAttribute):
                return String(typedAttribute)
            case .bs:
                throw SmokeDynamoDBError.unableToUpdateError(reason: "Unable to handle Binary Set types.")
            case .l(let typedAttribute):
                return try getFlattenedListAttribute(attribute: typedAttribute)
            case .m(let typedAttribute):
                return try getFlattenedMapAttribute(attribute: typedAttribute)
            case .n(let typedAttribute):
                return String(typedAttribute)
            case .ns:
                throw SmokeDynamoDBError.unableToUpdateError(reason: "Unable to handle Number Set types.")
            case .null:
                return nil
            case .s(let typedAttribute):
                return "'\(sanitizeString(typedAttribute))'"
            case .ss:
                throw SmokeDynamoDBError.unableToUpdateError(reason: "Unable to handle String Set types.")
            case .sdkUnknown(let payload):
                throw SmokeDynamoDBError.unableToUpdateError(reason: "Unable to handle unknown type: '\(payload)'.")
            }
        }
    
    private func getFlattenedListAttribute(attribute: [DynamoDBClientTypes.AttributeValue]) throws -> String {
        let elements: [String] = try attribute.compactMap { nestedAttribute in
            return try getFlattenedAttribute(attribute: nestedAttribute)
        }
        
        let joinedElements = elements.joined(separator: ", ")
        return "[\(joinedElements)]"
    }
    
    private func getFlattenedMapAttribute(attribute: [String: DynamoDBClientTypes.AttributeValue]) throws -> String {
        let elements: [String] = try attribute.compactMap { (key, nestedAttribute) in
            guard let flattenedNestedAttribute = try getFlattenedAttribute(attribute: nestedAttribute) else {
                return nil
            }
            
            return "'\(key)': \(flattenedNestedAttribute)"
        }
        
        let joinedElements = elements.joined(separator: ", ")
        return "{\(joinedElements)}"
    }

    /// In PartiQL single quotes indicate start and end of a string attribute.
    /// If, however, the string itself contains a single quote then the database
    /// does not know where the string should end. Therefore, need to escape
    /// single quote by doubling it. E.g. 'foo'bar' becomes 'foo''bar'.
    private func sanitizeString(_ string: String) -> String {
        if self.escapeSingleQuoteInPartiQL {
            return string.replacingOccurrences(of: "'", with: "''")
        } else {
            return string
        }
    }
}
