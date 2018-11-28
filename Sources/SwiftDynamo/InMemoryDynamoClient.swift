//
//  InMemoryDynamoClient.swift
//  SwiftDynamo
//

import Foundation

public protocol PolymorphicDatabaseItemConvertable {
    var createDate: Date { get }
    var rowStatus: RowStatus { get }
    
    func convertToPolymorphicItem<RowIdentity: DynamoRowIdentity, PossibleTypes: PossibleItemTypes>() throws
        -> PolymorphicDatabaseItem<RowIdentity, PossibleTypes>
}

extension TypedDatabaseItem: PolymorphicDatabaseItemConvertable {
    public func convertToPolymorphicItem<RequestedRowIdentity, PossibleTypes>() throws
        -> PolymorphicDatabaseItem<RequestedRowIdentity, PossibleTypes> {
        guard let convertedCompositePrimaryKey = compositePrimaryKey as? CompositePrimaryKey<RequestedRowIdentity> else {
            let description = "Expected to use RowIdentity \(RequestedRowIdentity.self)."
            let context = DecodingError.Context(codingPath: [], debugDescription: description)
            throw DecodingError.typeMismatch(RequestedRowIdentity.self, context)
        }
        
        return PolymorphicDatabaseItem<RequestedRowIdentity, PossibleTypes>(compositePrimaryKey: convertedCompositePrimaryKey,
                                                      createDate: createDate,
                                                      rowStatus: rowStatus,
                                                      rowValue: rowValue)
    }
}

public class InMemoryDynamoClient: DynamoClient {
    
    public var store: [String: [String: PolymorphicDatabaseItemConvertable]] = [:]
    
    public init() {
        
    }
    
    public func insertItem<RowIdentity, ItemType>(_ item: TypedDatabaseItem<RowIdentity, ItemType>) throws {
        let partition = store[item.compositePrimaryKey.partitionKey]
        
        // if there is already a partition
        var updatedPartition: [String: PolymorphicDatabaseItemConvertable]
        if let partition = partition {
            updatedPartition = partition
            
            // if the row already exists
            if partition[item.compositePrimaryKey.sortKey] != nil {
                throw SwiftDynamoError.conditionalCheckFailed(paritionKey: item.compositePrimaryKey.partitionKey,
                                                              sortKey: item.compositePrimaryKey.sortKey,
                                                              message: "Row already exists.")
            }
            
            updatedPartition[item.compositePrimaryKey.sortKey] = item
        } else {
            updatedPartition = [item.compositePrimaryKey.sortKey: item]
        }
        
        store[item.compositePrimaryKey.partitionKey] = updatedPartition
    }
    
    public func clobberItem<RowIdentity, ItemType>(_ item: TypedDatabaseItem<RowIdentity, ItemType>) throws {
        let partition = store[item.compositePrimaryKey.partitionKey]
        
        // if there is already a partition
        var updatedPartition: [String: PolymorphicDatabaseItemConvertable]
        if let partition = partition {
            updatedPartition = partition
            
            updatedPartition[item.compositePrimaryKey.sortKey] = item
        } else {
            updatedPartition = [item.compositePrimaryKey.sortKey: item]
        }
        
        store[item.compositePrimaryKey.partitionKey] = updatedPartition
    }
    
    public func updateItem<RowIdentity, ItemType>(newItem: TypedDatabaseItem<RowIdentity, ItemType>, existingItem: TypedDatabaseItem<RowIdentity, ItemType>) throws {
        let partition = store[newItem.compositePrimaryKey.partitionKey]
        
        // if there is already a partition
        var updatedPartition: [String: PolymorphicDatabaseItemConvertable]
        if let partition = partition {
            updatedPartition = partition
            
            // if the row already exists
            if let acutallyExistingItem = partition[newItem.compositePrimaryKey.sortKey] {
                if existingItem.rowStatus.rowVersion != acutallyExistingItem.rowStatus.rowVersion {
                    throw SwiftDynamoError.conditionalCheckFailed(paritionKey: newItem.compositePrimaryKey.partitionKey,
                                                                  sortKey: newItem.compositePrimaryKey.sortKey,
                                                                  message: "Trying to overwrite incorrect version.")
                }
            } else {
                throw SwiftDynamoError.conditionalCheckFailed(paritionKey: newItem.compositePrimaryKey.partitionKey,
                                                              sortKey: newItem.compositePrimaryKey.sortKey,
                                                              message: "Existing item does not exist.")
            }
            
            updatedPartition[newItem.compositePrimaryKey.sortKey] = newItem
        } else {
            throw SwiftDynamoError.conditionalCheckFailed(paritionKey: newItem.compositePrimaryKey.partitionKey,
                                                          sortKey: newItem.compositePrimaryKey.sortKey,
                                                          message: "Existing item does not exist.")
        }
        
        store[newItem.compositePrimaryKey.partitionKey] = updatedPartition
    }
    
    public func getItem<RowIdentity, ItemType>(forKey key: CompositePrimaryKey<RowIdentity>) throws -> TypedDatabaseItem<RowIdentity, ItemType>? {
        if let partition = store[key.partitionKey] {
            
            guard let value = partition[key.sortKey] else {
                return nil
            }
            
            guard let item = value as? TypedDatabaseItem<RowIdentity, ItemType> else {
                let foundType = type(of: value)
                let description = "Expected to decode \(TypedDatabaseItem<RowIdentity, ItemType>.self). Instead found \(foundType)."
                let context = DecodingError.Context(codingPath: [], debugDescription: description)
                throw DecodingError.typeMismatch(TypedDatabaseItem<RowIdentity, ItemType>.self, context)
            }
            
            return item
        }
        
        return nil
    }
    
    public func deleteItem<RowIdentity>(forKey key: CompositePrimaryKey<RowIdentity>) throws {
        store[key.partitionKey]?[key.sortKey] = nil
    }
    
    // swiftlint:disable cyclomatic_complexity
    public func query<RowIdentity, PossibleTypes>(forPartitionKey partitionKey: String,
                                                  sortKeyCondition: AttributeCondition?) throws
        -> [PolymorphicDatabaseItem<RowIdentity, PossibleTypes>] {
        var items: [PolymorphicDatabaseItem<RowIdentity, PossibleTypes>] = []
        
        if let partition = store[partitionKey] {
            sortKeyIteration: for (sortKey, value) in partition {
                
                if let currentSortKeyCondition = sortKeyCondition {
                    switch currentSortKeyCondition {
                    case .equals(let value):
                        if !(value == sortKey) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .lessThan(let value):
                        if !(sortKey < value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .lessThanOrEqual(let value):
                        if !(sortKey <= value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .greaterThan(let value):
                        if !(sortKey > value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .greaterThanOrEqual(let value):
                        if !(sortKey >= value) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .between(let value1, let value2):
                        if !(sortKey > value1 && sortKey < value2) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    case .beginsWith(let value):
                        if !(sortKey.hasPrefix(value)) {
                            // don't include this in the results
                            continue sortKeyIteration
                        }
                    }
                }
                
                items.append(try value.convertToPolymorphicItem())
            }
        }
        
        return items
    }
    
    public func query<RowIdentity, PossibleTypes>(forPartitionKey partitionKey: String,
                                                  sortKeyCondition: AttributeCondition?,
                                                  limit: Int,
                                                  exclusiveStartKey: String?) throws
        -> ([PolymorphicDatabaseItem<RowIdentity, PossibleTypes>], String?)
        where RowIdentity : DynamoRowIdentity, PossibleTypes : PossibleItemTypes {
            // get all the results
            let items: [PolymorphicDatabaseItem<RowIdentity, PossibleTypes>] = try query(
                forPartitionKey: partitionKey,
                sortKeyCondition: sortKeyCondition)
            
            let startIndex: Int
            // if there is an exclusiveStartKey
            if let exclusiveStartKey = exclusiveStartKey {
                guard let storedStartIndex = Int(exclusiveStartKey) else {
                    fatalError("Unexpectedly encoded exclusiveStartKey '\(exclusiveStartKey)'")
                }
                
                startIndex = storedStartIndex
            } else {
                startIndex = 0
            }
            
            let endIndex: Int
            let lastEvaluatedKey: String?
            if startIndex + limit < items.count {
                endIndex = startIndex + limit
                lastEvaluatedKey = String(endIndex)
            } else {
                endIndex = items.count
                lastEvaluatedKey = nil
            }
            
            return (Array(items[startIndex..<endIndex]), lastEvaluatedKey)
    }
    
    private func getItemAsPolymorphicDatabaseItemConvertable<ConvertableType>(value: Any) throws
        -> ConvertableType where ConvertableType: PolymorphicDatabaseItemConvertable {
        guard let polymorphicDatabaseItemConvertable = value as? ConvertableType else {
            let description = "Expected to decode \(ConvertableType.self). Instead found \(value.self)."
            let context = DecodingError.Context(codingPath: [], debugDescription: description)
            throw DecodingError.typeMismatch(ConvertableType.self, context)
        }
        
        return polymorphicDatabaseItemConvertable
    }
}
