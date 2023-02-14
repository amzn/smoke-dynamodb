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
//  PolymorphicWriteEntry.swift
//  SmokeDynamoDB
//

import DynamoDBModel

// Conforming types are provided by the Table implementation to convert a `WriteEntry` into
//something the table can use to perform the write.
public protocol PolymorphicWriteEntryTransform {
    associatedtype TableType

    init<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: WriteEntry<AttributesType, ItemType>, table: TableType) throws
}

// Conforming types are provided by the Table implementation to convert a `WriteEntry` into
//something the table can use to achieve the constraint.
public protocol PolymorphicTransactionConstraintTransform {
    associatedtype TableType
    
    init<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: TransactionConstraintEntry<AttributesType, ItemType>, table: TableType) throws
}

// Conforming types are provided by the application to express the different possible write entries
// and how they can be converted to the table-provided transform type.
public protocol PolymorphicWriteEntry {

    func handle<Context: PolymorphicWriteEntryContext>(context: Context) throws -> Context.WriteEntryTransformType
    
    var compositePrimaryKey: StandardCompositePrimaryKey? { get }
}

public extension PolymorphicWriteEntry {
    var compositePrimaryKey: StandardCompositePrimaryKey? {
        return nil
    }
}

public typealias StandardTransactionConstraintEntry<ItemType: Codable> = TransactionConstraintEntry<StandardPrimaryKeyAttributes, ItemType>

public enum TransactionConstraintEntry<AttributesType: PrimaryKeyAttributes, ItemType: Codable> {
    case required(existing: TypedDatabaseItem<AttributesType, ItemType>)
}

// Conforming types are provided by the application to express the different possible constraint entries
// and how they can be converted to the table-provided transform type.
public protocol PolymorphicTransactionConstraintEntry {

    func handle<Context: PolymorphicWriteEntryContext>(context: Context) throws -> Context.WriteTransactionConstraintType
    
    var compositePrimaryKey: StandardCompositePrimaryKey? { get }
}

public extension PolymorphicTransactionConstraintEntry {
    var compositePrimaryKey: StandardCompositePrimaryKey? {
        return nil
    }
}

public struct EmptyPolymorphicTransactionConstraintEntry: PolymorphicTransactionConstraintEntry {
    public func handle<Context: PolymorphicWriteEntryContext>(context: Context) throws -> Context.WriteTransactionConstraintType {
        fatalError("There are no items to transform")
    }
}

// Helper Context type that enables transforming Write Entries into the to the table-provided transform type.
public protocol PolymorphicWriteEntryContext {
    associatedtype WriteEntryTransformType: PolymorphicWriteEntryTransform
    associatedtype WriteTransactionConstraintType: PolymorphicTransactionConstraintTransform
    
    func transform<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: WriteEntry<AttributesType, ItemType>) throws
    -> WriteEntryTransformType
    
    func transform<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: TransactionConstraintEntry<AttributesType, ItemType>) throws
    -> WriteTransactionConstraintType
}

public struct StandardPolymorphicWriteEntryContext<WriteEntryTransformType: PolymorphicWriteEntryTransform,
                                                   WriteTransactionConstraintType: PolymorphicTransactionConstraintTransform>: PolymorphicWriteEntryContext
where WriteEntryTransformType.TableType == WriteTransactionConstraintType.TableType {
    public typealias TableType = WriteEntryTransformType.TableType
    
    private let table: TableType
    
    public init(table: TableType) {
        self.table = table
    }
    
    public func transform<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: WriteEntry<AttributesType, ItemType>) throws
    -> WriteEntryTransformType {
        return try .init(entry, table: self.table)
    }
    
    public func transform<AttributesType: PrimaryKeyAttributes, ItemType: Codable>(_ entry: TransactionConstraintEntry<AttributesType, ItemType>) throws
    -> WriteTransactionConstraintType {
        return try .init(entry, table: self.table)
    }
}
