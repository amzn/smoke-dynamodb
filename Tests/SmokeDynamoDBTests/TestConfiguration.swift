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
//  TestConfiguration.swift
//  SmokeDynamoDBTests
//

import Foundation
@testable import SmokeDynamoDB

struct TestTypeA: Codable, Equatable {
    let firstly: String
    let secondly: String
}

struct TestTypeB: Codable, Equatable, CustomRowTypeIdentifier {
    static var rowTypeIdentifier: String? = "TypeBCustom"
    
    let thirdly: String
    let fourthly: String
}

struct TestTypeC: Codable {
    let theString: String?
    let theNumber: Int?
    let theStruct: TestTypeA?
    let theList: [String]?
    
    init(theString: String?, theNumber: Int?, theStruct: TestTypeA?, theList: [String]?) {
        self.theString = theString
        self.theNumber = theNumber
        self.theStruct = theStruct
        self.theList = theList
    }
}

enum TestQueryableTypes: PolymorphicOperationReturnType {
    typealias AttributesType = StandardPrimaryKeyAttributes
    
    static var types: [(Codable.Type, PolymorphicOperationReturnOption<StandardPrimaryKeyAttributes, Self>)] = [
        (TestTypeA.self, .init( {.testTypeA($0)} )),
        (TestTypeB.self, .init( {.testTypeB($0)} )),
        ]
    
    case testTypeA(StandardTypedDatabaseItem<TestTypeA>)
    case testTypeB(StandardTypedDatabaseItem<TestTypeB>)
}

extension TestQueryableTypes: BatchCapableReturnType {
    func getItemKey() -> CompositePrimaryKey<StandardPrimaryKeyAttributes> {
        switch self {
        case .testTypeA(let databaseItem):
            return databaseItem.compositePrimaryKey
        case .testTypeB(let databaseItem):
            return databaseItem.compositePrimaryKey
        }
    }
}

typealias TestTypeAWriteEntry = StandardWriteEntry<TestTypeA>
typealias TestTypeBWriteEntry = StandardWriteEntry<TestTypeB>
typealias TestTypeAStandardTransactionConstraintEntry = StandardTransactionConstraintEntry<TestTypeA>
typealias TestTypeBStandardTransactionConstraintEntry = StandardTransactionConstraintEntry<TestTypeB>

enum TestPolymorphicWriteEntry: PolymorphicWriteEntry {
    case testTypeA(TestTypeAWriteEntry)
    case testTypeB(TestTypeBWriteEntry)

    func handle<Context: PolymorphicWriteEntryContext>(context: Context) throws -> Context.WriteEntryTransformType {
        switch self {
        case .testTypeA(let writeEntry):
            return try context.transform(writeEntry)
        case .testTypeB(let writeEntry):
            return try context.transform(writeEntry)
        }
    }
}

enum TestPolymorphicTransactionConstraintEntry: PolymorphicTransactionConstraintEntry {
    case testTypeA(TestTypeAStandardTransactionConstraintEntry)
    case testTypeB(TestTypeBStandardTransactionConstraintEntry)

    func handle<Context: PolymorphicWriteEntryContext>(context: Context) throws -> Context.WriteTransactionConstraintType {
        switch self {
        case .testTypeA(let writeEntry):
            return try context.transform(writeEntry)
        case .testTypeB(let writeEntry):
            return try context.transform(writeEntry)
        }
    }
}
