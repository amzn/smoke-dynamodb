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
//  TestInput.swift
//  SmokeDynamoDBTests
//

import Foundation
@testable import SmokeDynamoDB

struct AllCodableTypes : PossibleItemTypes {
    public static var types: [Codable.Type] = [TypeA.self, TypeB.self]
}

struct SomeCodableTypes : PossibleItemTypes {
    public static var types: [Codable.Type] = [TypeA.self]
}

struct GSI1PKIndexIdentity : IndexIdentity {
    static var codingKey =  createRowWithIndexCodingKey(stringValue: "GSI-1-PK")
    static var identity = "GSI1PK"
}

struct AllCodableTypesWithIndex : PossibleItemTypes {
    public static var types: [Codable.Type] = [RowWithIndex<TypeA, GSI1PKIndexIdentity>.self, TypeB.self]
}

struct TypeA: Codable {
    let firstly: String
    let secondly: String
    
    init(firstly: String, secondly: String) {
        self.firstly = firstly
        self.secondly = secondly
    }
}

struct TypeB: Codable, CustomRowTypeIdentifier {
    static var rowTypeIdentifier: String? = "TypeBCustom"
    
    let thirdly: String
    let fourthly: String
}

let serializedTypeADatabaseItem = """
    {
        "M" : {
            "PK" : { "S": "partitionKey" },
            "SK" : { "S": "sortKey" },
            "CreateDate" : { "S" : "2018-01-06T23:36:20.355Z" },
            "RowVersion" : { "N": "5" },
            "LastUpdatedDate" : { "S" : "2018-01-06T23:36:20.355Z" },
            "RowType": { "S": "TypeA" },
            "firstly" : { "S": "aaa" },
            "secondly": { "S": "bbb" }
        }
    }
    """

let serializedPolymorphicDatabaseItemList = """
    [
        {
            "M" : {
                "PK" : { "S": "partitionKey1" },
                "SK" : { "S": "sortKey1" },
                "CreateDate" : { "S" : "2018-01-06T23:36:20.355Z" },
                "RowVersion" : { "N": "5" },
                "LastUpdatedDate" : { "S" : "2018-01-06T23:36:20.355Z" },
                "RowType": { "S": "TypeA" },
                "firstly" : { "S": "aaa" },
                "secondly": { "S": "bbb" }
            }
        },
        {
            "M" : {
                "PK" : { "S": "partitionKey2" },
                "SK" : { "S": "sortKey2" },
                "CreateDate" : { "S" : "2018-01-06T23:36:20.355Z" },
                "RowVersion" : { "N": "12" },
                "LastUpdatedDate" : { "S" : "2018-01-06T23:36:20.355Z" },
                "RowType": { "S": "TypeBCustom" },
                "thirdly" : { "S": "ccc" },
                "fourthly": { "S": "ddd" }
            }
        }
    ]
    """

let serializedPolymorphicDatabaseItemListWithIndex = """
    [
        {
            "M" : {
                "PK" : { "S": "partitionKey1" },
                "SK" : { "S": "sortKey1" },
                "GSI-1-PK" : { "S": "gsi-index" },
                "CreateDate" : { "S" : "2018-01-06T23:36:20.355Z" },
                "RowVersion" : { "N": "5" },
                "LastUpdatedDate" : { "S" : "2018-01-06T23:36:20.355Z" },
                "RowType": { "S": "TypeAWithGSI1PKIndex" },
                "firstly" : { "S": "aaa" },
                "secondly": { "S": "bbb" }
            }
        },
        {
            "M" : {
                "PK" : { "S": "partitionKey2" },
                "SK" : { "S": "sortKey2" },
                "CreateDate" : { "S" : "2018-01-06T23:36:20.355Z" },
                "RowVersion" : { "N": "12" },
                "LastUpdatedDate" : { "S" : "2018-01-06T23:36:20.355Z" },
                "RowType": { "S": "TypeBCustom" },
                "thirdly" : { "S": "ccc" },
                "fourthly": { "S": "ddd" }
            }
        }
    ]
    """
