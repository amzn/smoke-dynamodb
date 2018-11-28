//
//  TestInput.swift
//  PlaybackObjectsActivitiesTests
//

import Foundation
@testable import SwiftDynamo

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

struct TypeB: Codable, DynamoDbCustomRowIdentity {
    static var identity: String? = "TypeBCustom"
    
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
