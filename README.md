<p align="center">
<a href="https://travis-ci.com/amzn/smoke-dynamodb">
<img src="https://travis-ci.com/amzn/smoke-dynamodb.svg?branch=master" alt="Build - Master Branch">
</a>
<img src="https://img.shields.io/badge/os-linux-green.svg?style=flat" alt="Linux">
<a href="http://swift.org">
<img src="https://img.shields.io/badge/swift-5.0-orange.svg?style=flat" alt="Swift 5.0 Compatible">
</a>
<a href="http://swift.org">
<img src="https://img.shields.io/badge/swift-5.1-orange.svg?style=flat" alt="Swift 5.1 Compatible">
</a>
<a href="https://gitter.im/SmokeServerSide">
<img src="https://img.shields.io/badge/chat-on%20gitter-ee115e.svg?style=flat" alt="Join the Smoke Server Side community on gitter">
</a>
<img src="https://img.shields.io/badge/license-Apache2-blue.svg?style=flat" alt="Apache 2">
</p>

SmokeDynamoDB is a library to make it easy to use DynamoDB from Swift-based applications, with a particular focus on usage with polymorphic database tables (tables that don't have a single schema for all rows.

# Getting Started

## Step 1: Add the SmokeDynamoDB dependency

SmokeDynamoDB uses the Swift Package Manager. To use the framework, add the following dependency
to your Package.swift-

```swift
dependencies: [
    .package(url: "https://github.com/amzn/smoke-dynamodb.git", .upToNextMajor(from: "1.0.0"))
]


.target(
    name: ...,
    dependencies: [..., "SmokeDynamoDB"]),
```

# Basic Usage

## Naming Schema

For consistency in naming across the library, SmokeDynamoDB will case DynamoDB to what is observed and standardized in AWS's documentation of DynamoDB:

- Uppercase: `DynamoDB`
  - Use-cases: Class names, struct names, upper-cased while in the middle of a camel cased function/variable name, and strings referring to it as a proper noun.
  - Examples:
    - `DynamoDBTable`
    - `dropAsDynamoDBKeyPrefix`

- Lowercase: `dynamodb`
  - Use-cases: When used as a prefix to a function/variable name that is lower-cased or camel-cased.
  - Example:
    - `dynamodbKeyWithPrefixedVersion`

## Performing operations on a DynamoDB Table

This package enables operations to be performed on a DynamoDB table using a type that conforms to the `DynamoDBTable` protocol. In a production scenario, operations can be performed using `AwsDynamoDBTable`-

```swift
let table = AwsDynamoDBTable(credentialsProvider: credentialsProvider,
                             region: region, endpointHostName: dynamodbEndpointHostName,
                             tableName: dynamodbTableName)
```

For testing `InMemoryDynamoDBTable` can be used to locally verify what rows will be added to the database table.

## Insertion

An item can be inserted into the DynamoDB table using the following-

```swift
struct PayloadType: Codable, Equatable {
    let firstly: String
    let secondly: String
}

let key = StandardCompositePrimaryKey(partitionKey: "partitionId",
                                      sortKey: "sortId")
let payload = PayloadType(firstly: "firstly", secondly: "secondly")
let databaseItem = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)
        
try table.insertItemSync(databaseItem)
```

The `insertItemSync` (or `insertItemAsync`) operation will attempt to create the following row in the DynamoDB table-
* **PK**: "partitionId" (table partition key)
* **SK**: "sortId" (table sort key)
* **CreateDate**: <the current date>
* **RowType**: "PayloadType"
* **RowVersion**: 1
* **LastUpdatedDate**: <the current date>
* **firstly**: "firstly"
* **secondly**: "secondly"

By default, this operation will fail if an item with the same partition key and sort key already exists.

**Note:** The `StandardCompositePrimaryKey` will place the partition key in the attribute called *PB* and the sort key in an attribute called *SK*. Custom partition and sort key attribute names can be used by dropping down to the underlying `CompositePrimaryKey` type and the `PrimaryKeyAttributes` protocol.

## Retrieval 

An item can be retrieved from the DynamoDB table using the following-

```swift
let retrievedItem: StandardTypedDatabaseItem<PayloadType>? = try table.getItemSync(forKey: key)
```

The `getItemSync` (or `getItemAsync`) operation return an optional `TypedDatabaseItem` which will be nil if the item doesn't exist in the table. These operations will also fail if the *RowType* recorded in the database row doesn't match the type being requested.

## Update

An item can be updated in the DynamoDB table using the following-

```swift
let updatedPayload = PayloadType(firstly: "firstlyX2", secondly: "secondlyX2")
let updatedDatabaseItem = retrievedItem.createUpdatedItem(withValue: updatedPayload)
try table.updateItemSync(newItem: updatedDatabaseItem, existingItem: retrievedItem)
```

The `updateItemSync` (or `updateItemAsync`) operation will attempt to insert the following row in the DynamoDB table-
* **PK**: "partitionId" (table partition key)
* **SK**: "sortId" (table sort key)
* **CreateDate**: <the orginial date when the row was created>
* **RowType**: "PayloadType"
* **RowVersion**: 2
* **LastUpdatedDate**: <the current date>
* **firstly**: "firstlyX2"
* **secondly**: "secondlyX2"

By default, this operation will fail if an item with the same partition key and sort key doesn't exist in the table and if the existing row doesn't have the same version number as the `existingItem` submitted in the operation. The `DynamoDBTable` protocol also provides the `clobberItemSync` and `clobberItemAsync` operations which will overwrite a row in the database regardless of the existing row.

## Conditionally Update

`conditionallyUpdateItemSync` and `conditionallyUpdateItemAsync` operations will attempt to update the primary item, repeatedly calling the `primaryItemProvider` to retrieve an updated version of the current row until the  `update` operation succeeds. The `primaryItemProvider` can throw an exception to indicate that the current row is unable to be updated.

```swift
try table.conditionallyUpdateItemSync(forKey: key, updatedPayloadProvider: updatedPayloadProvider)
```

## Delete

An item can be deleted in the DynamoDB table using the following-

```swift
try table.deleteItemSync(forKey: key)
```

The `deleteItemSync` and `deleteItemAsync` operations will succeed even if the specified row doesn't exist in the database table.

## Queries

All or a subset of the rows from a parition can be retreived using a query-

```swift
struct ExpectedCodableTypes: PossibleItemTypes {
    public static var types: [Codable.Type] = [PayloadType.self]
}

let retrievedItems: [StandardPolymorphicDatabaseItem<ExpectedCodableTypes>] =
    try table.querySync(forPartitionKey: "partitionId",
                        sortKeyCondition: nil)
                                 
for item in retrievedItems {                         
    switch item.rowValue {
    case let payloadType as PayloadType:
        ...
    default:
        // handle unknown row in database
    }
}
```

The sort key condition can restrict the query to a subset of the partition rows. A nil condition will return all rows in the partition. The `querySync` and `queryAsync` operations will fail if the partition contains rows that are not specified in the output `PossibleItemTypes` type.

## Recording updates in a historical parition

This package contains a number of convenience functions for storing versions of a row in a historical parition

### Insertion

`insertItemWithHistoricalRowSync` and `insertItemWithHistoricalRowAsync` operations provide a single call to insert both a primary and historical item-

```swift
try table.insertItemWithHistoricalRowSync(primaryItem: databaseItem, historicalItem: historicalItem)
```

### Update

`updateItemWithHistoricalRowSync` and `updateItemWithHistoricalRowAsync` operations provide a single call to update a primary item and insert a historical item-

```swift
try table.updateItemWithHistoricalRowSync(primaryItem: updatedItem, 
                                          existingItem: databaseItem, 
                                          historicalItem: historicalItem)
```

### Clobber

`clobberItemWithHistoricalRowSync` and `clobberItemWithHistoricalRowAsync` operations will attempt to insert or update the primary item, repeatedly calling the `primaryItemProvider` to retrieve an updated version of the current row (if it exists) until the appropriate `insert` or  `update` operation succeeds. The `historicalItemProvider` is called to provide the historical item based on the primary item that was inserted into the database table. The primary item may not exist in the database table to begin with.

```swift
try table.clobberItemWithHistoricalRowSync(primaryItemProvider: primaryItemProvider,
                                           historicalItemProvider: historicalItemProvider)
```

The `clobberItemWithHistoricalRow*` are typically used when it is unknown if the primary item already exists in the database table and you want to either insert it or write a new version of that row (which may or may not be based on the existing item).

These operations can fail with an concurrency error if the `insert` or  `update` operation repeatedly fails (the default is after 10 attempts).

### Conditionally Update

`conditionallyUpdateItemWithHistoricalRowSync` and `conditionallyUpdateItemWithHistoricalRowAsync` operations will attempt to update the primary item, repeatedly calling the `primaryItemProvider` to retrieve an updated version of the current row until the  `update` operation succeeds. The `primaryItemProvider` can thrown an exception to indicate that the current row is unable to be updated. The `historicalItemProvider` is called to provide the historical item based on the primary item that was inserted into the database table.

```swift
try table.conditionallyUpdateItemWithHistoricalRowSync(
    forPrimaryKey: dKey,
    primaryItemProvider: conditionalUpdatePrimaryItemProvider,
    historicalItemProvider: conditionalUpdateHistoricalItemProvider)
```

The `conditionallyUpdateItemWithHistoricalRow*` operations are typically used when it is known that the primary item exists and you want to test if you can update it based on some attribute of its current version. A common scenario is adding a subordinate related item to the primary item where there is a limit of the number of related items. Here you would want to test the current version of the primary item to ensure the number of related items isn't exceeded.

These operations can fail with an concurrency error if the  `update` operation repeatedly fails (the default is after 10 attempts).

**Note:** The `clobberItemWithHistoricalRow*` and `conditionallyUpdateItemWithHistoricalRow*` operations are similar in nature but have slightly different use cases. The `clobber` operations are typically used to create or update the primary item. The `conditionallyUpdate` operations are typically used when creating a subordinate related item that requires checking if the primary item can be updated.

## Managing versioned rows

`clobberVersionedItemWithHistoricalRowSync` and `clobberVersionedItemWithHistoricalRowAsync` operations provide a mechanism for managing mutable database rows and storing all previous versions of that row in a historical partition. These operations store the primary item under a "version zero" sort key with a payload that replicates the current version of the row. This historical partition contains rows for each version, including the current version under a sort key for that version.

```swift
let payload1 = PayloadType(firstly: "firstly", secondly: "secondly")
let partitionKey = "partitionId"
let historicalPartitionPrefix = "historical"
let historicalPartitionKey = "\(historicalPartitionPrefix).\(partitionKey)"
                
func generateSortKey(withVersion version: Int) -> String {
    let prefix = String(format: "v%05d", version)
    return [prefix, "sortId"].dynamodbKey
}
    
try table.clobberVersionedItemWithHistoricalRowSync(forPrimaryKey: partitionKey,
                                                    andHistoricalKey: historicalPartitionKey,
                                                    item: payload1,
                                                    primaryKeyType: StandardPrimaryKeyAttributes.self,
                                                    generateSortKey: generateSortKey)
                                                             
// the v0 row, copy of version 1
let key1 = StandardCompositePrimaryKey(partitionKey: partitionKey, sortKey: generateSortKey(withVersion: 0))
let item1: StandardTypedDatabaseItem<RowWithItemVersion<PayloadType>> = try table.getItemSync(forKey: key1)
item1.rowValue.itemVersion // 1
item1.rowStatus.rowVersion // 1
item1.rowValue.rowValue // payload1
        
// the v1 row, has version 1
let key2 = StandardCompositePrimaryKey(partitionKey: historicalPartitionKey, sortKey: generateSortKey(withVersion: 1))
let item2: StandardTypedDatabaseItem<RowWithItemVersion<PayloadType>> = try table.getItemSync(forKey: key2)
item1.rowValue.itemVersion // 1
item1.rowStatus.rowVersion // 1
item1.rowValue.rowValue // payload1
        
let payload2 = PayloadType(firstly: "thirdly", secondly: "fourthly")
        
try table.clobberVersionedItemWithHistoricalRowSync(forPrimaryKey: partitionKey,
                                                    andHistoricalKey: historicalPartitionKey,
                                                    item: payload2,
                                                    primaryKeyType: StandardPrimaryKeyAttributes.self,
                                                    generateSortKey: generateSortKey)
        
// the v0 row, copy of version 2
let key3 = StandardCompositePrimaryKey(partitionKey: partitionKey, sortKey: generateSortKey(withVersion: 0))
let item3: StandardTypedDatabaseItem<RowWithItemVersion<PayloadType>> = try table.getItemSync(forKey: key3)
item1.rowValue.itemVersion // 2
item1.rowStatus.rowVersion // 2
item1.rowValue.rowValue // payload2
        
// the v1 row, still has version 1
let key4 = StandardCompositePrimaryKey(partitionKey: historicalPartitionKey, sortKey: generateSortKey(withVersion: 1))
let item4: StandardTypedDatabaseItem<RowWithItemVersion<PayloadType>> = try table.getItemSync(forKey: key4)
item1.rowValue.itemVersion // 1
item1.rowStatus.rowVersion // 1
item1.rowValue.rowValue // payload1
        
// the v2 row, has version 2
let key5 = StandardCompositePrimaryKey(partitionKey: historicalPartitionKey, sortKey: generateSortKey(withVersion: 2))
let item5: StandardTypedDatabaseItem<RowWithItemVersion<PayloadType>> = try table.getItemSync(forKey: key5)
item1.rowValue.itemVersion // 2
item1.rowStatus.rowVersion // 1
item1.rowValue.rowValue // payload2
```

This provides a localized synchronization mechanism for updating mutable rows in a database table where the lock is tracked as the rowVersion of the primary item. This allows versioned mutable rows to updated safely and updates to different primary items do not contend for a table-wide lock.

# Entities

The main entities provided by this package are
* *CompositePrimaryKey*: a struct that stores the partition and sort values for a composite primary key.
* *TypedDatabaseItem*: a struct that manages decoding and encoding rows of a particular type from polymorphic database tables.
* *PolymorphicDatabaseItem*: a struct that manages decoding rows that are one out of a number of types from polymorphic database tables.
* *DynamoDBTable*: a protocol for interacting with a DynamoDB database table.
* *InMemoryDynamoDBTable*: a struct conforming to the `DynamoDBTable` protocol that interacts with a local in-memory table.
* *AwsDynamoDBTable*: a struct conforming to the `DynamoDBTable` protocol that interacts with the AWS DynamoDB service.

## CompositePrimaryKey

The CompositePrimaryKey struct defines the partition and sort key values for a row in the database. It is also used to serialize and deserialize these values. For convenience, this package provides a typealias called `StandardCompositePrimaryKey` that uses a partition key with an attribute name of *PK* and a sort key with an attribute name of *SK*. This struct can be instantiated as shown-

```swift
let key = StandardCompositePrimaryKey(partitionKey: "partitionKeyValue",
                                      sortKey: "sortKeyValue")
```

## TypedDatabaseItem

The TypedDatabaseItem struct manages a number of attributes in the database table to enable decoding and encoding rows to and from the correct type. In addition it also manages other conveniences such as versioning. The attributes this struct will add to a database row are-
* *CreateDate*: The timestamp when the row was created.
* *RowType*: Specifies the schema used by the other attributes of this row.
* *RowVersion*: A version number for the values currently in this row. Used to enable optimistic locking.
* *LastUpdatedDate*: The timestamp when the row was last updated.

Similar to CompositePrimaryKey, this package provides a typealias called `StandardTypedDatabaseItem` that expects the standard partition and sort key attribute names.

This struct can be instantiated as shown-

```swift
let newDatabaseItem = StandardTypedDatabaseItem.newItem(withKey: compositePrimaryKey, andValue: rowValueType)
```

Here *compositePrimaryKey* must be of type `CompositePrimaryKey` and *rowValueType* must conform to the [Codable protocol](https://developer.apple.com/documentation/foundation/archives_and_serialization/encoding_and_decoding_custom_types). By default, performing a **PutItem** operation with this item on a table where this row already exists will fail.

The *createUpdatedItem* function on this struct can be used to create an updated version of this row-

```swift
let updatedDatabaseItem = newDatabaseItem.createUpdatedItem(withValue: updatedValue)
```

This function will create a new instance of TypedDatabaseItem with the same key and updated LastUpdatedDate and RowVersion values. By default, performing a **PutItem** operation with this item on a table where this row already exists and the RowVersion isn't equal to the value of the original row will fail.

## DynamoDBTable

The DynamoDBTable protocol provides a number of functions for interacting with the DynamoDB tables. Typically the `AwsDynamoDBTable` implementation of this protocol is instantiated using a `CredentialProvider` (such as one from the `smoke-aws-credentials` module to automatically handle rotating credentials), the service region and endpoint and the table name to use.

```swift
let dynamodbClient = AwsDynamoDBTable(credentials: credentials,
                                    region: region, endpoint: dynamodbEndpoint,
                                    tableName: dynamodbTableName)
```

Internally AwsDynamoDBTable uses a custom Decoder and Encoder to serialize types that conform to `Codable` to and from the JSON schema required by the DynamoDB service. These Decoder and Encoder implementation automatically captialize attribute names.

# Customization

## PrimaryKeyAttributes

`CompositePrimaryKey`, `TypedDatabaseItem` and `PolymorphicDatabaseItem` are all generic to a type conforming to the `PrimaryKeyAttributes` protocol. This protocol can be used to use custom attribute names for the partition and sort keys.

```swift
public struct MyPrimaryKeyAttributes: PrimaryKeyAttributes {
    public static var partitionKeyAttributeName: String {
        return "MyPartitionAttributeName"
    }
    public static var sortKeyAttributeName: String {
        return "MySortKeyAttributeName"
    }
}
```

## CustomRowTypeIdentifier

If the `Codable` type is used for a row type also conforms to the `CustomRowTypeIdentifier`, the *rowTypeIdentifier* property of this type will be used as the RowType recorded in the database row.

```swift
struct TypeB: Codable, CustomRowTypeIdentifier {
    static var rowTypeIdentifier: String? = "TypeBCustom"
    
    let thirdly: String
    let fourthly: String
}
```

## RowWithIndex

RowWithIndex is a helper struct that provides an index (such as a GSI) attribute as part of the type of a database row.


## RowWithItemVersion

RowWithItemVersion is a helper struct that provides an "ItemVersion" to be used in conjunction with the historical item extensions.

## License
This library is licensed under the Apache 2.0 License.
