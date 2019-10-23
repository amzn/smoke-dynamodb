import XCTest
@testable import SmokeDynamoDBTests

XCTMain([
    testCase(DynamoDBHistoricalClientTests.allTests),
    testCase(DynamoDBTableUpdateItemConditionallyAtKeyTests.allTests),
    testCase(DynamoDBEncoderDecoderTests.allTests),
    testCase(InMemoryDynamoDBTableTests.allTests),
    testCase(SimulateConcurrencyDynamoDBTableTests.allTests),
    testCase(StringDynamoDBKeyTests.allTests),
    testCase(SmokeDynamoDBTests.allTests),
    testCase(TypedDatabaseItemRowWithItemVersionProtocolTests.allTests),
    testCase(DynamoDBTableClobberVersionedItemWithHistoricalRowTests.allTests),
    testCase(DynamoDBCompositePrimaryKeyTableClobberVersionedItemWithHistoricalRowTests.allTests),
    testCase(CompositePrimaryKeyDynamoDBHistoricalClientTests.allTests),
    testCase(DynamoDBCompositePrimaryKeyTableUpdateItemConditionallyAtKeyTests.allTests),
    testCase(InMemoryDynamoDBCompositePrimaryKeyTableTests.allTests),
    testCase(SimulateConcurrencyDynamoDBCompositePrimaryKeyTableTests.allTests),
])
