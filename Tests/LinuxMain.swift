import XCTest
@testable import SmokeDynamoDBTests

XCTMain([
    testCase(DynamoDBCompositePrimaryKeyTableClobberVersionedItemWithHistoricalRowTests.allTests),
    testCase(CompositePrimaryKeyDynamoDBHistoricalClientTests.allTests),
    testCase(DynamoDBCompositePrimaryKeyTableUpdateItemConditionallyAtKeyTests.allTests),
    testCase(DynamoDBEncoderDecoderTests.allTests),
    testCase(InMemoryDynamoDBCompositePrimaryKeyTableTests.allTests),
    testCase(SimulateConcurrencyDynamoDBCompositePrimaryKeyTableTests.allTests),
    testCase(SmokeDynamoDBTests.allTests),
    testCase(StringDynamoDBKeyTests.allTests),
    testCase(TypedDatabaseItemRowWithItemVersionProtocolTests.allTests),
])
