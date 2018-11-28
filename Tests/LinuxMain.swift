import XCTest
@testable import SmokeDynamoTests

XCTMain([
    testCase(DynamoHistoricalClientTests.allTests),
    testCase(DynamoDBTableUpdateItemConditionallyAtKeyTests.allTests),
    testCase(DynamoEncoderDecoderTests.allTests),
    testCase(InMemoryDynamoDBTableTests.allTests),
    testCase(SimulateConcurrencyDynamoDBTableTests.allTests),
    testCase(StringDynamoKeyTests.allTests),
    testCase(SmokeDynamoTests.allTests),
    testCase(TypedDatabaseItemRowWithItemVersionProtocolTests.allTests).allTests,
    testCase(DynamoDBTableClobberVersionedItemWithHistoricalRowTests.allTests),
])
