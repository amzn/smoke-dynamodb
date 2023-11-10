// swift-tools-version:5.7
//
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

import PackageDescription

let package = Package(
    name: "smoke-dynamodb",
    platforms: [
        .macOS(.v10_15), .iOS(.v13), .watchOS(.v6), .tvOS(.v13)
        ],
    products: [
        .library(
            name: "SmokeDynamoDB",
            targets: ["SmokeDynamoDB"]),
    ],
    dependencies: [
        .package(url: "https://github.com/awslabs/aws-sdk-swift.git", from: "0.31.0"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", "1.0.0"..<"3.0.0"),
        .package(url: "https://github.com/JohnSundell/CollectionConcurrencyKit", from :"0.2.0")
    ],
    targets: [
        .target(
            name: "SmokeDynamoDB", dependencies: [
                .product(name: "Logging", package: "swift-log"),
                .product(name: "Metrics", package: "swift-metrics"),
                .product(name: "AWSDynamoDB", package: "aws-sdk-swift"),
                .product(name: "CollectionConcurrencyKit", package: "CollectionConcurrencyKit"),
            ]),
        .testTarget(
            name: "SmokeDynamoDBTests", dependencies: [
                .target(name: "SmokeDynamoDB"),
            ]),
    ],
    swiftLanguageVersions: [.v5]
)
