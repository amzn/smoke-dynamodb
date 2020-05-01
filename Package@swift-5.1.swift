// swift-tools-version:5.0
//
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

import PackageDescription

let package = Package(
    name: "SmokeDynamoDB",
    platforms: [
        .macOS(.v10_12), .iOS(.v10)
        ],
    products: [
        .library(
            name: "SmokeDynamoDB",
            targets: ["SmokeDynamoDB"]),
    ],
    dependencies: [
        .package(url: "https://github.com/amzn/smoke-aws.git", from: "2.0.0-rc.1"),
        .package(url: "https://github.com/amzn/smoke-http.git", from: "2.0.0-rc.1"),
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-metrics.git", "1.0.0"..<"3.0.0"),
    ],
    targets: [
        .target(
            name: "SmokeDynamoDB",
            dependencies: ["Metrics", "Logging", "DynamoDBClient", "SmokeHTTPClient"]),
        .testTarget(
            name: "SmokeDynamoDBTests",
            dependencies: ["SmokeDynamoDB", "SmokeHTTPClient"]),
    ],
    swiftLanguageVersions: [.v5]
)
