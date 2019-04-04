// swift-tools-version:4.1
//
// Copyright 2018-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
    products: [
        // Products define the executables and libraries produced by a package, and make them visible to other packages.
        .library(
            name: "SmokeDynamoDB",
            targets: ["SmokeDynamoDB"]),
    ],
    dependencies: [
        .package(url: "https://github.com/amzn/smoke-aws.git", .upToNextMajor(from: "1.0.0")),
        .package(url: "https://github.com/amzn/smoke-http.git", .upToNextMajor(from: "1.0.0")),
        .package(url: "https://github.com/IBM-Swift/LoggerAPI.git", .upToNextMajor(from: "1.0.0")),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .target(
            name: "SmokeDynamoDB",
            dependencies: ["LoggerAPI", "DynamoDBClient", "SmokeHTTPClient"]),
        .testTarget(
            name: "SmokeDynamoDBTests",
            dependencies: ["SmokeDynamoDB", "SmokeHTTPClient"]),
    ]
)
