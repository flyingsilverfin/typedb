#
# Copyright (C) 2022 Vaticle
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#


load("@rules_rust//rust:defs.bzl", "rust_static_library", "rust_library", "rust_shared_library")
load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")


filegroup(
    name = "librocksdb-mac-arm64-dylib",
    srcs = [":librocksdb.dylib"],
)

cc_import(
    name = "librocksdb-mac-arm64",
    shared_library = ":librocksdb.dylib",
    visibility = ["//visibility:public"]
)

rust_library(
    name = "typedb",
    srcs = [
        "lib.rs",
    ],
    deps = [
        "@crates//:rocksdb",
    ],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
    ]),
    exclude = ["LICENSE", "README.md"],
    license_type = "agpl-header",
)
