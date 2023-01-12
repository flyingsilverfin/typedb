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


load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_library", "rust_test")


filegroup(
    name = "rocksdb",
    srcs = [":libspeedb.so.2.2.0"],
    visibility = ["//visibility:public"],
)

rust_library(
    name = "typedb",
    srcs = glob([
        "*.rs",
    ]),
    deps = [
        "@vaticle_dependencies//library/crates:rocksdb"
    ],
    visibility = ["//visibility:public"]
)

genrule(
    name = "prepare-dirs",
    srcs = [":rocksdb"],
    cmd = "mkdir -p $(@D) && cp -R $(location :rocksdb) $(@D)/rocksdb.so",
    outs = ["lib"]
)

rust_binary(
    name = "typedb-binary",
    srcs = ["main.rs"],
    deps = [":typedb"],
    data = [":prepare-dirs"]
)
