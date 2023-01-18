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


load("@rules_rust//rust:defs.bzl", "rust_binary")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")


filegroup(
    name = "librocksdb-mac",
    srcs = [":librocksdb.7.10.dylib"],
    visibility = ["//visibility:public"],
)

filegroup(
    name = "librocksdb-linux",
    srcs = ["libspeedb.so.2.2.0"],
    visibility = ["//visibility:public"],
)

#genrule(
#    name = "prepare-dirs-compile-mac",
#    srcs = [":librocksdb-mac"],
#    cmd = "mkdir -p $(@D) && cp $(location :librocksdb-mac) $(@D)/librocksdb.dylib",
#    outs = ["lib/librocksdb.dylib"]
#)
#
## this will put the DYLIB in the output lib/ directory. However, this wont be in the search path on Mac automatically, we may have to use 'DYLD_LIBRARY_PATH=lib' for example
#genrule(
#    name = "prepare-dirs-versioned-run",
#    srcs = [":librocksdb-mac"],
#    cmd = "mkdir -p $(@D) && cp $(location :librocksdb-mac) $(@D)/librocksdb.7.10.dylib",
#    outs = ["lib/librocksdb.7.10.dylib"]
#)

cc_import(
    name = "rocksdb-lib-mac",
    shared_library = ":librocksdb-mac",
)

rust_binary(
    name = "typedb-binary-mac",
    srcs = ["main.rs"],
    deps = ["//rust/storage:typedb", ":rocksdb-lib-mac"],
#    data = [":prepare-dirs-versioned-run"]
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "**/*",
    ]),
    exclude = ["LICENSE", "README.md"],
    license_type = "apache-header",
)
