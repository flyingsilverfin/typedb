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


load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_library", "rust_shared_library", "rust_static_library")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

#genrule(
#    name = "copy-dynamic-libraries-for-compile",
#    srcs = ["//rust/storage:librocksdb-mac-arm64"],
#    cmd = "mkdir -p $(@D) && ln -s `realpath $(location //rust/storage:librocksdb-mac-arm64)` $(@D)/librocksdb.dylib",
#    outs = ["librocksdb.dylib"]
#)
#
#genrule(
#    name = "copy-librocksdb",
#    srcs = ["//rust/storage:librocksdb-mac-arm64"],
#    cmd = "mkdir -p $(@D) && cp $(location //rust/storage:librocksdb-mac-arm64) $(@D)/librocksdb.dylib",
#    outs = ["librocksdb.dylib"]
#)

rust_binary(
    name = "typedb-server-binary",
    srcs = ["main.rs"],
    deps = ["//rust/storage:typedb"],#, "//rust/storage:librocksdb-mac-arm64"],
    rustc_flags = [
#        "-C", "link-arg=-Wl,-rpath,$$ORIGIN"
    ],
#    data = [":copy-librocksdb"]
)

#genrule(
#    name = "typedb-server-binary-windows-x86_64",
#    srcs = [],
#    cmd_bat = """
#    cargo build
#    """,
#    outs = ["typedb-server-windows-x86_64.exe"]
#)


checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
    ]),
    exclude = ["LICENSE", "README.md"],
    license_type = "agpl-header",
)
