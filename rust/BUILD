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


load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_library", "rust_shared_library")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

genrule(
    name = "copy-dynamic-libraries-for-compile",
    srcs = ["//rust/storage:libspeedb-mac"],
    cmd = "mkdir -p $(@D) && ln -s `realpath $(location //rust/storage:libspeedb-mac)` $(@D)/librocksdb.dylib",
    outs = ["librocksdb.dylib"]
)

genrule(
    name = "copy-libspeedb",
    srcs = ["//rust/storage:libspeedb-mac"],
    cmd = "mkdir -p $(@D) && cp $(location //rust/storage:libspeedb-mac) $(@D)/libspeedb.dylib",
    outs = ["libspeedb.dylib"]
)
#

#genrule(
#    name = "prepare-dirs-versioned-run",
#    srcs = [":librocksdb-mac"],
#    cmd = "mkdir -p $(@D) && cp $(location :librocksdb-mac) $(@D)/librocksdb.7.10.dylib",
#    outs = ["lib/librocksdb.7.10.dylib"]
#)

cc_import(
    name = "speedb-lib-mac",
    shared_library = ":copy-dynamic-libraries-for-compile",
    visibility = ["//visibility:public"],
)

rust_binary(
    name = "typedb-binary-mac",
    srcs = ["main.rs"],
    deps = ["//rust/storage:typedb", ":speedb-lib-mac"], # , ":copy-linker-dynamic-library"],
    rustc_flags = [
        "-C", "link-arg=-Wl,-rpath,$$ORIGIN"
    ],
    data = [":copy-libspeedb"]
#    compile_data = [":copy-dynamic-libraries-for-compile"], #":copy-runtime-dynamic-library"]
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
