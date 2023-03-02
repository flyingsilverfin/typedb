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
load("@vaticle_bazel_distribution//common/tgz2zip:rules.bzl", "tgz2zip")
load("@rules_pkg//:pkg.bzl", "pkg_tar", "pkg_zip")
load("//rust:helpers.bzl", "package_variables_info", "create_empty_dir")


rust_binary(
    name = "typedb-server-binary",
    srcs = ["main.rs"],
    deps = ["//rust/storage:typedb"],
    target_compatible_with = select({
        "@platforms//os:osx": [],
        "@platforms//os:linux": [],
        "//conditions:default": ["@platforms//:incompatible"],
    }),
)

create_empty_dir(
    name = "data-dir",
    dir = "data"
)

pkg_files = {
    ":data-dir": "server/data",
    ":typedb-server-binary": "server/bin/typedb",
    "//server:config": "server/conf/config.yml",
    "//server/resources:logo": "server/resources/typedb-ascii.txt",
    "//:LICENSE": "LICENSE",
}

pkg_file_permissions = {
    "server/conf/config.yml" : "0755",
    "server/data" : "0755",
}

package_variables_info(
    name = "pkg_vars",
    target_os = select({
        "@vaticle_dependencies//util/platform:is_mac": "mac",
        "@vaticle_dependencies//util/platform:is_linux": "linux",
        "@vaticle_dependencies//util/platform:is_windows": "windows",
    }),
    target_cpu = select({
        "@vaticle_dependencies//util/platform:is_arm64": "arm64",
        "@vaticle_dependencies//util/platform:is_x86_64": "x86_64",
    }),
)

pkg_tar(
    name = "typedb-server-native-targz",
    deps = [
        "@vaticle_typedb_common//binary:assemble-bash-targz",
    ],
    files = pkg_files,
    modes = pkg_file_permissions,
    extension = "tar.gz",
    package_file_name = "typedb-server-{target_os}-{target_cpu}-{version}.tar.gz",
    package_dir = "typedb-server-{target_os}-{target_cpu}",
    package_variables = ":pkg_vars",
    strip_prefix = ".", # Preserve directory structures
)

# TODO - best way to generate the windows binary

genrule(
    name = "typedb-server-binary-windows",
    srcs = glob(["**/*.rs", "**/**/Cargo.toml", "storage/*"]),
    cmd = "pwd; \
    tree; \
    cd rust; \
    cargo build --release; \
    ",
    outs = ["target/release/typedb-server-windows-x86_64.exe"],
    target_compatible_with = select({
        "@platforms//os:windows": [],
        "//conditions:default": ["@platforms//:incompatible"],
    }),
)


checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
    ]),
    exclude = ["LICENSE", "README.md"],
    license_type = "agpl-header",
)
