#
# Copyright (C) 2023 Vaticle
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
load("@rules_pkg//:pkg.bzl", "pkg_tar")
load("//rust:packaging.bzl", "package_variables_info", "create_empty_dir")


# NOTE: This will fail to compile transitive RocksDB sources on Windows, due to the 255-char path-length limitation
rust_binary(
    name = "typedb-server-binary-native",
    srcs = ["main.rs"],
    deps = [
        "//rust/storage:storage",
        "//rust/traversal:traversal",
    ],
)

genrule(
    name = "typedb-server-binary-windows",
    cmd_bat = ".\\$(location :build-typedb-windows.bat) $@",
    outs = ["typedb-server-windows-x86_64.exe"],
    tools = [":build-typedb-windows.bat"],
    executable = True,
    target_compatible_with = select({
        "@platforms//os:windows": [],
        "//conditions:default": ["@platforms//:incompatible"],
    }),
)

pkg_files_unix = {
    ":typedb-server-binary-native": "server/bin/typedb",
    ":data-dir": "server/data",
    "//server:config": "server/conf/config.yml",
    "//server/resources:logo": "server/resources/typedb-ascii.txt",
    "//:LICENSE": "LICENSE",
}

pkg_files_windows = {
    ":typedb-server-binary-windows": "server/bin/typedb",
    ":data-dir": "server/data",
    "//server:config": "server/conf/config.yml",
    "//server/resources:logo": "server/resources/typedb-ascii.txt",
    "//:LICENSE": "LICENSE",
}

pkg_file_permissions = {
    "server/conf/config.yml" : "0755",
    "server/data" : "0755",
}

create_empty_dir(
    name = "data-dir",
    dir = "data"
)

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
    name = "typedb-server-native-unix-targz",
    deps = [
        "@vaticle_typedb_common//binary:assemble-bash-targz",
    ],
    files = pkg_files_unix,
    modes = pkg_file_permissions,
    extension = "tar.gz",
    package_file_name = "typedb-server-{target_os}-{target_cpu}-{version}.tar.gz",
    package_dir = "typedb-server-{target_os}-{target_cpu}",
    package_variables = ":pkg_vars",
    strip_prefix = ".", # Preserve directory structures
    target_compatible_with = select({
        "@platforms//os:osx": [],
        "@platforms//os:linux": [],
        "//conditions:default": ["@platforms//:incompatible"],
    }),
)

pkg_tar(
    name = "typedb-server-native-windows-targz",
    deps = [
        "@vaticle_typedb_common//binary:assemble-bat-targz",
    ],
    files = pkg_files_windows,
    modes = pkg_file_permissions,
    extension = "tar.gz",
    package_file_name = "typedb-server-{target_os}-{target_cpu}-{version}.tar.gz",
    package_dir = "typedb-server-{target_os}-{target_cpu}",
    package_variables = ":pkg_vars",
    strip_prefix = ".", # Preserve directory structures
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
