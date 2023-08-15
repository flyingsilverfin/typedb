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

load("@vaticle_bazel_distribution//apt:rules.bzl", "assemble_apt", "deploy_apt")
load("@vaticle_bazel_distribution//common:rules.bzl", "assemble_targz", "assemble_zip", "java_deps")
load("@vaticle_bazel_distribution//artifact:rules.bzl", "deploy_artifact")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@rules_proto_grpc//java:defs.bzl", "java_grpc_library", "java_proto_library")

package(default_visibility = ["//visibility:private",])

proto_library(
    name = "migrator-protocol-protos",
    srcs = ["migrator.proto", "database/data.proto"],
)

java_grpc_library(
    name = "migrator-protocol",
    protos = [":migrator-protocol-protos"],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-migrator-protocol:{pom_version}"],
)

native_java_libraries(
    name = "migrator",
    srcs = glob(["*.java", "*/*.java", "*/*/*.java"]),
    native_libraries_deps = [
        # Internal dependencies
        "//:typedb",
        "//common:common",
        "//concept:concept",
        "//concurrent:concurrent",
        "//encoding:encoding",
        "//logic:logic",
    ],
    deps = [
        ":migrator-protocol",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/pattern:pattern",
        "@vaticle_typeql//java/common:common",

        # External dependencies from Maven
        "@maven//:org_slf4j_slf4j_api",
    ],
    mac_deps = [
        "@maven//:io_github_speedb_io_speedbjni"
    ],
    linux_deps = [
        "@maven//:io_github_speedb_io_speedbjni",
    ],
    windows_deps = [
        "@maven//:io_github_speedb_io_speedbjni",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-migrator:{pom_version}"],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl-header",
)

