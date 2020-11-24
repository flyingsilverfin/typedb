#
# Copyright (C) 2020 Grakn Labs
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

load("//:deployment.bzl", deployment_github = "deployment")
load("@graknlabs_bazel_distribution//brew:rules.bzl", "deploy_brew")
load("@graknlabs_bazel_distribution//common:rules.bzl", "assemble_targz", "java_deps", "assemble_zip", "checksum", "assemble_versioned")
load("@graknlabs_bazel_distribution//github:rules.bzl", "deploy_github")
load("@graknlabs_bazel_distribution//apt:rules.bzl", "assemble_apt", "deploy_apt")
load("@graknlabs_bazel_distribution//rpm:rules.bzl", "assemble_rpm", "deploy_rpm")
load("@graknlabs_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@graknlabs_dependencies//distribution:deployment.bzl", "deployment")
load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@graknlabs_dependencies//tool/release:rules.bzl", "release_validate_deps")

exports_files(
    ["VERSION", "deployment.bzl", "RELEASE_TEMPLATE.md", "LICENSE", "README.md"],
)

native_java_libraries(
    name = "grakn",
    srcs = glob(["*.java"]),
    deps = [
        # Internal dependencies
        "//common:common",
    ],
    native_libraries_deps = [
        "//query:query",
        "//concept:concept",
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-core:{pom_version}"],
    visibility = ["//visibility:public"],
)

assemble_files = {
    "//server/conf:logback": "server/conf/logback.xml",
    "//server/conf:logback-debug": "server/conf/logback-debug.xml",
    "//server/conf:grakn-properties": "server/conf/grakn.properties",
    "//server/resources:logo": "server/resources/grakn-core-ascii.txt",
    "//:LICENSE": "LICENSE",
}

assemble_deps_common = [
    "//server:server-deps-dev",
#    "//server:server-deps-prod",
    "@graknlabs_common//binary:assemble-bash-targz",
    "@graknlabs_console_artifact//file"
]

assemble_targz(
    name = "assemble-linux-targz",
    targets = assemble_deps_common + ["//server:server-deps-linux"],
    additional_files = assemble_files,
    output_filename = "grakn-core-all-linux",
)

assemble_zip(
    name = "assemble-mac-zip",
    targets = assemble_deps_common + ["//server:server-deps-mac"],
    additional_files = assemble_files,
    output_filename = "grakn-core-all-mac",
)

assemble_zip(
    name = "assemble-windows-zip",
    targets = assemble_deps_common + ["//server:server-deps-windows"],
    additional_files = assemble_files,
    output_filename = "grakn-core-all-windows",
)

assemble_versioned(
    name = "assemble-versioned-all",
    targets = [
        ":assemble-linux-targz",
        ":assemble-mac-zip",
        ":assemble-windows-zip",
        "//server:assemble-linux-targz",
        "//server:assemble-mac-zip",
        "//server:assemble-windows-zip",
    ],
)

assemble_versioned(
    name = "assemble-versioned-mac",
    targets = [":assemble-mac-zip"],
)

checksum(
    name = "checksum-mac",
    archive = ":assemble-versioned-mac",
)

deploy_github(
    name = "deploy-github",
    organisation = deployment_github['github.organisation'],
    repository = deployment_github['github.repository'],
    title = "Grakn Core",
    title_append_version = True,
    release_description = "//:RELEASE_TEMPLATE.md",
    archive = ":assemble-versioned-all",
    draft = False
)

deploy_brew(
    name = "deploy-brew",
    snapshot = deployment['brew.snapshot'],
    release = deployment['brew.release'],
    formula = "//config/brew:grakn-core.rb",
    checksum = "//:checksum-mac",
    version_file = "//:VERSION"
)

assemble_apt(
    name = "assemble-linux-apt",
    package_name = "grakn-core-all",
    maintainer = "Grakn Labs <community@grakn.ai>",
    description = "Grakn Core (all)",
    depends = [
        "openjdk-8-jre",
        "grakn-core-server (=%{version})",
        "grakn-console (=%{@graknlabs_console_artifact})",
    ],
    workspace_refs = "@graknlabs_grakn_core_workspace_refs//:refs.json",
)

deploy_apt(
    name = "deploy-apt",
    target = ":assemble-linux-apt",
    snapshot = deployment['apt.snapshot'],
    release = deployment['apt.release'],
)

assemble_rpm(
    name = "assemble-linux-rpm",
    package_name = "grakn-core-all",
    spec_file = "//config/rpm:grakn-core-all.spec",
    workspace_refs = "@graknlabs_grakn_core_workspace_refs//:refs.json",
)

deploy_rpm(
    name = "deploy-rpm",
    target = ":assemble-linux-rpm",
    snapshot = deployment['rpm.snapshot'],
    release = deployment['rpm.release'],
)

release_validate_deps(
    name = "release-validate-deps",
    refs = "@graknlabs_grakn_core_workspace_refs//:refs.json",
    tagged_deps = [
        "@graknlabs_common",
        "@graknlabs_graql",
        "@graknlabs_protocol",
    ],
    tags = ["manual"]  # in order for bazel test //... to not fail
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", ".grabl/*", "bin/*"]),
    exclude = glob(["docs/*"]),
    license_type = "agpl",
)

# CI targets that are not declared in any BUILD file, but are called externally
filegroup(
    name = "ci",
    data = [
        "@graknlabs_dependencies//image/rbe:ubuntu-1604",
        "@graknlabs_dependencies//library/maven:update",
        "@graknlabs_dependencies//tool/checkstyle:test-coverage",
        "@graknlabs_dependencies//tool/sonarcloud:code-analysis",
        "@graknlabs_dependencies//tool/unuseddeps:unused-deps",
    ],
)
