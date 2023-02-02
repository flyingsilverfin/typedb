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

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

def librocksdb_mac_arm64():
    http_archive(
        name = "librocksdb_mac_arm64",
        urls = ["https://repo.vaticle.com/repository/dll/com.facebook/7.9.2/librocksdb-mac-arm64-7.9.2.zip"],
        sha256 = "f80da6612beab644a06c9cab65ff36948a7b1c1be4542c7e73667b131fa5b2e2",
        build_file_content = """
filegroup(
    name = "lib-files",
    srcs = glob(["librocksdb*"]),
    visibility = ["//visibility:public"]
)

cc_import(
    name = "librocksdb-mac-arm64",
    shared_library = ":librocksdb.dylib",
    visibility = ["//visibility:public"]
)
        """
    )
