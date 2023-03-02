REM
REM Copyright (C) 2022 Vaticle
REM
REM This program is free software: you can redistribute it and/or modify
REM it under the terms of the GNU Affero General Public License as
REM published by the Free Software Foundation, either version 3 of the
REM License, or (at your option) any later version.
REM
REM This program is distributed in the hope that it will be useful,
REM but WITHOUT ANY WARRANTY; without even the implied warranty of
REM MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
REM GNU Affero General Public License for more details.
REM
REM You should have received a copy of the GNU Affero General Public License
REM along with this program.  If not, see <https://www.gnu.org/licenses/>.
REM

CALL refreshenv

set DIR=%cd%

REM Use platform's git instead of built-in.
REM Equivalent to setting `net.git-fetch-with-cli = true` for crate_repositories defined in @vaticle_dependencies
set CARGO_NET_GIT_FETCH_WITH_CLI=true

cd %BUILD_WORKSPACE_DIRECTORY%

REM Prepare cargo manifests
bazel run @vaticle_dependencies//tool/cargo:sync

cd rust
cargo build --release

mv target/release/typedb-server-binary.exe %DIR%

cd %DIR%