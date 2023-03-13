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

set DIR=%cd%
set OUTPUT=%1

CALL setx CARGO_NET_GIT_FETCH_WITH_CLI true
set CARGO_NET_GIT_FETCH_WITH_CLI=true

cd %BUILD_WORKSPACE_DIRECTORY%

echo "RUNNING SYNC TOOL" >> log.txt
@REM
@REM REM Prepare cargo manifests
CALL bazel run @vaticle_dependencies//tool/cargo:sync

cd rust

echo "RUNNING CARGO BUILD" >> log.txt
cargo build --release

echo "MOVING BINARY" >> log.txt
move target\release\typedb-server-binary.exe %DIR%\%OUTPUT%

cd %DIR%
