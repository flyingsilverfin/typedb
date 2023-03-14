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

REM First generate all Cargo.toml files
bazel run @vaticle_dependencies//tool/cargo:sync

REM We set some environment variables when building using Bazel to invoke Cargo builds
REM   PATH is required in order to find the 'cargo' executable
REM   ProgramData is required in order to find the right MSVC compiler
REM   BUILD_WORKSPACE_DIRECTORY is set explicitly since Bazel may not set it on Windows
REM   CARGO_NET_GIT_FETCH_WITH_CLI is lets Cargo use the Windows 'git' executable instead of Bazels'
set /p VERSION=<VERSION
bazel build --define version=%VERSION% //rust:typedb-server-native-windows-targz --action_env=PATH="%PATH%" --action_env=ProgramData=%ProgramData% --action_env=BUILD_WORKSPACE_DIRECTORY=%cd% --action_env=CARGO_NET_GIT_FETCH_WITH_CLI=true

:error
IF %errorlevel% NEQ 0 EXIT /b %errorlevel%
