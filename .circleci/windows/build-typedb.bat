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

set CARGO_NET_GIT_FETCH_WITH_CLI=true
bazel run @vaticle_dependencies//tool/cargo:sync
bazel run //rust:typedb-server-binary-windows --action_env=path="%PATH%" --action_env=ProgramData=%ProgramData% --action_env=BUILD_WORKSPACE_DIRECTORY=%cd% --action_env=CARGO_NET_GIT_FETCH_WITH_CLI=true

:error
IF %errorlevel% NEQ 0 EXIT /b %errorlevel%
