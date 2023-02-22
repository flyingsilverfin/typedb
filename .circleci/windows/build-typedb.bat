CALL refreshenv

REM Use platform's git instead of built-in.
REM Equivalent to setting `net.git-fetch-with-cli = true` for crate_repositories defined in @vaticle_dependencies
set CARGO_NET_GIT_FETCH_WITH_CLI=true

REM Prepare cargo manifests
bazel run @vaticle_dependencies//tool/cargo:sync

cd rust
cargo build --release

:error
IF %errorlevel% NEQ 0 EXIT /b %errorlevel%
