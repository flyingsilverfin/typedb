CALL refreshenv

REM prepare the Cargo files
bazel run @vaticle_dependencies//ide/rust:sync

REM build typedb-all-windows archive
cd rust
cargo build


:error
IF %errorlevel% NEQ 0 EXIT /b %errorlevel%
