CALL refreshenv

REM prepare the Cargo files
mkdir .cargo
cd .cargo
(
    echo "[net]"
    echo "git-fetch-with-cli = true"
) > config.toml
cd ..

bazel run @vaticle_dependencies//tool/cargo:sync

REM build typedb-all-windows archive
cd rust


cargo build


:error
IF %errorlevel% NEQ 0 EXIT /b %errorlevel%
