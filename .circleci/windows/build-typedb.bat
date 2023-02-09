CALL refreshenv

REM build typedb-all-windows archive
bazel --output_base=D:/ build --enable_runfiles //rust:typedb-binary-mac --verbose_failures

:error
IF %errorlevel% NEQ 0 EXIT /b %errorlevel%
