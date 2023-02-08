CALL refreshenv

REM build typedb-all-windows archive
bazel build //rust:typedb-binary-mac

:error
IF %errorlevel% NEQ 0 EXIT /b %errorlevel%
