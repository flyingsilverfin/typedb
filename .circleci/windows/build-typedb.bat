CALL refreshenv

REM build typedb-all-windows archive
bazel--output_user_root=C:/_bzl build --enable-runfiles //rust:typedb-binary-mac

:error
IF %errorlevel% NEQ 0 EXIT /b %errorlevel%
