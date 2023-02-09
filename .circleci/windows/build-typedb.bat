CALL refreshenv

REM build typedb-all-windows archive
bazel --output_base=C:\_b --output_user_root=C:\_c build --enable_runfiles //rust:typedb-binary-mac --verbose_failures --experimental_ui_max_stdouterr_bytes=999999999

:error
IF %errorlevel% NEQ 0 EXIT /b %errorlevel%
