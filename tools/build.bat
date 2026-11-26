@echo off
setlocal

set BIN_NAME=ws_bench

:: parse args, default to release
set MODE=%~1
if "%MODE%"=="" set MODE=release

if /I "%MODE%"=="release" (
    set CARGO_FLAG=--release
    set BUILD_DIR=release
) else if /I "%MODE%"=="debug" (
    set CARGO_FLAG=
    set BUILD_DIR=debug
) else (
    echo [error] Unknown mode "%MODE%". Use: release ^| debug
    exit /b 1
)

set OUT_DIR=dist\windows\%BUILD_DIR%

echo [build] Compiling %BIN_NAME% for Windows (%MODE%)...
cargo build %CARGO_FLAG%
if %ERRORLEVEL% neq 0 (
    echo [error] Build failed.
    exit /b %ERRORLEVEL%
)

if not exist %OUT_DIR% mkdir %OUT_DIR%
copy /Y target\%BUILD_DIR%\%BIN_NAME%.exe %OUT_DIR%\%BIN_NAME%.exe

echo [done] Output: %OUT_DIR%\%BIN_NAME%.exe
endlocal
