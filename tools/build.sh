#!/usr/bin/env sh
# Cross-platform build script: auto-detects the current OS/arch and packages the
# matching target. Works on Linux, macOS and Windows (run under Git Bash / MSYS2).
set -e

BIN_NAME=ws_bench

# Parse arguments, default to release
MODE=${1:-release}
case "$MODE" in
    release) CARGO_FLAG="--release" ; BUILD_DIR=release ;;
    debug)   CARGO_FLAG=""          ; BUILD_DIR=debug   ;;
    *)
        echo "[error] Unknown mode \"$MODE\". Use: release | debug"
        exit 1
        ;;
esac

# Detect OS and architecture, resolve target triple and output dir
OS=$(uname -s)
ARCH=$(uname -m)
case "$OS" in
    Linux)
        case "$ARCH" in
            aarch64) TARGET=aarch64-unknown-linux-gnu ;;
            *)       TARGET=x86_64-unknown-linux-gnu  ;;
        esac
        PLATFORM=linux
        OUT_DIR=dist/linux/$BUILD_DIR
        EXE_EXT=""
        ;;
    Darwin)
        case "$ARCH" in
            arm64)  TARGET=aarch64-apple-darwin ;;
            x86_64) TARGET=x86_64-apple-darwin  ;;
            *)
                echo "[error] Unsupported macOS architecture: $ARCH"
                exit 1
                ;;
        esac
        PLATFORM=macos
        OUT_DIR=dist/macos/$BUILD_DIR
        EXE_EXT=""
        ;;
    MINGW*|MSYS*|CYGWIN*)
        TARGET=x86_64-pc-windows-msvc
        PLATFORM=windows
        OUT_DIR=dist/windows/$BUILD_DIR
        EXE_EXT=".exe"
        ;;
    *)
        echo "[error] Unsupported OS: $OS"
        exit 1
        ;;
esac

echo "[build] Detected $PLATFORM ($ARCH) -> $TARGET ($MODE)..."

rustup target add "$TARGET" 2>/dev/null || true
cargo build $CARGO_FLAG --target "$TARGET"

mkdir -p "$OUT_DIR"
cp "target/$TARGET/$BUILD_DIR/$BIN_NAME$EXE_EXT" "$OUT_DIR/$BIN_NAME$EXE_EXT"

# Ensure the executable bit on unix-like platforms
if [ -z "$EXE_EXT" ]; then
    chmod +x "$OUT_DIR/$BIN_NAME"
fi

echo "[done] Output: $OUT_DIR/$BIN_NAME$EXE_EXT"
