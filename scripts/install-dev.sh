#!/bin/sh

set -eu

if [ "$#" -ne 3 ]; then
  printf '%s\n' 'usage: install-dev.sh SOURCE DEV_INSTALL_DIR DEV_BIN_NAME' >&2
  exit 2
fi

source_binary=$1
install_dir=$2
binary_name=$3

if [ ! -f "$source_binary" ] || [ ! -x "$source_binary" ]; then
  printf 'error: development binary is missing or not executable: %s\n' "$source_binary" >&2
  exit 2
fi
if [ -z "$install_dir" ]; then
  printf '%s\n' 'error: DEV_INSTALL_DIR must not be empty' >&2
  exit 2
fi
case "$binary_name" in
  nac-web)
    printf '%s\n' 'error: DEV_BIN_NAME must not be nac-web; that name is reserved for stable installs' >&2
    exit 2
    ;;
  ''|.*|*[!A-Za-z0-9._-]*)
    printf '%s\n' 'error: DEV_BIN_NAME must start with a letter or digit and contain only letters, digits, dot, underscore, or hyphen' >&2
    exit 2
    ;;
esac

if [ -e "$install_dir" ] && [ ! -d "$install_dir" ]; then
  printf 'error: DEV_INSTALL_DIR exists but is not a directory: %s\n' "$install_dir" >&2
  exit 2
fi
if [ -L "$install_dir" ]; then
  printf 'error: DEV_INSTALL_DIR must not be a symbolic link: %s\n' "$install_dir" >&2
  exit 2
fi
mkdir -p "$install_dir"
if [ ! -d "$install_dir" ] || [ ! -w "$install_dir" ]; then
  printf 'error: DEV_INSTALL_DIR is not a writable directory: %s\n' "$install_dir" >&2
  exit 2
fi

target=$install_dir/$binary_name
temporary=$(mktemp "$install_dir/.${binary_name}.tmp.XXXXXX")
cleanup() {
  rm -f "$temporary"
}
trap cleanup EXIT HUP INT TERM
install -m 755 "$source_binary" "$temporary"
mv -f "$temporary" "$target"
trap - EXIT HUP INT TERM

printf 'installed development build to %s\n' "$target"
printf 'run %s from a project directory to start NAC\n' "$binary_name"
