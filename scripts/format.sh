#!/usr/bin/env bash
# Format (or check) every C++ source file with a pinned clang-format.
#
# The version is pinned in .clang-format-version and installed from PyPI into a
# local virtualenv. This matters more than it looks: clang-format's output
# changes between major versions, so a developer on Homebrew's clang-format and
# a CI job on the distro's will disagree forever about a file neither of them
# touched. Pinning one version, installed the same way everywhere, makes the
# check reproducible.
#
#   scripts/format.sh          rewrite files in place
#   scripts/format.sh --check  fail if anything is unformatted (what CI runs)
set -euo pipefail

cd "$(dirname "$0")/.."
VERSION="$(cat .clang-format-version)"
VENV=".venv-format"

if [[ ! -x "$VENV/bin/clang-format" ]]; then
  echo "installing clang-format==$VERSION into $VENV"
  python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet --disable-pip-version-check "clang-format==$VERSION"
fi

ACTUAL="$("$VENV/bin/clang-format" --version)"
case "$ACTUAL" in
  *"$VERSION"*) ;;
  *) echo "error: expected clang-format $VERSION, got: $ACTUAL" >&2; exit 1 ;;
esac

# `git ls-files` into a temp file rather than `mapfile`: macOS ships bash 3.2,
# which has neither mapfile nor readarray, and this script has to run on a
# developer's Mac as well as on a Linux CI runner.
FILE_LIST="$(mktemp)"
trap 'rm -f "$FILE_LIST"' EXIT
git ls-files '*.cc' '*.h' > "$FILE_LIST"

COUNT="$(wc -l < "$FILE_LIST" | tr -d ' ')"
if [[ "$COUNT" -eq 0 ]]; then
  echo "no source files found" >&2
  exit 1
fi

if [[ "${1:-}" == "--check" ]]; then
  echo "checking $COUNT files with $ACTUAL"
  xargs "$VENV/bin/clang-format" --dry-run --Werror < "$FILE_LIST"
  echo "all files are correctly formatted"
else
  echo "formatting $COUNT files with $ACTUAL"
  xargs "$VENV/bin/clang-format" -i < "$FILE_LIST"
  echo "done"
fi
