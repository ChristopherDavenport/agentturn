#!/usr/bin/env bash
# Prints the CHANGELOG.md section for a version, with the heading
# replaced by the bare version, for use as an annotated tag message. The
# release workflow publishes that message as the GitHub release notes,
# so every tag of one release carries the same section.
#
#   git tag -a v0.1.0 -m "$(scripts/release-notes.sh v0.1.0)"

set -euo pipefail

VERSION="${1:-}"
[ -n "$VERSION" ] || { echo "usage: $0 vX.Y.Z" >&2; exit 1; }

notes="$(awk -v v="$VERSION" '/^## / { p = ($2 == v) } p' CHANGELOG.md \
  | sed "1s/.*/$VERSION/")"

[ -n "$notes" ] \
  || { echo "release-notes: CHANGELOG.md has no $VERSION section" >&2; exit 1; }

printf '%s\n' "$notes"
