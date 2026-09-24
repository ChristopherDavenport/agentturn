#!/usr/bin/env bash
# Sets or asserts the first-party requirements of the nested modules.
#
#   scripts/versions.sh set   v0.1.0 front/a2a tools/a2a session
#   scripts/versions.sh check v0.1.0 front/a2a tools/a2a session
#
# Every module in this repository is released at one version, from one
# commit, and requires its first-party siblings at exactly that version.
# `set` writes that; `check` refuses to let a release proceed unless it
# holds — `go mod tidy` is free to move a requirement that `set` just
# wrote, so what actually landed has to be read back.
#
# Only requirements already present are touched. A module that does not
# use a sibling does not acquire one.

set -euo pipefail

MODULE_ROOT="github.com/ChristopherDavenport/agentturn"

die() { echo "versions: $*" >&2; exit 1; }

MODE="${1:-}"; shift || true
VERSION="${1:-}"; shift || true

case "$MODE" in set|check) ;; *) die "usage: $0 set|check <version> <module-dir>..." ;; esac
[ -n "$VERSION" ] || die "usage: $0 $MODE <version> <module-dir>..."
[ $# -gt 0 ] || die "usage: $0 $MODE $VERSION <module-dir>..."

# The first-party module paths a go.mod requires, direct or indirect, in
# either the block or the single-line spelling. A module never requires
# itself, so this cannot produce a self-reference.
requires() {
  awk -v p="$MODULE_ROOT" '
    /^require[[:space:]]*\(/ { blk = 1; next }
    blk && /^\)/             { blk = 0; next }
    blk && index($1, p) == 1 { print $1; next }
    /^require[[:space:]]/ && index($2, p) == 1 { print $2 }
  ' "$1" | sort -u
}

# The version a go.mod requires a given path at.
required_at() {
  awk -v d="$2" '
    /^require[[:space:]]*\(/ { blk = 1; next }
    blk && /^\)/             { blk = 0; next }
    blk && $1 == d { print $2; exit }
    /^require[[:space:]]/ && $2 == d { print $3; exit }
  ' "$1"
}

status=0

for m in "$@"; do
  gomod="$m/go.mod"
  [ -f "$gomod" ] || die "no such module: $gomod"

  while read -r path; do
    [ -n "$path" ] || continue
    case "$MODE" in
      set)
        (cd "$m" && go mod edit -require="$path@$VERSION")
        ;;
      check)
        got="$(required_at "$gomod" "$path")"
        if [ "$got" != "$VERSION" ]; then
          echo "$gomod requires $path at $got, not $VERSION" >&2
          status=1
        fi
        ;;
    esac
  done <<EOF
$(requires "$gomod")
EOF
done

[ "$status" -eq 0 ] || echo "versions: every first-party require must name the version being released (see CLAUDE.md)" >&2
exit $status
