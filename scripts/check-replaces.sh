#!/usr/bin/env bash
# Asserts that every first-party module a nested module requires is also
# replaced with a path that exists.
#
#   scripts/check-replaces.sh front/a2a tools/a2a session
#
# This is load-bearing, not cosmetic. Every module here is released at
# one version from one commit and requires its siblings at exactly that
# version — a version that does not exist on the proxy until the tag is
# pushed. There is no go.work here: the replace is the only thing
# that builds the tree against itself, and the only thing that lets the
# release commit resolve, tidy and build. Lose one and the
# next release fails at `make tidy`, or silently pins that module to the
# previous release.
#
# Consumers ignore a replace in a dependency and get the require, which
# names the commit the module was tagged from. release-guard is what
# proves that correspondence.

set -euo pipefail

MODULE_ROOT="github.com/ChristopherDavenport/agentturn"

die() { echo "check-replaces: $*" >&2; exit 1; }

[ $# -gt 0 ] || die "usage: $0 <module-dir>..."

# The first-party module paths a go.mod requires, direct or indirect,
# in either the block or the single-line spelling.
requires() {
  awk -v p="$MODULE_ROOT" '
    /^require[[:space:]]*\(/ { blk = 1; next }
    blk && /^\)/             { blk = 0; next }
    blk && index($1, p) == 1 { print $1; next }
    /^require[[:space:]]/ && index($2, p) == 1 { print $2 }
  ' "$1" | sort -u
}

# "<old> <new>" per replace directive, in either spelling.
replaces() {
  awk '
    /^replace[[:space:]]*\(/ { blk = 1; next }
    blk && /^\)/             { blk = 0; next }
    blk && /=>/              { print $1, $3; next }
    /^replace[[:space:]]/ && /=>/ { print $2, $4 }
  ' "$1"
}

status=0

for m in "$@"; do
  gomod="$m/go.mod"
  [ -f "$gomod" ] || die "no such module: $gomod"

  reps="$(replaces "$gomod")"

  while read -r path; do
    [ -n "$path" ] || continue
    if ! printf '%s\n' "$reps" | awk -v d="$path" '$1 == d { found = 1 } END { exit !found }'; then
      echo "$gomod requires $path without replacing it;" >&2
      echo "  the next release would resolve it from the proxy, where the version" >&2
      echo "  being released does not exist yet (see CLAUDE.md)" >&2
      status=1
    fi
  done <<EOF
$(requires "$gomod")
EOF

  while read -r old new; do
    [ -n "$new" ] || continue
    case "$new" in
      /* | ./* | ../*)
        [ -d "$m/$new" ] || { echo "$gomod replaces $old with $new, which is not a directory" >&2; status=1; } ;;
    esac
  done <<EOF
$reps
EOF
done

exit $status
