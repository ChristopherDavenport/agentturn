#!/usr/bin/env bash
# Builds, vets and tests each nested module the way a consumer gets it:
# extracted from the tree into a directory with no parent go.mod, with
# the in-tree replace directives dropped, so every first-party require is
# answered by the module proxy and not by the directory next door.
#
#   scripts/check-extracted.sh session
#
# This is the only thing here that compiles a nested module against the
# version its own go.mod names. A replace is a property of the main
# module, so a consumer ignores it and gets the require — but while the
# module is only ever built inside this tree the replace answers first
# and that require line is never resolved.
#
# What it catches that check-replaces.sh and release-guard.sh do not:
# those check that a require is present and that it names the version
# being tagged. Both are claims about a version string. Neither compiles
# anything against it. Pin agenttool's mcpclient to agenttool v0.0.1 and
# both stay silent while this fails:
#
#   ./mcp.go:387:33: undefined: agenttool.WithResource
#   ./mcp.go:394:19: undefined: agenttool.NewFunc
#   ./mcp.go:409:44: undefined: agenttool.Annotations
#   ./mcp.go:519:24: undefined: agenttool.ProgressInfo
#
# That reproduction is the whole justification for this script; keep it
# here. openresponses' release-guard.sh did this once, at v0.0.11 —
# "build it the way a consumer does: outside the workspace, against the
# published root rather than the checkout next door", and it ran build,
# vet and test. It worked because the nested go.mod carried no replace
# then. The replace added at v0.0.12 turned that same line into a build
# against the tree, because GOWORK=off stopped meaning "no local root",
# and nothing said so: the line still runs and still prints ok. This
# restores what it used to prove, by dropping the replace in a copy
# rather than removing it from the tree, which the release needs.
#
# Needs the network; the requires resolve from the proxy. A required
# version the proxy cannot serve is reported as that, not as a build
# failure — see the note in release-guard.sh about where that happens.

set -euo pipefail

die() { echo "check-extracted: $*" >&2; exit 1; }

[ $# -gt 0 ] || die "usage: $0 <module-dir>..."

# The module path of each replace whose target is a filesystem path.
# Those are the replaces that cannot survive extraction: the directory
# they name sits outside the module and so is absent from its zip.
# Handles both the block and the single-line spelling.
local_replaces() {
  awk '
    /^replace[[:space:]]*\(/ { blk = 1; next }
    blk && /^\)/             { blk = 0; next }
    blk && /=>/              { if ($3 ~ /^[.\/]/) print $1; next }
    /^replace[[:space:]]/ && /=>/ { if ($4 ~ /^[.\/]/) print $2 }
  ' "$1"
}

# The version a go.mod requires a given module path at.
required_version() {
  awk -v d="$2" '
    /^require[[:space:]]*\(/ { blk = 1; next }
    blk && /^\)/             { blk = 0; next }
    blk && $1 == d { print $2; exit }
    /^require[[:space:]]/ && $2 == d { print $3; exit }
  ' "$1"
}

# One parent for every copy. mktemp -d lands outside any module, which is
# the whole point: a relative replace left in place would otherwise be
# satisfied by accident from somewhere up the tree.
tmproot="$(mktemp -d)"
trap 'rm -rf "$tmproot"' EXIT

status=0

for m in "$@"; do
  [ -f "$m/go.mod" ] || die "no such module: $m/go.mod"

  echo "check-extracted: $m"
  dropped="$(local_replaces "$m/go.mod" | tr '\n' ' ')"

  # Every version about to be resolved from the proxy has to be there.
  # It is not there during make release, between the version bump and
  # the push on the last line, which is why release-guard.sh asks this
  # question itself before calling here. Reported separately because it
  # is a fact about the proxy, not about the code.
  unpublished=""
  for p in $dropped; do
    v="$(required_version "$m/go.mod" "$p")"
    [ -n "$v" ] || die "$m/go.mod replaces $p without requiring it"
    err="$(GOWORK=off go list -m -e -f '{{with .Error}}{{.Err}}{{end}}' "$p@$v")"
    [ -z "$err" ] || unpublished="$unpublished
    $err"
  done
  if [ -n "$unpublished" ]; then
    echo "$m requires a version the proxy does not serve:$unpublished" >&2
    echo "  Nothing can be resolved the way a consumer would until it is" >&2
    echo "  published. During make release that is expected between the bump" >&2
    echo "  and the push; anywhere else it is a require that names a version" >&2
    echo "  nobody can fetch." >&2
    status=1
    continue
  fi

  dest="$tmproot/${m//\//-}"
  mkdir -p "$dest"
  cp -R "$m"/. "$dest"/

  # Each step exits explicitly: errexit does not apply inside a subshell
  # whose status is being tested, which this one's is.
  if (
    cd "$dest" || exit 1
    export GOWORK=off
    for p in $dropped; do
      go mod edit -dropreplace="$p" || exit 1
    done
    # A module replaced with a directory has no hash in the tree's
    # go.sum, so the zip has to be fetched before the build can verify
    # it. This is not a tidy: nothing here changes a version.
    if [ -n "$dropped" ]; then
      go mod download $dropped || exit 1
    fi
    # Build, vet and test, which is what the v0.0.11 check ran before
    # the replace hollowed it out. Named one by one, because "it does not
    # build extracted" and "its tests cannot run from its own zip" are
    # different problems with different owners.
    go build ./... || { echo "  !!  go build failed" >&2; exit 1; }
    go vet ./...   || { echo "  !!  go vet failed" >&2; exit 1; }
    go test ./...  || { echo "  !!  go test failed" >&2; exit 1; }
  ); then
    echo "  ok  $m builds, vets and tests against the versions it requires"
  else
    echo "$m does not build outside the tree against the versions its go.mod" >&2
    echo "  requires. In-tree the replace answers first, so nothing else here" >&2
    echo "  reads those require lines; a consumer reads nothing else." >&2
    status=1
  fi
done

exit $status
