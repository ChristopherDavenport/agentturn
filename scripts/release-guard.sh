#!/usr/bin/env bash
# Checks a tag is safe to push before it is pushed. A pushed tag is
# permanent: the module proxy and the checksum database keep the version
# forever, so a mistake can only be retracted, never withdrawn.
#
#   scripts/release-guard.sh v0.0.7
#   scripts/release-guard.sh front/a2a/v0.0.7
#
# Exits non-zero, with the reason, if the tag would publish a version
# that sorts below one already released, or a module whose go.mod does
# not name the very commit it is tagged from.
#
# A nested tag is only checkable once the root tag of the same version
# exists locally, so run this in the order `make release` does: guard the
# root, tag it, then guard and tag each nested module. Nothing is public
# until the push.

set -euo pipefail

TAG="${1:-}"

die() { echo "release-guard: $*" >&2; exit 1; }
ok()  { echo "  ok  $*"; }

[ -n "$TAG" ] || die "usage: $0 <tag>"

# Newest version wins a version sort; -V orders v0.0.9 before v0.0.10.
newest() { sort -V | tail -1; }

echo "release-guard: $TAG"

# --- Repository state -------------------------------------------------
[ -z "$(git status --porcelain)" ] || die "working tree is dirty; commit or stash first"
ok "working tree clean"

git rev-parse -q --verify "refs/tags/$TAG" >/dev/null \
  && die "tag $TAG already exists locally"
[ -z "$(git ls-remote --tags origin "refs/tags/$TAG")" ] \
  || die "tag $TAG already exists on origin"
ok "tag is new"

# --- Root version already released ------------------------------------
# Nested tags are <dir>/vX.Y.Z, so 'v*' matches root tags only.
ROOT_LATEST="$(git tag -l 'v*' | newest)"
[ -n "$ROOT_LATEST" ] || die "no root tag found; cannot establish the version floor"

case "$TAG" in
  v*)
    # Root tag: must move forward.
    [ "$(printf '%s\n%s\n' "$ROOT_LATEST" "$TAG" | newest)" = "$TAG" ] \
      || die "$TAG does not sort above the current root release $ROOT_LATEST"
    ok "$TAG is newer than $ROOT_LATEST"
    ;;

  */v*)
    DIR="${TAG%/v*}"                 # front/a2a
    VERSION="v${TAG##*/v}"           # v0.0.7
    [ -f "$DIR/go.mod" ] || die "no module at $DIR (expected $DIR/go.mod)"

    # Every published module shares one version line. A nested module
    # numbered below the newest root drags consumers' root module
    # backwards: they select the highest nested version available, and
    # its go.mod then pins the root, through minimal version selection.
    [ "$(printf '%s\n%s\n' "$ROOT_LATEST" "$VERSION" | newest)" = "$VERSION" ] \
      || die "$VERSION sorts below the current root release $ROOT_LATEST;
            a nested version under the root silently downgrades consumers.
            Tag $ROOT_LATEST or later."
    ok "$VERSION is at or above the root release $ROOT_LATEST"

    # And it has to move that module forward too, or the proxy keeps
    # serving the older content under a version nobody can supersede.
    DIR_LATEST="$(git tag -l "$DIR/v*" | sed "s|^$DIR/||" | newest)"
    if [ -n "$DIR_LATEST" ]; then
      [ "$(printf '%s\n%s\n' "$DIR_LATEST" "$VERSION" | newest)" = "$VERSION" ] \
        || die "$VERSION does not sort above $DIR's current release $DIR_LATEST"
      ok "$VERSION is newer than $DIR/$DIR_LATEST"
    fi

    # The whole invariant, in two checks.
    #
    # First: every first-party version this module requires is the
    # version being tagged. A consumer who takes only this module gets
    # exactly those versions, so this is what makes the shared version
    # line mean something rather than being decoration.
    scripts/versions.sh check "$VERSION" "$DIR" \
      || die "$DIR does not require its first-party siblings at $VERSION"
    ok "requires every first-party sibling at $VERSION"

    # Second: the root tag of that version names this very commit. The
    # require above claims this module was built against root $VERSION;
    # this is what makes the claim true rather than merely asserted. It
    # is why there is no build against a published root here — there is
    # no older root in the picture to drift from.
    ROOT_TAG_COMMIT="$(git rev-list -n1 "$VERSION" 2>/dev/null || true)"
    [ -n "$ROOT_TAG_COMMIT" ] \
      || die "$VERSION is not tagged; the root and its nested modules are tagged from one commit,
            so tag the root in the same run (make release does this)"
    [ "$ROOT_TAG_COMMIT" = "$(git rev-parse HEAD)" ] \
      || die "root $VERSION points at $ROOT_TAG_COMMIT, not HEAD;
            $DIR would claim to be built against a root it was not built against"
    ok "root $VERSION is this commit"

    # Cheap proof the replace resolves and the module is buildable as
    # published. The heavy vet and test already ran under make check,
    # against this same code.
    echo "  ..  building $DIR outside the workspace"
    (cd "$DIR" && GOWORK=off go build ./...) \
      || die "$DIR does not build outside the workspace"
    ok "builds outside the workspace"
    ;;

  *)
    die "unrecognised tag shape: $TAG (expected vX.Y.Z or <dir>/vX.Y.Z)"
    ;;
esac

echo "release-guard: $TAG is safe to push"
