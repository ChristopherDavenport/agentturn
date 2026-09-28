#!/usr/bin/env bash
# Lands a stack of pull requests on main, bottom first, each retargeted
# to main before it is merged.
#
#   scripts/merge-stack.sh [--dry-run] [--allow-extracted] <pr> [<pr>...]
#
# A stacked PR is based on the branch of the PR below it. Merging it
# into that base and then deleting the base strands its commits: the
# v0.0.6 stack (#57–#60) was landed that way and #61 had to re-land
# four commits by hand. This script never merges into a stacked base.
# For each PR in the order given it checks that the PR is open and that
# its head contains the head of the PR before it, retargets it to main,
# waits for GitHub to recompute its mergeability against main, checks
# its CI, merges it with a merge commit at the head commit it checked,
# and confirms that head is now an ancestor of origin/main. Branches
# are deleted only once every PR has landed and been verified, and
# local main is fast-forwarded and checked at the end.
#
# --dry-run prints what would be done and changes nothing.
#
# --allow-extracted lets the "Nested modules build extracted" job, and
# the "Checks" job that aggregates it, be red. That job builds each
# nested module against the root version its go.mod requires, which is
# the last release; a PR whose nested module uses root API this stack
# adds cannot pass it until the root is tagged, and passes on main once
# the release is cut. Every other job must be green.

set -euo pipefail

die() { echo "merge-stack: $*" >&2; exit 1; }
say() { echo "merge-stack: $*"; }

DRY=0
ALLOW_EXTRACTED=0
PRS=()
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY=1 ;;
    --allow-extracted) ALLOW_EXTRACTED=1 ;;
    --*) die "unknown flag $arg" ;;
    *) PRS+=("$arg") ;;
  esac
done
[ ${#PRS[@]} -gt 0 ] || die "usage: $0 [--dry-run] [--allow-extracted] <pr> [<pr>...]"

REPO=$(gh repo view --json nameWithOwner --jq .nameWithOwner)
DEFAULT=$(gh repo view --json defaultBranchRef --jq .defaultBranchRef.name)
say "repository $REPO, default branch $DEFAULT"

run() {
  if [ "$DRY" = 1 ]; then echo "  would: $*"; else echo "  run:   $*"; "$@"; fi
}

# pr_field <pr> <jq expression>
pr_field() {
  gh pr view "$1" --json number,state,baseRefName,headRefName,headRefOid,mergeable,mergeStateStatus,statusCheckRollup --jq "$2"
}

# checks_ok <pr>: every CI job succeeded, or only the extracted job and
# its aggregate failed and that was allowed.
checks_ok() {
  local bad rest
  bad=$(pr_field "$1" '[.statusCheckRollup[]? | select((.conclusion // .state) != "SUCCESS") | (.name // .context)] | join(",")')
  [ -z "$bad" ] && return 0
  if [ "$ALLOW_EXTRACTED" = 1 ]; then
    rest=$(echo "$bad" | tr ',' '\n' | grep -vxE 'Nested modules build extracted|Checks' || true)
    if [ -z "$rest" ]; then
      say "  extracted job red and allowed on #$1: $bad"
      return 0
    fi
    bad=$rest
  fi
  echo "  !!  CI not green on #$1: $bad" >&2
  return 1
}

git fetch --quiet origin "$DEFAULT"
PREV_HEAD=""
PREV_BRANCH=""
MERGED_BRANCHES=()

for PR in "${PRS[@]}"; do
  say "#$PR"
  STATE=$(pr_field "$PR" .state)
  [ "$STATE" = OPEN ] || die "#$PR is $STATE, not open"
  BASE=$(pr_field "$PR" .baseRefName)
  HEAD=$(pr_field "$PR" .headRefName)
  SHA=$(pr_field "$PR" .headRefOid)
  say "  $HEAD ($SHA) based on $BASE"

  # The stack must be coherent: the base is main or the previous PR's
  # branch, and the head contains the previous head, so retargeting to
  # main changes nothing about what lands.
  if [ -n "$PREV_BRANCH" ]; then
    [ "$BASE" = "$PREV_BRANCH" ] || [ "$BASE" = "$DEFAULT" ] || die "#$PR is based on $BASE, not on $PREV_BRANCH or $DEFAULT: not the next PR of this stack"
    git fetch --quiet origin "$HEAD"
    git merge-base --is-ancestor "$PREV_HEAD" "$SHA" || die "#$PR's head does not contain the previous PR's head $PREV_HEAD: rebase it first"
  else
    [ "$BASE" = "$DEFAULT" ] || die "#$PR, the bottom of the stack, is based on $BASE rather than $DEFAULT"
  fi

  if [ "$BASE" != "$DEFAULT" ]; then
    run gh pr edit "$PR" --base "$DEFAULT"
  fi

  if [ "$DRY" = 0 ]; then
    # GitHub recomputes mergeability after a retarget; wait for it.
    M=UNKNOWN
    for _ in $(seq 1 30); do
      M=$(pr_field "$PR" .mergeable)
      [ "$M" = UNKNOWN ] || break
      sleep 2
    done
    [ "$M" = MERGEABLE ] || die "#$PR is $M against $DEFAULT"
    MS=$(pr_field "$PR" .mergeStateStatus)
    case "$MS" in CLEAN|UNSTABLE|HAS_HOOKS) ;; *) die "#$PR merge state is $MS" ;; esac
  fi
  checks_ok "$PR" || die "fix CI on #$PR, or pass --allow-extracted if only the extracted job is red"

  run gh pr merge "$PR" --merge --match-head-commit "$SHA"

  if [ "$DRY" = 0 ]; then
    git fetch --quiet origin "$DEFAULT"
    git merge-base --is-ancestor "$SHA" "origin/$DEFAULT" || die "#$PR merged but $SHA is not on origin/$DEFAULT: stop and look"
    say "  landed: $SHA is on origin/$DEFAULT"
  fi
  MERGED_BRANCHES+=("$HEAD")
  PREV_HEAD="$SHA"
  PREV_BRANCH="$HEAD"
done

say "every PR landed; deleting the stack's branches"
for b in "${MERGED_BRANCHES[@]}"; do
  run git push --quiet origin --delete "$b"
  if git show-ref --verify --quiet "refs/heads/$b"; then
    run git branch -D "$b"
  fi
done

if [ "$DRY" = 0 ]; then
  git checkout --quiet "$DEFAULT"
  git pull --quiet --ff-only origin "$DEFAULT"
  say "$DEFAULT is at $(git rev-parse --short HEAD); running make check"
  make check
fi
say "done"
