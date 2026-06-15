#!/usr/bin/env bash
# Fetch GitHub Copilot's review comments on a pull request.
#
# Why this exists: Copilot posts its review a couple of minutes AFTER a PR is
# opened, so merging as soon as CI goes green routinely lands before the review
# arrives and its comments get missed. Run this before merging (with --wait), or
# as a follow-up sweep, so Copilot feedback is never silently skipped.
#
# Usage:
#   scripts/gh-copilot-comments.sh [PR]         # current branch's PR if PR omitted
#   scripts/gh-copilot-comments.sh --wait [PR]  # poll up to ~3 min for Copilot's review
#   scripts/gh-copilot-comments.sh --all        # sweep every PR (open + closed)
#
# Requires the gh CLI, authenticated.
set -euo pipefail

# Fail fast if gh can't talk to the API — otherwise a "(none)" result could just
# mean "not logged in", silently hiding the very feedback this script exists to
# surface.
if ! gh auth status >/dev/null 2>&1; then
  echo "error: gh CLI is not authenticated — run 'gh auth login'" >&2
  exit 1
fi

repo=$(gh repo view --json nameWithOwner --jq .nameWithOwner)

# Inline review comments left by Copilot on a PR: "path:line: body". Real API
# errors (rate limit, permissions, bad repo) are surfaced on stderr; a failure
# warns and returns empty so an --all sweep keeps going rather than aborting.
copilot_comments() {
  local body
  if ! body=$(gh api "repos/$repo/pulls/$1/comments" --paginate \
      --jq '.[] | select(.user.login | test("copilot"; "i"))
            | "  • \(.path):\(.line // .original_line // 0): \(.body | gsub("\n"; " "))"'); then
    echo "warning: failed to fetch comments for PR #$1" >&2
    return 0
  fi
  printf '%s' "$body"
}

copilot_reviewed() {
  gh api "repos/$repo/pulls/$1/reviews" --jq '.[].user.login' 2>/dev/null | grep -qi copilot
}

case "${1:-}" in
  --all)
    for pr in $(gh pr list --state all --limit 200 --json number --jq '.[].number'); do
      out=$(copilot_comments "$pr")
      [ -n "$out" ] && { echo "===== PR #$pr ====="; echo "$out"; }
    done
    ;;
  --wait)
    shift
    pr="${1:-$(gh pr view --json number --jq .number)}"
    echo "Waiting for Copilot's review on PR #$pr ..."
    for _ in $(seq 1 18); do
      copilot_reviewed "$pr" && break
      sleep 10
    done
    out=$(copilot_comments "$pr")
    echo "Copilot comments on PR #$pr:"
    [ -n "$out" ] && echo "$out" || echo "  (none — Copilot left no inline comments)"
    ;;
  *)
    pr="${1:-$(gh pr view --json number --jq .number)}"
    out=$(copilot_comments "$pr")
    echo "Copilot comments on PR #$pr:"
    [ -n "$out" ] && echo "$out" || echo "  (none yet — Copilot may still be reviewing; try --wait)"
    ;;
esac
