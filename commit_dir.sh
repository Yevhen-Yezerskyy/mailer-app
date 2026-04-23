#!/usr/bin/env bash
# FILE: commit_dir.sh
# DATE: 2026-04-07
# PURPOSE: Commit one repo directory, push the current branch, then pull it inside /app in mailer-tools.

set -euo pipefail

usage() {
  echo "Usage: $0 <directory>" >&2
}

if [[ $# -ne 1 ]]; then
  usage
  exit 1
fi

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "$repo_root" ]]; then
  echo "ERROR: run this script inside a git repository" >&2
  exit 1
fi

cd "$repo_root"

target_input="${1%/}"
target_input="${target_input#./}"
if [[ -z "$target_input" || "$target_input" == "." ]]; then
  echo "ERROR: directory argument is empty" >&2
  exit 1
fi

target_abs="$(realpath -m "$target_input")"
repo_abs="$(realpath -m "$repo_root")"

case "$target_abs" in
  "$repo_abs"/*) ;;
  *)
    echo "ERROR: directory must be inside the repository" >&2
    exit 1
    ;;
esac

target_rel="${target_abs#$repo_abs/}"

if [[ ! -d "$target_rel" ]]; then
  echo "ERROR: directory not found: $target_rel" >&2
  exit 1
fi

branch="$(git branch --show-current)"
if [[ -z "$branch" ]]; then
  echo "ERROR: current git branch is not available" >&2
  exit 1
fi

commit_message="${target_rel} доделка"

echo "Adding changes from: $target_rel"
git add -A -- "$target_rel"

if git diff --cached --quiet -- "$target_rel"; then
  echo "No changes to commit in: $target_rel"
  exit 0
fi

echo "Committing: $commit_message"
git commit -m "$commit_message" -- "$target_rel"

echo "Pushing branch: $branch"
git push origin "$branch"

echo "Pulling inside mailer-tools:/app"
docker exec mailer-tools git -C /app pull --ff-only origin "$branch"

echo "Done"
