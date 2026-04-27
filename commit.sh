#!/usr/bin/env bash
# FILE: commit.sh
# DATE: 2026-04-27
# PURPOSE: Commit repo (whole or one directory), push current branch, then pull it inside /app in mailer-tools.

set -euo pipefail

usage() {
  echo "Usage: $0 [directory]" >&2
}

if [[ $# -gt 1 ]]; then
  usage
  exit 1
fi

repo_root="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [[ -z "$repo_root" ]]; then
  echo "ERROR: run this script inside a git repository" >&2
  exit 1
fi

cd "$repo_root"

scope_mode="all"
target_rel=""

if [[ $# -eq 1 ]]; then
  scope_mode="dir"
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
fi

branch="$(git branch --show-current)"
if [[ -z "$branch" ]]; then
  echo "ERROR: current git branch is not available" >&2
  exit 1
fi

collect_changed_files() {
  if [[ "$scope_mode" == "all" ]]; then
    git diff --cached --name-only | sort
  else
    git diff --cached --name-only -- "$target_rel" | sort
  fi
}

build_change_summary() {
  local tmp_file="$1"
  awk -F'/' '
  {
    if ($0 == "") next
    file = $NF
    dir = "."
    if (NF > 1) {
      dir = $1
      for (i = 2; i < NF; i++) {
        dir = dir "/" $i
      }
    }
    if (dir == ".") dir = "root"

    if (!(dir in seen)) {
      order[++n] = dir
      seen[dir] = 1
    }

    if (files[dir] == "") files[dir] = file
    else files[dir] = files[dir] "," file
  }
  END {
    if (n == 0) {
      print "none: none"
    } else {
      for (i = 1; i <= n; i++) {
        d = order[i]
        printf "%s: %s", d, files[d]
        if (i < n) printf "; "
      }
      printf "\n"
    }
  }
  ' "$tmp_file"
}

if [[ "$scope_mode" == "all" ]]; then
  echo "Adding changes from: whole repository"
  git add -A
  if git diff --cached --quiet; then
    echo "No changes to commit in: whole repository"
    exit 0
  fi
else
  echo "Adding changes from: $target_rel"
  git add -A -- "$target_rel"
  if git diff --cached --quiet -- "$target_rel"; then
    echo "No changes to commit in: $target_rel"
    exit 0
  fi
fi

tmp_changed="$(mktemp)"
trap 'rm -f "$tmp_changed"' EXIT
collect_changed_files > "$tmp_changed"

change_summary="$(build_change_summary "$tmp_changed")"
stamp="$(date '+%Y-%m-%d %H:%M:%S')"
commit_message="Revision ${stamp}, <${change_summary}>"

echo "Committing: $commit_message"
if [[ "$scope_mode" == "all" ]]; then
  git commit -m "$commit_message"
else
  git commit -m "$commit_message" -- "$target_rel"
fi

echo "Pushing branch: $branch"
git push origin "$branch"

echo "Pulling inside mailer-tools:/app"
docker exec mailer-tools git -C /app pull --ff-only origin "$branch"

echo "Done"
