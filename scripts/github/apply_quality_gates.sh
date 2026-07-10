#!/usr/bin/env bash
set -euo pipefail

repo="${GITHUB_REPOSITORY:-}"
branch="${GITHUB_BRANCH:-main}"
required_check="${GITHUB_REQUIRED_CHECK:-quality}"

if [[ -z "${repo}" ]]; then
  repo="$(gh repo view --json nameWithOwner --jq '.nameWithOwner')"
fi

if [[ -z "${repo}" ]]; then
  echo "Could not determine GitHub repository. Set GITHUB_REPOSITORY=owner/name." >&2
  exit 1
fi

echo "Configuring GitHub quality gates for ${repo}:${branch}"

gh api \
  --method PATCH \
  "repos/${repo}" \
  --field allow_squash_merge=true \
  --field allow_merge_commit=false \
  --field allow_rebase_merge=false \
  --field delete_branch_on_merge=true \
  --field squash_merge_commit_title=PR_TITLE \
  --field squash_merge_commit_message=PR_BODY

gh api \
  --method PUT \
  "repos/${repo}/branches/${branch}/protection" \
  --input - <<JSON
{
  "required_status_checks": {
    "strict": true,
    "contexts": ["${required_check}"]
  },
  "enforce_admins": false,
  "required_pull_request_reviews": null,
  "restrictions": null,
  "required_linear_history": true,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "block_creations": false,
  "required_conversation_resolution": true,
  "lock_branch": false,
  "allow_fork_syncing": true
}
JSON

echo "Configured squash-only merges, automatic branch deletion, required PR checks, linear history, and conversation resolution."
