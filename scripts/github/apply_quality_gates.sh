#!/usr/bin/env bash
set -euo pipefail

repo="${GITHUB_REPOSITORY:-}"
branch="${GITHUB_BRANCH:-main}"
pr_required_check="${GITHUB_PR_REQUIRED_CHECK:-pr-quality}"
main_required_check="${GITHUB_MAIN_REQUIRED_CHECK:-main-quality}"
ruleset_name="${GITHUB_RULESET_NAME:-main merge queue}"

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
    "checks": [{"context": "${pr_required_check}", "app_id": -1}]
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

ruleset_id="$(
  gh api "repos/${repo}/rulesets" \
    --jq ".[] | select(.name == \"${ruleset_name}\") | .id" \
    | head -n 1
)"

ruleset_payload="$(
  cat <<JSON
{
  "name": "${ruleset_name}",
  "target": "branch",
  "enforcement": "active",
  "conditions": {
    "ref_name": {
      "include": ["refs/heads/${branch}"],
      "exclude": []
    }
  },
  "rules": [
    {"type": "deletion"},
    {"type": "non_fast_forward"},
    {"type": "required_linear_history"},
    {
      "type": "pull_request",
      "parameters": {
        "required_approving_review_count": 0,
        "dismiss_stale_reviews_on_push": false,
        "require_code_owner_review": false,
        "require_last_push_approval": false,
        "required_review_thread_resolution": true,
        "allowed_merge_methods": ["squash"]
      }
    },
    {
      "type": "required_status_checks",
      "parameters": {
        "strict_required_status_checks_policy": true,
        "do_not_enforce_on_create": false,
        "required_status_checks": [
          {"context": "${main_required_check}"}
        ]
      }
    },
    {
      "type": "merge_queue",
      "parameters": {
        "merge_method": "SQUASH",
        "max_entries_to_build": 5,
        "max_entries_to_merge": 5,
        "min_entries_to_merge": 1,
        "min_entries_to_merge_wait_minutes": 0,
        "grouping_strategy": "ALLGREEN",
        "check_response_timeout_minutes": 60
      }
    }
  ],
  "bypass_actors": []
}
JSON
)"

if [[ -n "${ruleset_id}" ]]; then
  gh api \
    --method PUT \
    "repos/${repo}/rulesets/${ruleset_id}" \
    --input - <<<"${ruleset_payload}"
else
  gh api \
    --method POST \
    "repos/${repo}/rulesets" \
    --input - <<<"${ruleset_payload}"
fi

echo "Configured squash-only merges, automatic branch deletion, pr-quality branch protection, and main-quality merge queue."
