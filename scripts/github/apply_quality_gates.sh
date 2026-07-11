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
  --field squash_merge_commit_message=PR_BODY \
  --silent

gh api \
  --method PUT \
  "repos/${repo}/branches/${branch}/protection" \
  --input - \
  --silent <<JSON
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

repo_owner="${repo%%/*}"
repo_name="${repo#*/}"
repo_node_id="$(
  gh api graphql \
    -f owner="${repo_owner}" \
    -f name="${repo_name}" \
    -f query='query($owner: String!, $name: String!) { repository(owner: $owner, name: $name) { id } }' \
    --jq '.data.repository.id'
)"
existing_ruleset_id="$(
  gh api graphql \
    -f owner="${repo_owner}" \
    -f name="${repo_name}" \
    -f query='query($owner: String!, $name: String!) {
      repository(owner: $owner, name: $name) {
        rulesets(first: 100) {
          nodes { id name }
        }
      }
    }' \
    --jq ".data.repository.rulesets.nodes[] | select(.name == \"${ruleset_name}\") | .id" \
    | head -n 1
)"

if [[ -n "${existing_ruleset_id}" ]]; then
  echo "Merge-queue ruleset '${ruleset_name}' already exists; leaving it unchanged."
  echo "Configured squash-only merges, automatic branch deletion, and pr-quality branch protection."
  exit 0
fi

merge_queue_result="$(
  gh api graphql \
    -f sourceId="${repo_node_id}" \
    -f rulesetName="${ruleset_name}" \
    -f branchRef="refs/heads/${branch}" \
    -f mainCheck="${main_required_check}" \
    -f query='mutation($sourceId: ID!, $rulesetName: String!, $branchRef: String!, $mainCheck: String!) {
      createRepositoryRuleset(input: {
        sourceId: $sourceId
        name: $rulesetName
        target: BRANCH
        enforcement: ACTIVE
        conditions: { refName: { include: [$branchRef], exclude: [] } }
        bypassActors: []
        rules: [
          { type: DELETION }
          { type: NON_FAST_FORWARD }
          { type: REQUIRED_LINEAR_HISTORY }
          { type: PULL_REQUEST, parameters: { pullRequest: {
            requiredApprovingReviewCount: 0
            dismissStaleReviewsOnPush: false
            requireCodeOwnerReview: false
            requireLastPushApproval: false
            requiredReviewThreadResolution: true
            allowedMergeMethods: [SQUASH]
          } } }
          { type: REQUIRED_STATUS_CHECKS, parameters: { requiredStatusChecks: {
            strictRequiredStatusChecksPolicy: true
            doNotEnforceOnCreate: false
            requiredStatusChecks: [{ context: $mainCheck }]
          } } }
          { type: MERGE_QUEUE, parameters: { mergeQueue: {
            mergeMethod: SQUASH
            maxEntriesToBuild: 5
            maxEntriesToMerge: 5
            minEntriesToMerge: 1
            minEntriesToMergeWaitMinutes: 0
            groupingStrategy: ALLGREEN
            checkResponseTimeoutMinutes: 60
          } } }
        ]
      }) { ruleset { databaseId name } }
    }' 2>&1
)" || {
  echo "Warning: GitHub rejected merge-queue ruleset '${ruleset_name}'." >&2
  echo "${merge_queue_result}" >&2
  echo "Branch protection still requires ${pr_required_check}; ${main_required_check} runs on pushes to ${branch}." >&2
  echo "Enable merge queue in the GitHub UI if this repository plan or account does not allow API setup." >&2
  exit 0
}

echo "${merge_queue_result}"
echo "Configured squash-only merges, automatic branch deletion, pr-quality branch protection, and main-quality merge queue."
