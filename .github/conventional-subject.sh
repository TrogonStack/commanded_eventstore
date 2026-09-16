#!/usr/bin/env bash
set -euo pipefail

status=0

while IFS= read -r subject; do
  [ -n "$subject" ] || continue

  if ! printf '%s' "$subject" | grep -qE '^(chore|feat|fix)(\([a-z0-9._-]+\))?!?: .+'; then
    echo "::error::\"$subject\" must start with chore:, feat: or fix:, optionally scoped. Squash merging takes the pull request title as the commit subject, and release-please only recognises those three types."
    status=1
  fi
done

exit $status
