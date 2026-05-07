#!/bin/sh
STAGED=$(git diff --cached --name-only --diff-filter=ACMR | grep -E '\.(ex|exs)$' || true)
[ -z "$STAGED" ] && exit 0

BEFORE=$(git stash list | wc -l)
git stash push --keep-index -q 2>/dev/null || true
AFTER=$(git stash list | wc -l)

mix format --check-formatted $STAGED
RESULT=$?

[ "$AFTER" -gt "$BEFORE" ] && git stash pop -q 2>/dev/null || true
exit $RESULT
