#!/bin/bash -e
# Exits with a non-zero status and error message if there are untracked or
# uncommitted changes.
UNTRACKED=`git ls-files --others --exclude-standard`
UNCOMMITTED=`git diff --name-only`
if [ "$UNTRACKED" -o "$UNCOMMITTED" ]
then
  echo "You have untracked or uncommitted files: $UNTRACKED $UNCOMMITTED"
  exit 1
fi
