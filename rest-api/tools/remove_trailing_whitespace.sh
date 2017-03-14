#!/bin/bash
# Removes trailing whitespace from all files and suggests a commit.
MESSAGE="Remove trailing whitespace."

git ls-files | grep "\." | parallel sed -i "'s/[ \t]*$//'"

UNCOMMITTED=`git diff --name-only`
if [ "$UNCOMMITTED" ]
then
  git diff
  echo "I removed the trailing whitespace above. Suggestion: git commit -a -m \"$MESSAGE\""
  exit 1
fi
