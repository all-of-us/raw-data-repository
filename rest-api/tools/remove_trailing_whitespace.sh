#!/bin/bash
# Removes trailing whitespace from all files and commits.
MESSAGE="Automatically removed trailing whitespace."

git ls-files | grep "\." | grep -v "\.csv" | parallel sed -i "'s/[ \t]*$//'"

UNCOMMITTED=`git diff --name-only`
if [ "$UNCOMMITTED" ]
then
  git diff
  echo "I removed the trailing whitespace above, auto-committing."
  git commit -a -m "$MESSAGE"
fi
