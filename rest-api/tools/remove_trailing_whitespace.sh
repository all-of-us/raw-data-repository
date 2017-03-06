#!/bin/bash
# Removes trailing whitespace from all files and optionally commits changes.
MESSAGE="Remove trailing whitespace."

git ls-files | grep '.py$' | parallel sed -i "'s/[ \t]*$//'"

UNCOMMITTED=`git diff --name-only`
if [ "$UNCOMMITTED" ]
then
  git diff
  echo "I removed some trailing whitespace. Shall I commit the changes as \"$MESSAGE\"?"
  select yn in "Yes" "No"
  do
    case $yn in
      Yes ) git commit -a -m "$MESSAGE";;
      No  ) break;;
    esac
  done
fi
