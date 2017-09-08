#!/bin/bash
# Run tests which are fast enough to be run before very push.

set -e

echo "Grepping for checked-in credentials..."
set +e  # OK if grep does not find any matches.
KEY_FILES=`git grep -il "BEGIN PRIVATE KEY" . | grep -v $0 | grep -v oauth2client`
set -e
if [ "${KEY_FILES}" ]
then
  echo "No keys may be checked in, but found: $KEY_FILES"
  exit 1
fi
echo "No keys found!"

# Pylint checks. Use pylint --list-msgs to see more available messages.
# More options are set in rest-api/pylintrc.
PYLINT_VERSION=`pylint --version | head -1 | sed 's/pylint \([0-9.]*\),/\1/g'`
echo "`date -u` Linting with pylint ${PYLINT_VERSION}..."
ENABLE_FOR_TESTS="\
  --enable=bad-indentation,broad-except,bare-except,logging-too-many-args \
  --enable=unused-argument,redefined-outer-name,redefined-builtin,superfluous-parens \
  --enable=trailing-whitespace,unused-import,unused-variable,undefined-variable"
ENABLE_FOR_ALL="$ENABLE_FOR_TESTS --enable=bad-whitespace,line-too-long,unused-import,unused-variable"
PYLINT_OPTS="-r n --disable=all --score=n"
echo "`date -u` Linting application files..."
FILES_NON_TEST=`git ls-files | grep '.py$' | grep -v -e 'alembic/versions/' -e '_test'`
pylint $PYLINT_OPTS $ENABLE_FOR_ALL $FILES_NON_TEST
echo "`date -u` Linting test files..."
FILES_TEST=`git ls-files | grep '.py$' | grep -v -e 'alembic/versions/'`
pylint $PYLINT_OPTS $ENABLE_FOR_TESTS $FILES_TEST
echo "`date -u` No lint errors!"
