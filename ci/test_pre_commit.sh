#!/bin/bash
if [ "$RDR_VENV" ]; then
  source "$RDR_VENV"/bin/activate
fi

# Run tests which are fast enough to be run before every push.
set -e

echo "Grepping for checked-in credentials..."
set +e  # OK if grep does not find any matches.
KEY_FILES=`git grep -il "BEGIN PRIVATE KEY" . | grep -v $0 | grep -v oauth2client | grep -v full_static_analysis`
set -e
if [ "${KEY_FILES}" ]
then
  echo "No keys may be checked in, but found: $KEY_FILES"
fi
echo "No private keys found, continuing."

# Pylint checks. Use pylint --list-msgs to see more available messages.
# More options are set in rest-api/pylintrc.
# On CircleCI, increasing parallelism with `-j 0` (or the `parallel` command)
# reduces performance significantly (10s becomes about 1m).
PYLINT_VERSION=`pylint --version | head -1 | sed 's/pylint \([0-9.]*\),/\1/g'`
echo "`date -u` Linting with pylint ${PYLINT_VERSION}..."
ENABLE_FOR_TESTS="\
  --enable=bad-indentation,broad-except,bare-except,logging-too-many-args \
  --enable=unused-argument,redefined-outer-name,redefined-builtin,superfluous-parens \
  --enable=syntax-error \
  --max-line-length=120 \
  --enable=trailing-whitespace,unused-import,unused-variable,undefined-variable"
ENABLE_FOR_ALL="$ENABLE_FOR_TESTS --enable=bad-whitespace,line-too-long,unused-import,unused-variable"
PYLINT_OPTS="-r n --disable=all --score=n"

FILES_NON_TEST=`git diff --name-only --staged | { grep '.py$' || true; } | { grep -v -e 'alembic/versions/' -e '_test' -e 'lib_fhir' || true; }`
if test "$FILES_NON_TEST"
then
  echo "`date -u` Linting $(echo $FILES_NON_TEST | wc -w) application files..."
  pylint $PYLINT_OPTS $ENABLE_FOR_ALL $FILES_NON_TEST
else
  echo "`date -u` No application files to lint..."
fi

FILES_TEST=`git diff --name-only --staged | { grep '.py$' || true; } | { grep -v -e 'alembic/versions/' -e 'lib_fhir' -e 'rdr_service/test/' || true; }`
if test "$FILES_TEST"
then
  echo "`date -u` Linting $(echo $FILES_TEST | wc -w) test files..."
  pylint $PYLINT_OPTS $ENABLE_FOR_TESTS $FILES_TEST
else
  echo "`date -u` No test files to lint..."
fi

echo "`date -u` No lint errors!"
