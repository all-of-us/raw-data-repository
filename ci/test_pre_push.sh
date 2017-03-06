#!/bin/bash
# Run tests which are fast enough to be run before very push.

set -e

# Pylint checks. Use pylint --list-msgs to see more available messages.
# More options are set in rest-api/pylintrc.
ENABLE_FOR_TESTS="\
  --enable=bad-indentation,broad-except,bare-except,logging-too-many-args \
  --enable=unused-argument,redefined-outer-name,redefined-builtin,superfluous-parens \
  --enable=trailing-whitespace,unused-import,unused-variable,undefined-variable"
ENABLE_FOR_ALL="$ENABLE_FOR_TESTS --enable=bad-whitespace,line-too-long,unused-import,unused-variable"
PYLINT_OPTS="-r n --disable=all"
git ls-files | grep '.py$' | grep -v -e 'alembic/versions/' -e '_test' | \
    parallel pylint $PYLINT_OPTS $ENABLE_FOR_ALL
git ls-files | grep '.py$' | grep -v -e 'alembic/versions/' | \
    parallel pylint $PYLINT_OPTS $ENABLE_FOR_TESTS
