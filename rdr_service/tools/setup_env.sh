#!/bin/bash -e
# Sets up the local working environment. Run this on checkout or whenever
# requirements.txt changes.

PROJ_DIR=`git rev-parse --show-toplevel`
APP_DIR=$PROJ_DIR/rdr_service
cd ${APP_DIR};

# Depreciated
#echo "Removing old libs..."
#rm -rf lib
echo "Cleaning up artifacts (pyo, pyc, etc)..."
find . -type f -name "*.py[co]" -delete
find . -type d -name "__pycache__" -delete
rm -f .coverage
rm -rf htmlcov/
# Depreciated
# export PYTHONDONTWRITEBYTECODE=True

#
# Depreciated : don't install python packages here.  Always use requirements.in.
#
#echo "Installing libs..."
## If this fails due to missing mysql_config, try `sudo apt-get install libmysqlclient-dev`.
## pip install -r requirements.txt -t lib/
#
## Needed to setup the local DB.
#pip install -r ../rdr_client/requirements.txt
#
## MySQL-python must be installed outside the lib directory, or dev_appserver.py will fail with
## "No module named _mysql".
#pip install MySQL-python
#
#echo "Installing Alembic..."
#pip install alembic
#echo "Installing JIRA..."
#pip install jira
#pip install requests[security]
#
#echo "Installing pylint for git hooks..."
#pip install pylint
#echo "Installing coverage.py..."
#pip install coverage
#
#git submodule update --init

echo "Configuring Git hooks..."
HOOKS_DIR=../.git/hooks
HOOKS_FILE=$HOOKS_DIR/pre-commit
if [ -f $HOOKS_FILE ]
then
  echo Hooks file $HOOKS_FILE already exists.
  echo Please add git-hooks/pre-commit to $HOOKS_FILE
else
  mkdir -p $HOOKS_DIR
  echo Creating $HOOKS_FILE, edit to add/enable additional checks.
  # Add default hooks (linting) and tools scripts to optionally run as hooks.
  cat > $HOOKS_FILE <<EOF
#!/bin/bash -e
git-hooks/pre-commit
EOF
  chmod +x $HOOKS_FILE
fi
