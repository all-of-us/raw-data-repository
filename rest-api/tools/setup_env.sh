#!/bin/bash -e
# Sets up the local working environment. Run this on checkout or whenever
# requirements.txt changes.

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
cd ${BASE_DIR};

echo "Removing old libs..."
rm -rf lib
find . | grep \.pyc | xargs rm -if $*

echo "Installing libs..."
# If this fails due to missing mysql_config, try `sudo apt-get install libmysqlclient-dev`.
pip install -r requirements.txt -t lib/

# MySQL-python must be installed outside the lib directory, or dev_appserver.py will fail with
# "No module named _mysql".
sudo pip install MySQL-python

echo "Installing Alembic..."
pip install --user alembic
echo "Installing JIRA..."
pip install --user jira
pip install --user requests[security]

git submodule update --init

echo "Configuring Git hooks..."
HOOKS_DIR=../.git/hooks
HOOKS_FILE=$HOOKS_DIR/pre-push
if [ -f $HOOKS_FILE ]
then
  echo Hooks file $HOOKS_FILE already exists.
else
  mkdir -p $HOOKS_DIR
  echo Creating $HOOKS_FILE, edit to add/enable additional checks.
  # Add default hooks (linting) and tools scripts to optionally run as hooks.
  cat > $HOOKS_FILE <<EOF
#!/bin/bash -e
git-hooks/pre-push
#rest-api/tools/check_uncommitted.sh
#rest-api/tools/remove_trailing_whitespace.sh -y
EOF
  chmod +x $HOOKS_FILE
fi
