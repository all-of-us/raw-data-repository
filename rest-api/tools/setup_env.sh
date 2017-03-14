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
pip install MySQL-python

echo "Installing Alembic..."
sudo pip install alembic

git submodule update --init

echo "Getting Cloud SQL Proxy..."
wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64
mkdir -p bin
mv -f cloud_sql_proxy.linux.amd64 bin/cloud_sql_proxy
chmod +x bin/cloud_sql_proxy

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
