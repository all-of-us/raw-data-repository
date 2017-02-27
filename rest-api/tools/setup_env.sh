#!/bin/bash -e

# Sets up the local working environment. Run this on checkout or whenever
# requirements.txt changes.

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
cd ${BASE_DIR};


echo "Removing old libs..."
rm -rf lib
find . | grep \.pyc | xargs rm -if $*

echo "Installing libs..."
pip install -r requirements.txt -t lib/
# MySQL-python must be installed outside the lib directory, or dev_appserver.py will fail with
# "No module named _mysql"
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
$(cd ../.git && rm -r hooks && ln -s ../git-hooks hooks)
