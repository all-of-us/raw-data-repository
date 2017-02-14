#!/bin/bash

# Sets up the local working environment. Run this from the rest-api directory on checkout or 
# whenever requirements.txt changes.

BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
set -e
cd ${BASE_DIR};


echo "Removing old libs..."
rm -rf lib
find . | grep \.pyc | xargs rm -if $*

echo "Installing libs..."
pip install -r requirements.txt -t lib/
pip install MySQL-python -t lib/

echo "Installing Alembic..."
sudo pip install alembic

git submodule update --init

echo "Getting Cloud SQL Proxy..."
wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64
mkdir -p bin
mv -f cloud_sql_proxy.linux.amd64 bin/cloud_sql_proxy
chmod +x bin/cloud_sql_proxy



 