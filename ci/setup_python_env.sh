#!/bin/bash

#
# Install the python library requirements
#
sudo apt-get update
sudo apt-get install python3.11-venv python3-pip libpython3.11-dev libmysqlclient-dev
python3.11 -m venv venv
source venv/bin/activate
export PYTHONPATH=`pwd`
echo "PYTHONPATH=${PYTHONPATH}"
pip install --upgrade pip
pip install --upgrade pip setuptools
pip install Cython
pip install "safety"
pip install -r requirements.txt
