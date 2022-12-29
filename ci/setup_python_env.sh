#!/bin/bash

#
# Install the python library requirements
#
sudo apt-get update
sudo apt-get install python3.7-venv python3-pip libpython3.7-dev libmysqlclient-dev
python3.7 -m venv venv
source venv/bin/activate
export PYTHONPATH=`pwd`
echo "PYTHONPATH=${PYTHONPATH}"
pip install --upgrade pip
pip install Cython
pip install "safety"
pip install -r requirements.txt
