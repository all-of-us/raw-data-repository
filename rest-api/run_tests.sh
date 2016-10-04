#!/bin/bash

if [ -z "$1" ];
then
  echo "Usage: run_test.sh /path/to/google/cloud/sdk_dir"
  exit 1
fi

export PYTHONPATH=$PYTHONPATH:`pwd`:lib
python test/runner.py --test-path offline/ $1

