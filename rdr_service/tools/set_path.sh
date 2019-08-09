#!/bin/bash -e

PROJ_DIR=`git rev-parse --show-toplevel`
export APP_DIR=$PROJ_DIR/rdr_service
export PYTHONPATH=$PYTHONPATH:${APP_DIR}
