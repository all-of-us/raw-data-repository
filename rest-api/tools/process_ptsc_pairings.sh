#!/bin/bash -e

USAGE="tools/process_ptsc_pairings.sh --file <INPUT_FILE>"

while true; do
  case "$1" in
    --file) INPUT_FILE=$2; shift 2;;
    -- ) shift; break ;;
    * ) break ;;
  esac
done
if [ -z "${INPUT_FILE}" ]
then
  echo "Usage: $USAGE"
  exit 1
fi

source tools/set_path.sh

python tools/process_ptsc_pairings.py --file $INPUT_FILE
