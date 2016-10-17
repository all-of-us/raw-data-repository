#!/bin/bash

XSRF_TOKEN=$(
  curl -s "http://localhost:8000/console" | \
  grep  -oP "(?<=xsrf_token': ').*(?=')"
)

SCRIPT=$(cat $1)

curl -s 'http://localhost:8000/console' \
    -d "code=$SCRIPT" \
    -d "module_name=default" \
    -d "xsrf_token=$XSRF_TOKEN"
