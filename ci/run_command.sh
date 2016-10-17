#!/bin/bash

# This is a quick and dirty hack to approximate a way
# to run one-off commands against a local instance of
# App Engine. If there's a correct way to do this, we
# couldn't figure it out, so we're using the console
# feature of the local web server.

XSRF_TOKEN=$(
  curl -s "http://localhost:8000/console" | \
  grep  -oP "(?<=xsrf_token': ').*(?=')"
)

SCRIPT=$(cat $1)

curl -s 'http://localhost:8000/console' \
    -d "code=$SCRIPT" \
    -d "module_name=default" \
    -d "xsrf_token=$XSRF_TOKEN"
