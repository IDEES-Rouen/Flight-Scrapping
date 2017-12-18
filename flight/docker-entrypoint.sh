#!/usr/bin/env bash

set -e

# Add local user
# Either use the LOCAL_USER_ID if passed in at runtime or
# fallback

echo "Starting with UID : $USER_ID"
useradd --shell /bin/bash -u $USER_ID -o -c "" -m scrapy
export HOME=/home/scrapy

exec su-exec scrapy "$@"