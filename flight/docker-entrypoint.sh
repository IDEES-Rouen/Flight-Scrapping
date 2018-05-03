#!/bin/sh
set -e

if [ "$(whoami)" == "root" ]; then
    chown -R scrapy:scrapy /home/scrapy/backup
    chown --dereference scrapy "/proc/$$/fd/1" "/proc/$$/fd/2" || :
    exec su-exec scrapy "$@"
fi