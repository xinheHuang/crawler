#!/bin/sh
echo 'start deploy crawler'
cd crawler
git pull
cnpm i
pm2 restart crawler
echo 'deploy success'
