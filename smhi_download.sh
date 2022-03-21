#!/bin/bash
# bash script to download NEMO model data from SMHI
# API documentation at https://opendata.smhi.se/apidocs/grid/

today=$(date +%F)
yesterday=$(date -d "yesterday" '+%F')
geturl="opendata-download-grid-archive.smhi.se/feed/8/data?from=${yesterday}T00:00:00.000Z&to=${today}T00:01:00.000Z"
wget -nv -O /tmp/smhidata.zip $geturl
unzip -jo /tmp/smhidata.zip -d /data/third_party/smhi/model_output
