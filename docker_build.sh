#!/bin/bash

SCRIPT_NAME=$(basename $0)
SCRIPT_PATH=$(cd `dirname $0` && pwd)

docker build --no-cache -t hadoop-mini-clusters $SCRIPT_PATH
docker run -m 6g -d sakserv/hadoop-mini-clusters
#docker rm $(docker ps -a --filter ancestor=hadoop-mini-clusters)
