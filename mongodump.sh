#!/bin/bash

CONTAINER_NAME="liow2dataimport_mongo_1"
ARCHIVE_NAME="liow2.archive.gz"

docker exec ${CONTAINER_NAME} mongodump --archive=/data-out/${ARCHIVE_NAME} --gzip --db=liow2 \
&& \
docker-machine scp dev:/data-out/${ARCHIVE_NAME} data/
