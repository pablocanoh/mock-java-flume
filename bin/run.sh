#!/usr/bin/env bash
docker kill $(docker ps -q)
docker rm $(docker ps -a -q)
docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=localhost --env ADVERTISED_PORT=9092 spotify/kafka &>/dev/null &
cd ..
mvn clean install
bin/flume-ng agent -n a1 -c conf -f conf/flume.properties -Dflume.root.logger=INFO,console


