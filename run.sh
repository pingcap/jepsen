#!/bin/bash 
docker exec jepsen-control bash -c "rm -rf /jepsen/tidb"
docker cp tidb jepsen-control:/jepsen/


