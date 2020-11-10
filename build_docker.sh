#!/bin/bash

if [ $# -eq 0 ]
  then
    echo "No version specified, using default tag"
fi

docker build -t registry.spin.nersc.gov/dmcreyno/splash_ingest_api:$1 .  
