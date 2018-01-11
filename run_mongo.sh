#!/usr/bin/env bash

docker run --name mongo-data -v mongo_data:/data/db -p27017:27017 -d mongo