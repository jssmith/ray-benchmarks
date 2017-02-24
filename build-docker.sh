#!/bin/bash

git rev-parse HEAD > ./docker/benchmark/git-rev
git archive -o ./docker/benchmark/benchmark.tar $(git rev-parse HEAD)
docker build --no-cache -t ray-project/benchmark docker/benchmark
rm ./docker/benchmark/benchmark.tar ./docker/benchmark/git-rev
