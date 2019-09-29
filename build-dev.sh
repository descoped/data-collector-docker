#!/usr/bin/env bash

mvn clean verify dependency:copy-dependencies &&\
docker build -t data-collector:dev -f Dockerfile-dev .
