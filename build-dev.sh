#!/usr/bin/env bash

mvn clean verify dependency:copy-dependencies &&\
docker build -t dc-filesystem:dev -f Dockerfile-dev .
