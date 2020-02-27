#!/usr/bin/env bash

mvn clean verify dependency:copy-dependencies -DskipTests &&\
docker build -t data-collector:dev -f Dockerfile-dev .
