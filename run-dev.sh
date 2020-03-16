#!/bin/sh

docker run -it -p 9990:9990 -p 9992:9992 -v $PWD/certs:/opt/dc/certs data-collector:dev

