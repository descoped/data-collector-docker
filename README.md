# Data Collector Docker

[![Build Status](https://drone.prod-bip-ci.ssb.no/api/badges/statisticsnorway/data-collector-docker/status.svg)](https://drone.prod-bip-ci.ssb.no/statisticsnorway/data-collector-docker)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/fa233ed462d64bbe8093fe134d2175c9)](https://www.codacy.com/manual/oranheim/data-collector-docker?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=statisticsnorway/data-collector-docker&amp;utm_campaign=Badge_Grade)
[![codecov](https://codecov.io/gh/statisticsnorway/data-collector-docker/branch/master/graph/badge.svg)](https://codecov.io/gh/statisticsnorway/data-collector-docker)

For more information about Data Collector, please refer to the [Data Collector documentation](https://github.com/statisticsnorway/data-collector-project).

## Build

`mvn clean install`


### Build and release docker-dev image

```
mvn -B clean install -DskipTests && mvn -B dependency:copy-dependencies
```

```
docker login -u USERNAME
docker build --no-cache -t statisticsnorway/data-collector:0.1 -f ./Dockerfile-dev .
docker push statisticsnorway/data-collector:0.1
docker tag statisticsnorway/data-collector:0.1 statisticsnorway/data-collector:latest
docker push statisticsnorway/data-collector:latest
```

### Pull and run Data Collector

```
docker pull statisticsnorway/data-collector:latest
docker run -it -p 9990:9990 -v $PWD/conf:/conf -v $PWD/certs:/certs -v /tmp/rawdata:/rawdata statisticsnorway/data-collector:latest
```

## Build dev

```
./build-dev.sh

docker run -it -p 9990:9990 data-collector:dev
```
