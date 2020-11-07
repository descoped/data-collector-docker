# Data Collector Docker

[![Build Status](https://drone.prod-bip-ci.ssb.no/api/badges/statisticsnorway/data-collector-docker/status.svg)](https://drone.prod-bip-ci.ssb.no/statisticsnorway/data-collector-docker)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/fa233ed462d64bbe8093fe134d2175c9)](https://www.codacy.com/manual/oranheim/data-collector-docker?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=statisticsnorway/data-collector-docker&amp;utm_campaign=Badge_Grade)
[![codecov](https://codecov.io/gh/statisticsnorway/data-collector-docker/branch/master/graph/badge.svg)](https://codecov.io/gh/statisticsnorway/data-collector-docker)

For more information about Data Collector, please refer to the [Data Collector documentation](https://github.com/statisticsnorway/data-collector-project).

## Build

```
cd data-collector-project
mvn clean install
```


### Build and release docker-dev image

> :bulb: To determine next version manually, check latest version at [Docker Hub](https://cloud.docker.com/u/statisticsnorway/repository/docker/statisticsnorway/data-collector/tags)

> :warning: Versioning must follow: https://semver.org

```
cd data-collector-docker

mvn -B clean install -DskipTests && mvn -B dependency:copy-dependencies

docker login -u USERNAME

docker build --no-cache -t statisticsnorway/data-collector:MAJOR.MINOR.PATCH -f ./Dockerfile-dev .
docker push statisticsnorway/data-collector:MAJOR.MINOR.PATCH
docker tag statisticsnorway/data-collector:MAJOR.MINOR.PATCH statisticsnorway/data-collector:latest
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

## Build dev and Visual VM Profiling

Visual VM can be connected to `no.ssb.dc.server.Server` or using `data-collector:dev` image.

Follow these instructions for setting up profiling.

### Run `Server` from IntelliJ

1. Configure `Server` (see enum inside)
1. Consult readme in `data-collection-consumer-specifications`-repo regarding GCS configuration
1. Run `Server` in IntelliJ using Visual VM plugin
1. `make collect-freg-playground`

### Docker-dev (`data-collection-consumer-specifications`)

> Consult readme in `data-collection-consumer-specifications`-repo regarding GCS configuration.

1. `make build-data-collector-dev-image`
1. `make start-gcs-dev`
1. `make tail-gcs-dev`
1. [Download and open Visual VM](https://visualvm.github.io/) (standard version, not Graal download)
1. File --> Add Remote Host ("0.0.0.0")
1. In left pane, right click "0.0.0.0" and Choose "Add JMX Connection"
1. Set port to 9992
1. `make collect-freg-playground`

## Configuration

### Google Secret Manager SSL properties

```
data.collector.sslBundle.provider=google-secret-manager
data.collector.sslBundle.gcp.projectId=SECRET_MANAGER_PROJECT_ID
data.collector.sslBundle.gcp.serviceAccountKeyPath=(optional)
data.collector.sslBundle.type=(pem | p12)
data.collector.sslBundle.name=ssb-prod-certs
data.collector.sslBundle.publicCertificate=secretName
data.collector.sslBundle.privateCertificate=secretName
data.collector.sslBundle.archiveCertificate=secretName
data.collector.sslBundle.passphrase=secretNam
```

### Rawdata Encryption Credentials

```
rawdata.encryption.provider = (dynamic-secret-configuration | google-secret-manager)
rawdata.encryption.gcp.projectId => google-secret-manager
rawdata.encryption.gcp.serviceAccountKeyPath => google-secret-manager
rawdata.encryption.key  = (encryptionKey | secretName)
rawdata.encryption.salt = (encryptionSalt | secretName)
```

### Logstash

Set environment variable `LOGBACK_CONFIGURATION_FILE=/opt/dc/logback-stash.xml` to enable structured logging using Logstash.

.
