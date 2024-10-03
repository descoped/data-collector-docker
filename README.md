# Data Collector Docker

![Build Status](https://img.shields.io/github/actions/workflow/status/descoped/data-collector-docker/coverage-and-sonar-analysis.yml)
![Latest Tag](https://img.shields.io/github/v/tag/descoped/data-collector-docker)
![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=descoped_data-collector-docker&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=descoped_data-collector-docker) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=descoped_data-collector-docker&metric=coverage)](https://sonarcloud.io/summary/new_code?id=descoped_data-collector-docker)
[![Snyk Security Score](https://snyk.io/test/github/descoped/data-collector-docker/badge.svg)](https://snyk.io/test/github/descoped/data-collector-docker)

For more information about Data Collector, please refer to
the [Data Collector documentation](https://github.com/descoped/data-collector-project).


## Build

```
cd data-collector-project
mvn clean install
```

### Build and release docker-dev image

> :bulb: To determine next version manually, check latest version
> at [Docker Hub](https://cloud.docker.com/u/descoped/repository/docker/descoped/data-collector/tags)

> :warning: Versioning must follow: https://semver.org

```
cd data-collector-docker

mvn -B clean install -DskipTests && mvn -B dependency:copy-dependencies

docker login -u USERNAME

docker build --no-cache -t descoped/data-collector:MAJOR.MINOR.PATCH -f ./Dockerfile-dev .
docker push descoped/data-collector:MAJOR.MINOR.PATCH
docker tag descoped/data-collector:MAJOR.MINOR.PATCH descoped/data-collector:latest
docker push descoped/data-collector:latest
```

### Pull and run Data Collector

```
docker pull descoped/data-collector:latest
docker run -it -p 9990:9990 -v $PWD/conf:/conf -v $PWD/certs:/certs -v /tmp/rawdata:/rawdata descoped/data-collector:latest
```

## Build dev

```
./build-dev.sh

docker run -it -p 9990:9990 data-collector:dev
```

## Build dev and Visual VM Profiling

Visual VM can be connected to `io.descoped.dc.server.Server` or using `data-collector:dev` image.

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

Set environment variable `LOGBACK_CONFIGURATION_FILE=/opt/dc/log4j2-logstash.xml` to enable structured logging using
Logstash.

.
