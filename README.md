# Data Collector Server

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
docker run -it -p 9090:9090 -v $PWD/conf:/conf -v $PWD/certs:/certs -v /tmp/rawdata:/rawdata statisticsnorway/data-collector:latest
```

