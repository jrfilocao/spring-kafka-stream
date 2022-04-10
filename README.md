# Unique Email and Domain Counting App 

## Requirements
* Java 17
* Docker
* docker-compose

## Build and Run
```
docker-compose up

./mvnw clean spring-boot:run
```

## Create Events and Check Stores State
```
curl -X POST http://localhost:8080/events | jq .

curl -v http://localhost:8080/emails | jq .
curl -v http://localhost:8080/emails/windowed | jq . 

curl -v http://localhost:8080/domains | jq .
```

## Stop and Clean Kafka Clusters
```docker-compose down -v; rm -rf /tmp/streams-app```