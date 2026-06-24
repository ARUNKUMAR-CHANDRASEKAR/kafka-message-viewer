# Kafka Message Viewer

A read-only Spring Boot API for browsing Kafka topics, partitions, offsets, and messages. Viewing uses manual partition assignment and never commits consumer offsets.

## Run locally

Start Kafka:

```bash
docker compose up -d kafka
```

Start the backend:

```bash
cd backend
mvn spring-boot:run
```

The Kafka bootstrap address defaults to `localhost:9092`. Override it with `KAFKA_BOOTSTRAP_SERVERS`.

## API

```text
GET /api/topics
GET /api/topics?includeInternal=true
GET /api/topics/{topic}/partitions
GET /api/topics/{topic}/messages?limit=100
GET /api/topics/{topic}/messages?partition=0&limit=100
```

The messages endpoint returns the newest records across every partition, ordered newest first. Use the optional `partition` parameter to inspect one partition. Every key, value, and header is returned as Base64; valid UTF-8 data is also returned as text. A Kafka tombstone remains `null`.

The viewer uses manual partition assignment, disables automatic commits and automatic topic creation, validates topic names, and exposes no produce or mutation operations.

The maximum page size and poll timeout can be changed in `backend/src/main/resources/application.yml`.

## Build the complete Compose stack

The backend image expects a packaged JAR:

```bash
cd backend
mvn clean package
cd ..
docker compose up --build
```
