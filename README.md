## Overview

This project implements a scheduled workflow system using Go, Redis, and the DBOS workflow engine. The main workflow periodically scans Redis for machine impression data, processes it into snapshots, deletes processed keys, and simulates sending the data to an external service. There is also an ingestion workflow that pushes impression events into Redis for later processing.

### Key Components

- **Scheduled Workflow**: Finds a machine impression key in Redis, creates a snapshot, deletes the key, and sends the snapshot to an external service.
- **Ingestion Workflow**: Accepts impression events and stores them in Redis lists keyed by machine ID.


## Get Started

Follow these steps to get the application running locally.

### Prerequisites

- Docker
- Go (version 1.24 or later)

### 1. Start Dependencies

First, start the required PostgreSQL and Redis containers using Docker.

**PostgreSQL:**
```bash
docker run --name postgres-dbos \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=dbos_password \
  -e POSTGRES_DB=dbos \
  -p 5432:5432 \
  -d postgres:latest
```

**Redis:**
```bash
docker run -d --name redis -p 6379:6379 redis:latest
```

### 2. Configure Environment

Set the password for the PostgreSQL database as an environment variable. The application uses this to connect.

```bash
export PGPASSWORD=dbos_password
```

### 3. Build and Run the Application

Build the main application binary. The output will be placed in the `bin/` directory.

```bash
go build -o bin/app .
```

Now, run the application:

```bash
./bin/app
```

You should see logs indicating that the server has started on port 8080.

### 4. Ingest Sample Data

In a new terminal, run the `createImpressions` script to send a batch of sample impression data to the running application.

```bash
go run script/createImpressions.go
```

You can now observe the scheduled workflow in the application's terminal as it picks up these impressions from Redis and processes them.
