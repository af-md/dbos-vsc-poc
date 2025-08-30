## Overview

This project implements a scheduled workflow system using Go, Redis, and the DBOS workflow engine. The main workflow periodically scans Redis for machine impression data, processes it into snapshots, deletes processed keys, and simulates sending the data to an external service. There is also an ingestion workflow that pushes impression events into Redis for later processing.

### Key Components

- **Scheduled Workflow**: Finds a machine impression key in Redis, creates a snapshot, deletes the key, and sends the snapshot to an external service.
- **Ingestion Workflow**: Accepts impression events and stores them in Redis lists keyed by machine ID.