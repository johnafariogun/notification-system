# Push Notification Service

High-throughput, cloud-ready push notification microservice built with FastAPI, RabbitMQ, Redis, PostgreSQL, and Firebase Cloud Messaging (FCM). The service exposes REST APIs for queuing, sending, and tracking push notifications while handling retries, idempotency, and device token management.

---

## Overview

- **Service type:** Stateless FastAPI application with asynchronous workers
- **Primary responsibility:** Deliver mobile/web push notifications via Firebase Cloud Messaging
- **Message guarantees:** At-least-once delivery with idempotency protections
- **Operational focus:** Horizontal scalability, observability, and resilience (circuit breaker + retries)

---

## Architecture

```
Clients / Integrations
        |
        v
 FastAPI Gateway  ----->  PostgreSQL (notification history, device tokens)
        |                     |
        v                     |
 RabbitMQ Exchange  <---------+
        |
        v
 Async Worker  ---> Redis (idempotency cache, rate limiting)
        |
        v
 Firebase Cloud Messaging (FCM)
```

---

## Features

- REST API for single, immediate, and bulk notification dispatch
- Robust retry strategy with exponential backoff and dead-letter queue
- Circuit breaker around FCM requests to protect upstream stability
- Redis-backed idempotency to deduplicate notifications
- Device token registration, management, and lookup endpoints
- Health metrics endpoint and structured logging
- Docker-first development environment with RabbitMQ, Redis, and PostgreSQL

---

## Tech Stack

- **Runtime:** Python 3.12, FastAPI, Uvicorn
- **Messaging:** RabbitMQ (direct exchange with push/retry/DLQ queues)
- **Cache & Idempotency:** Redis 7
- **Persistence:** PostgreSQL 15 with async SQLAlchemy
- **Push Provider:** Firebase Cloud Messaging (HTTP v1 API)
- **Infrastructure:** Docker & docker-compose

---

## Prerequisites

- Docker 24+ and docker-compose v2
- Python 3.12 (if running the app/tests outside Docker)
- Firebase service account JSON with FCM access (`Editor` or `Firebase Admin` role)
- Outbound internet access to FCM endpoints

---

## Configuration

All settings can be supplied via environment variables or a `.env` file (see `app/config.py`). Defaults target the docker-compose environment.

| Variable | Description | Default |
| --- | --- | --- |
| `RABBITMQ_URL` | AMQP connection string | `amqp://guest:guest@rabbitmq:5672/` |
| `REDIS_URL` | Redis connection URI | `redis://redis:6379` |
| `DATABASE_URL` | PostgreSQL connection URI | `postgresql://notif_user:notif_pass@postgres:5432/notifications_db` |
| `PROJECT_ID` | GCP project ID for FCM | `mindful-torus-458106-p9` |
| `SERVICE_ACCOUNT_PATH` | Path to Firebase credentials JSON | `/app/firebase-credentials.json` |
| `LOG_LEVEL` | Python logging level | `INFO` |
| `SERVICE_PORT` | HTTP port exposed by FastAPI | `8001` |
| `MAX_RETRIES` | Max delivery attempts per notification | `3` |
| `CIRCUIT_BREAKER_THRESHOLD` | Failures before tripping breaker | `5` |
| `CIRCUIT_BREAKER_TIMEOUT` | Seconds before breaker half-opens | `60` |
| `RATE_LIMIT_PER_MINUTE` | Soft rate limit used by consumer | `1000` |
| `RATE_LIMIT_PER_HOUR` | Soft rate limit used by consumer | `10000` |
| `WORKER_PREFETCH_COUNT` | RabbitMQ prefetch for consumer | `10` |
| `WORKER_THREADS` | Worker thread pool size | `4` |

---

## Project Structure

```
.
├── app/
│   ├── main.py                # FastAPI application and consumers
│   ├── models.py              # SQLAlchemy models
│   ├── schemas.py             # Pydantic request/response schemas
│   ├── config.py              # Settings management
│   └── monitoring.py          # Metrics utilities
├── docker-compose.yml         # Local orchestration stack
├── Dockerfile                 # Service container definition
├── init.sql                   # Database bootstrap script
├── requirements.txt           # Python dependencies
└── tests/                     # Pytest suite for core flows
```

---

## Local Development

1. **Provide Firebase credentials**  
   Place your service account JSON in the project root and update `docker-compose.yml` if the filename differs from the checked-in sample.

2. **Start the stack**  
   ```bash
   docker-compose up -d
   ```

3. **Verify health**  
   ```bash
   curl http://localhost:8001/health
   ```

4. **Inspect logs**  
   ```bash
   docker-compose logs -f push_service
   ```

- RabbitMQ dashboard: http://localhost:15672 (guest/guest)  
- API docs (Swagger): http://localhost:8001/docs

### Running Without Docker

```bash
python -m venv .venv
source .venv/bin/activate        # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8001
```
Ensure RabbitMQ, Redis, and PostgreSQL are reachable at the configured URLs before starting.

---

## Database

- `init.sql` provisions notification history, device tokens, templates, and preference tables.  
- Applied automatically when `postgres` container initializes.  
- For fresh local data: `docker-compose down -v` to drop persisted volumes.

---

## API Reference

- `POST /api/notifications/send` – Queue a notification for asynchronous delivery.
- `POST /api/notifications/send-immediate` – Send immediately (synchronous). Use sparingly.
- `POST /api/notifications/send-bulk` – Queue up to 100 notifications in a single request.
- `GET /api/notifications/status` – Retrieve status by `notification_id` or `idempotency_key`.
- `POST /api/device-tokens` – Register or update an FCM device token for a user.
- `GET /api/device-tokens/{user_id}` – List active tokens for a user.
- `DELETE /api/device-tokens/{token}` – Deactivate a stored token.
- `GET /health` – Service and dependency health summary.
- `GET /docs` – Interactive OpenAPI documentation.

### Sample Request

```bash
curl -X POST http://localhost:8001/api/notifications/send \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Welcome!",
    "body": "Thanks for signing up.",
    "token": "YOUR_DEVICE_TOKEN",
    "user_id": "user-123",
    "idempotency_key": "user-123-welcome"
  }'
```

---

## Message Lifecycle

1. API request validated with Pydantic schemas.  
2. Notification persisted to PostgreSQL with status `pending`.  
3. Message published to RabbitMQ `notifications.direct` exchange (`push.queue`).  
4. Consumer pulls message, validates token/idempotency, and calls FCM via `PushServiceManager`.  
5. Success → status updated to `sent`, idempotency key cached in Redis.  
6. Failure → exponential backoff retry up to `MAX_RETRIES`; afterwards message is routed to `failed.queue` (DLQ).  
7. Circuit breaker protects FCM by opening when repeated errors occur; retries are paused until timeout elapses.

---

## Observability & Operations

- **Logging:** Structured application logs written to `push_logs.log` inside the container (mounted to `./logs`).  
- **Metrics:** `app/monitoring.py` provides hooks for custom metrics aggregation (extend for Prometheus/StatsD).  
- **Health:** `/health` reports dependency connectivity and circuit breaker state.  
- **Dead-letter queue:** Inspect `failed.queue` via RabbitMQ UI for messages that exhausted retries.

---

## Testing

Run the Pytest suite (requires services or mocks):

```bash
pytest
```

CI/CD pipelines should ensure supporting services are available (e.g., via docker-compose or test doubles).

---

## Scaling & Deployment

- Stateless design enables horizontal scaling:  
  ```bash
  docker-compose up -d --scale push_service=5
  ```
- Ensure each replica has access to the same RabbitMQ, Redis, PostgreSQL, and FCM credentials.  
- For production, deploy behind an ingress/load balancer and configure TLS termination.  
- Tune `WORKER_PREFETCH_COUNT`, `WORKER_THREADS`, and resource limits per workload.  
- Consider managed services (Cloud SQL, Memorystore, CloudAMQP) for high availability.

---

## Troubleshooting

- **FCM failures:** Check that the service account has the correct permissions and the `PROJECT_ID` matches the FCM project.  
- **Dead-letter messages:** Review `failed.queue` payloads; increase `MAX_RETRIES` or investigate client tokens.  
- **Circuit breaker open:** Wait for timeout or manually recycle the service after addressing FCM issues.  
- **Idempotency conflicts:** Ensure clients generate unique `idempotency_key` per logical notification.  
- **Connectivity errors:** Confirm Docker network is healthy (`docker network inspect notification_system_notification_network`).

---

## Next Steps

- Integrate external metrics (Prometheus exporter) and alerting.  
- Add authentication/authorization (API keys or OAuth) for production deployments.  
- Extend templates/preferences tables with admin APIs when needed.
