# Seedance2 2API Proxy

A 2API/OpenAI-compatible proxy for Seedance web generation flow.

## Features

- OpenAI/2API-style video generation API
- Optional response shape: `generation` or `responses`
- Redis-backed persistent job state
- BullMQ async queue worker
- Text-to-video and image-to-video endpoints
- API key authentication (Bearer)
- Per-IP rate limiting
- `/v1/models` model discovery
- Prometheus metrics endpoint (`/metrics`)
- Docker Compose deployment (Redis + Proxy + Nginx)

## Requirements

- Node.js 20+ (for local run)
- Redis 6+ (for local run)
- Docker + Docker Compose (for container deployment)

## Local Run

1. Copy `.env.example` to `.env` and edit values:

```bash
cp .env.example .env
```



```bash
npm install
API_KEY=your_secret_key npm start
```

Defaults:

- `HOST=127.0.0.1`
- `PORT=8787`
- `REDIS_URL=redis://127.0.0.1:6379`

## Docker Compose Run

Compose reads `API_KEY` and other values from your shell environment or a `.env` file in the project root.

1. Set environment variable (recommended):

```bash
export API_KEY='your_strong_key'
```

2. Start all services:

```bash
docker compose up -d --build
```

3. Verify health:

```bash
curl -s http://127.0.0.1:8080/health
```

4. Stop:

```bash
docker compose down
```

## Auth

When `API_KEY` is set, all `/v1/*` APIs require:

```http
Authorization: Bearer your_secret_key
```

## Endpoints

- `GET /health`
- `GET /metrics`
- `GET /v1/models`
- `POST /v1/videos/generations` (text-to-video)
- `POST /v1/videos/image-to-video` (multipart image upload)
- `GET /v1/videos/generations/:id` (poll status)

## 1) Text to Video

```bash
curl -s 'http://127.0.0.1:8080/v1/videos/generations?format=generation' \
  -H 'Authorization: Bearer your_secret_key' \
  -H 'Content-Type: application/json' \
  -d '{
    "prompt":"A cinematic drone shot over snowy mountains at sunrise, ultra realistic",
    "aspect_ratio":"VIDEO_ASPECT_RATIO_LANDSCAPE",
    "n":1
  }'
```

OpenAI responses-style:

```bash
curl -s 'http://127.0.0.1:8080/v1/videos/generations?format=responses' \
  -H 'Authorization: Bearer your_secret_key' \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"...long enough prompt..."}'
```

## 2) Image to Video

```bash
curl -s 'http://127.0.0.1:8080/v1/videos/image-to-video?format=generation' \
  -H 'Authorization: Bearer your_secret_key' \
  -F 'image=@/absolute/path/demo.png' \
  -F 'prompt=Animate this image with cinematic camera movement' \
  -F 'aspect_ratio=VIDEO_ASPECT_RATIO_LANDSCAPE'
```

## 3) Poll

```bash
curl -s 'http://127.0.0.1:8080/v1/videos/generations/<id>?format=generation' \
  -H 'Authorization: Bearer your_secret_key'
```

## 4) Models

```bash
curl -s 'http://127.0.0.1:8080/v1/models' \
  -H 'Authorization: Bearer your_secret_key'
```

## 5) Metrics

```bash
curl -s 'http://127.0.0.1:8080/metrics'
```

## Environment Variables

- `HOST` default `127.0.0.1`
- `PORT` default `8787`
- `API_KEY` default empty (disabled)
- `RATE_LIMIT_PER_MINUTE` default `20`
- `REDIS_URL` default `redis://127.0.0.1:6379`
- `QUEUE_NAME` default `seedance-video-jobs`
- `JOB_TTL_SECONDS` default `86400`
- `SEEDANCE_PAGE_URL` default `https://veoaifree.com/seedance-2-0-video-generator-free/`
- `SEEDANCE_AJAX_URL` default `https://veoaifree.com/wp-admin/admin-ajax.php`
- `POLL_INTERVAL_MS` default `10000`
- `MAX_POLL_ATTEMPTS` default `18`
- `REQUEST_TIMEOUT_MS` default `30000`

## Notes

- Upstream is web-private, not official public API. Stability is not guaranteed.
- Use retries, monitoring, and fallback providers in production.
