# Distributed Notification System

A production-ready notification service built with FastAPI, Redis, and SQL that handles multi-channel delivery with rate limiting, retries, and failure handling.

## Features
- Multi-channel support (Email, SMS, Push, Webhook)
- Redis-backed priority queue
- Rate limiting (10 req/min per channel/recipient)
- Exponential backoff retry logic (3 attempts)
- Persistent delivery tracking with SQLite
- Async background workers
- RESTful API with health checks

## Quick Start

### Prerequisites
- Python 3.10+
- Redis

### Installation
```bash
# Clone the repository
git clone https://github.com/yourusername/notification-system.git
cd notification-system

# Install dependencies
pip install -r requirements.txt

# Start Redis
redis-server

# Run the application
python app.py
```

### Usage
```bash
# Create a notification
curl -X POST http://localhost:8000/notifications \
  -H "Content-Type: application/json" \
  -d '{"channel":"email","recipient":"user@example.com","message":"Hello!"}'

# Check stats
curl http://localhost:8000/stats
```

## API Documentation
Visit `http://localhost:8000/docs` for interactive API documentation.

## Architecture
- **Queue Management**: Redis sorted sets for priority-based processing
- **Rate Limiting**: Redis-based token bucket per channel/recipient
- **Retry Logic**: Exponential backoff (2^retry_count seconds)
- **Database**: SQLite for persistent notification tracking
- **Worker**: Background async worker with graceful shutdown

## License
see LICENSE file
