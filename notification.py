# Distributed Notification System with FastAPI, Redis, and SQL
# Install: pip install fastapi uvicorn redis sqlalchemy aioredis python-multipart

from fastapi import FastAPI, BackgroundTasks, HTTPException, Depends
from pydantic import BaseModel, EmailStr
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Enum as SQLEnum, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from datetime import datetime, timedelta
from typing import Optional, List
import redis.asyncio as redis
import asyncio
import json
import enum
import logging
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database setup
SQLALCHEMY_DATABASE_URL = "sqlite:///./notifications.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Redis connection
redis_client: Optional[redis.Redis] = None


# Enums
class NotificationStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    SENT = "sent"
    FAILED = "failed"
    RETRYING = "retrying"


class NotificationChannel(str, enum.Enum):
    EMAIL = "email"
    SMS = "sms"
    PUSH = "push"
    WEBHOOK = "webhook"


# Database Models
class Notification(Base):
    __tablename__ = "notifications"

    id = Column(Integer, primary_key=True, index=True)
    channel = Column(SQLEnum(NotificationChannel), nullable=False)
    recipient = Column(String, nullable=False, index=True)
    subject = Column(String, nullable=True)
    message = Column(Text, nullable=False)
    status = Column(SQLEnum(NotificationStatus), default=NotificationStatus.PENDING, index=True)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    sent_at = Column(DateTime, nullable=True)
    failed_at = Column(DateTime, nullable=True)
    error_message = Column(Text, nullable=True)
    metadata = Column(Text, nullable=True)


Base.metadata.create_all(bind=engine)


# Pydantic Models
class NotificationCreate(BaseModel):
    channel: NotificationChannel
    recipient: str
    subject: Optional[str] = None
    message: str
    metadata: Optional[dict] = None


class NotificationResponse(BaseModel):
    id: int
    channel: NotificationChannel
    recipient: str
    status: NotificationStatus
    created_at: datetime

    class Config:
        from_attributes = True


class NotificationStats(BaseModel):
    total: int
    pending: int
    sent: int
    failed: int
    processing: int


# Rate Limiter
class RateLimiter:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    async def check_rate_limit(self, key: str, limit: int, window: int) -> bool:
        """Check if rate limit is exceeded. Returns True if allowed."""
        current = await self.redis.get(key)
        if current is None:
            await self.redis.setex(key, window, 1)
            return True

        count = int(current)
        if count >= limit:
            return False

        await self.redis.incr(key)
        return True


# Notification Queue Manager
class NotificationQueue:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.queue_key = "notification:queue"
        self.processing_key = "notification:processing"

    async def enqueue(self, notification_id: int, priority: int = 0):
        """Add notification to queue with priority."""
        await self.redis.zadd(self.queue_key, {str(notification_id): priority})
        logger.info(f"Enqueued notification {notification_id} with priority {priority}")

    async def dequeue(self) -> Optional[int]:
        """Get next notification from queue."""
        result = await self.redis.zpopmin(self.queue_key, 1)
        if result:
            notification_id = int(result[0][0])
            await self.redis.setex(f"{self.processing_key}:{notification_id}", 300, 1)
            return notification_id
        return None

    async def mark_complete(self, notification_id: int):
        """Remove notification from processing set."""
        await self.redis.delete(f"{self.processing_key}:{notification_id}")


# Notification Sender (simulated)
class NotificationSender:
    def __init__(self, rate_limiter: RateLimiter):
        self.rate_limiter = rate_limiter

    async def send(self, notification: Notification) -> tuple[bool, Optional[str]]:
        """Simulate sending notification. Returns (success, error_message)."""
        channel = notification.channel
        recipient = notification.recipient

        # Rate limiting per channel
        rate_key = f"rate_limit:{channel}:{recipient}"
        if not await self.rate_limiter.check_rate_limit(rate_key, limit=10, window=60):
            return False, "Rate limit exceeded"

        # Simulate different channel behaviors
        await asyncio.sleep(0.1)  # Simulate network delay

        # Simulate 10% failure rate for demonstration
        import random
        if random.random() < 0.1:
            return False, f"Failed to send via {channel}"

        logger.info(f"Sent {channel} notification to {recipient}")
        return True, None


# Background Worker
class NotificationWorker:
    def __init__(self, db: SessionLocal, queue: NotificationQueue, sender: NotificationSender):
        self.db = db
        self.queue = queue
        self.sender = sender
        self.running = False

    async def start(self):
        """Start the worker."""
        self.running = True
        logger.info("Notification worker started")

        while self.running:
            try:
                notification_id = await self.queue.dequeue()
                if notification_id:
                    await self.process_notification(notification_id)
                else:
                    await asyncio.sleep(1)  # Wait if queue is empty
            except Exception as e:
                logger.error(f"Worker error: {e}")
                await asyncio.sleep(1)

    async def process_notification(self, notification_id: int):
        """Process a single notification."""
        db = SessionLocal()
        try:
            notification = db.query(Notification).filter(Notification.id == notification_id).first()
            if not notification:
                return

            # Update status to processing
            notification.status = NotificationStatus.PROCESSING
            db.commit()

            # Attempt to send
            success, error = await self.sender.send(notification)

            if success:
                notification.status = NotificationStatus.SENT
                notification.sent_at = datetime.utcnow()
                logger.info(f"Successfully sent notification {notification_id}")
            else:
                notification.retry_count += 1
                if notification.retry_count >= notification.max_retries:
                    notification.status = NotificationStatus.FAILED
                    notification.failed_at = datetime.utcnow()
                    notification.error_message = error
                    logger.error(f"Notification {notification_id} failed after {notification.retry_count} retries")
                else:
                    notification.status = NotificationStatus.RETRYING
                    notification.error_message = error
                    # Re-enqueue with exponential backoff priority
                    priority = datetime.utcnow().timestamp() + (2 ** notification.retry_count)
                    await self.queue.enqueue(notification_id, int(priority))
                    logger.warning(f"Retrying notification {notification_id} (attempt {notification.retry_count})")

            db.commit()
            await self.queue.mark_complete(notification_id)

        except Exception as e:
            logger.error(f"Error processing notification {notification_id}: {e}")
            if notification:
                notification.status = NotificationStatus.FAILED
                notification.error_message = str(e)
                db.commit()
        finally:
            db.close()

    def stop(self):
        """Stop the worker."""
        self.running = False
        logger.info("Notification worker stopped")


# Database dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# FastAPI app with lifespan
worker_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global redis_client, worker_task
    redis_client = await redis.from_url("redis://localhost:6379", decode_responses=True)

    # Start background worker
    rate_limiter = RateLimiter(redis_client)
    queue = NotificationQueue(redis_client)
    sender = NotificationSender(rate_limiter)
    worker = NotificationWorker(SessionLocal, queue, sender)

    worker_task = asyncio.create_task(worker.start())
    logger.info("Application startup complete")

    yield

    # Shutdown
    worker.stop()
    if worker_task:
        worker_task.cancel()
    await redis_client.close()
    logger.info("Application shutdown complete")


app = FastAPI(title="Distributed Notification System", lifespan=lifespan)


# API Endpoints
@app.post("/notifications", response_model=NotificationResponse, status_code=201)
async def create_notification(
        notification: NotificationCreate,
        db: Session = Depends(get_db)
):
    """Create a new notification and queue it for delivery."""
    db_notification = Notification(
        channel=notification.channel,
        recipient=notification.recipient,
        subject=notification.subject,
        message=notification.message,
        metadata=json.dumps(notification.metadata) if notification.metadata else None
    )
    db.add(db_notification)
    db.commit()
    db.refresh(db_notification)

    # Enqueue for processing
    queue = NotificationQueue(redis_client)
    await queue.enqueue(db_notification.id)

    return db_notification


@app.get("/notifications/{notification_id}", response_model=NotificationResponse)
async def get_notification(notification_id: int, db: Session = Depends(get_db)):
    """Get notification status by ID."""
    notification = db.query(Notification).filter(Notification.id == notification_id).first()
    if not notification:
        raise HTTPException(status_code=404, detail="Notification not found")
    return notification


@app.get("/notifications", response_model=List[NotificationResponse])
async def list_notifications(
        status: Optional[NotificationStatus] = None,
        channel: Optional[NotificationChannel] = None,
        limit: int = 50,
        db: Session = Depends(get_db)
):
    """List notifications with optional filtering."""
    query = db.query(Notification)
    if status:
        query = query.filter(Notification.status == status)
    if channel:
        query = query.filter(Notification.channel == channel)

    notifications = query.order_by(Notification.created_at.desc()).limit(limit).all()
    return notifications


@app.get("/stats", response_model=NotificationStats)
async def get_stats(db: Session = Depends(get_db)):
    """Get notification statistics."""
    total = db.query(Notification).count()
    pending = db.query(Notification).filter(Notification.status == NotificationStatus.PENDING).count()
    sent = db.query(Notification).filter(Notification.status == NotificationStatus.SENT).count()
    failed = db.query(Notification).filter(Notification.status == NotificationStatus.FAILED).count()
    processing = db.query(Notification).filter(Notification.status == NotificationStatus.PROCESSING).count()

    return NotificationStats(
        total=total,
        pending=pending,
        sent=sent,
        failed=failed,
        processing=processing
    )


@app.post("/notifications/{notification_id}/retry")
async def retry_notification(notification_id: int, db: Session = Depends(get_db)):
    """Manually retry a failed notification."""
    notification = db.query(Notification).filter(Notification.id == notification_id).first()
    if not notification:
        raise HTTPException(status_code=404, detail="Notification not found")

    if notification.status not in [NotificationStatus.FAILED, NotificationStatus.RETRYING]:
        raise HTTPException(status_code=400, detail="Only failed notifications can be retried")

    notification.status = NotificationStatus.PENDING
    notification.retry_count = 0
    notification.error_message = None
    db.commit()

    queue = NotificationQueue(redis_client)
    await queue.enqueue(notification_id)

    return {"message": "Notification queued for retry", "id": notification_id}


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        await redis_client.ping()
        return {"status": "healthy", "redis": "connected"}
    except:
        return {"status": "unhealthy", "redis": "disconnected"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)