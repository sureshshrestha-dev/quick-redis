# Production Queue Resilience: At-Least-Once Delivery Patterns

## The Core Problem: "1% Failure Scenarios"

Production systems are **defined by how they handle failures**, not by the happy path.

### The Disaster Scenario (Your Original Code)

```python
# ❌ VULNERABLE TO JOB LOSS
job = await r.blpop("email_queue")  # Step 1: Get job from queue
user_id = job[1]
await welcome_email(user_id)   # Step 2: Process

# CRASH BETWEEN STEPS 1 & 2 → JOB IS LOST FOREVER!
```

**What happens if the server crashes on line 2?**
- Redis already removed the job from the queue (BLPOP removes immediately)
- The worker crashes before sending the email
- Result: User never gets the welcome email, and there's no record it ever happened

---

## Solution 1: RPOPLPUSH (Reliable Queue Pattern)

This pattern uses a "processing queue" to ensure jobs are never lost:

```python
# ✅ RELIABLE QUEUE PATTERN
job = await r.brpoplpush("email_queue", "email_processing")
# Job is now in TWO places:
# 1. Removed from "email_queue" (pending)
# 2. Added to "email_processing" (in-flight)

try:
    await welcome_email(user_id)
    # Success! Remove from processing
    await r.lrem("email_processing", 1, job)
except Exception as e:
    # Failure! Job stays in "email_processing"
    # Another worker will eventually process it (or an admin will investigate)
```

### Queue States:

```
                    ┌─────────────────┐
                    │   API Server    │
                    │  (enqueues job) │
                    └────────┬────────┘
                             │ RPUSH
                             ▼
                    ┌─────────────────┐
                    │  email_queue    │ ◄─ PENDING JOBS
                    │  (job1, job2)   │
                    └────────┬────────┘
                             │ BRPOPLPUSH
                             ▼
                    ┌─────────────────┐
                    │email_processing │ ◄─ JOBS BEING PROCESSED
                    │  (job1)         │    (if worker dies, these remain!)
                    └────────┬────────┘
                             │ Success/LREM
                             ▼ or Failure/LPUSH to DLQ
                    ┌─────────────────┐
                    │  email_failed   │ ◄─ DEAD LETTER QUEUE
                    │  (job2)         │    (jobs that failed)
                    └─────────────────┘
```

---

## Solution 2: Dead Letter Queue (DLQ)

When a job fails, **don't just ignore it**. Store it with error metadata:

```python
error_entry = {
    "user_id": user_id,
    "error": str(e),
    "timestamp": datetime.now().isoformat(),
    "attempt_count": 1
}

# Store in Dead Letter Queue with full context
await r.lpush("email_failed", json.dumps(error_entry))
```

### Why This Matters:

**Without DLQ:**
- Job fails → job disappears → user never knows → no audit trail

**With DLQ:**
- Job fails → stored in `email_failed` → team can investigate
- Can fix the bug and replay the job
- Full error history for debugging

---

## Production Monitoring: The Health Endpoints

### 1. Queue Health Check

```bash
GET /admin/queue-health
```

Response:
```json
{
  "status": "ok",
  "queues": {
    "pending": 5,
    "processing": 1,
    "failed": 0
  }
}
```

**Thresholds:**
- `pending < 100`: ✅ Healthy
- `pending 100-1000`: ⚠️ Warning (workers may be slow)
- `pending > 5000`: 🚨 Critical (workers likely down)

### 2. Dead Letter Queue Inspection

```bash
GET /admin/dead-letter-queue
```

See exactly what failed and why:
```json
{
  "total_failed": 2,
  "sample": [
    {
      "user_id": "bad_user",
      "error": "Invalid user ID: bad_user",
      "timestamp": "2024-05-21T10:30:45.123456",
      "attempt_count": 1
    }
  ]
}
```

### 3. Processing Queue Check

```bash
GET /admin/processing-queue-check
```

Detects **stuck jobs** (indicating worker crash):
```json
{
  "status": "warning",
  "stuck_jobs": ["user123", "user456"],
  "count": 2,
  "action": "Workers may have crashed. Check logs and restart workers."
}
```

### 4. Replay Failed Jobs

```bash
POST /admin/replay-failed-queue?limit=10
```

After fixing the bug, replay failures:
```json
{
  "replayed": 2,
  "message": "Moved 2 jobs back to pending queue"
}
```

---

## The "Senior Engineer" Approach to Queue Monitoring

### Real-Time Alerting (What Should Trigger PagerDuty)

```python
# Pseudo-code for monitoring service
async def monitor_queues():
    while True:
        health = await queue_health()
        
        # Critical: Queue explosion
        if health["queues"]["pending"] > 5000:
            alert_pagerduty("Queue backed up - workers may be down!")
        
        # Critical: Jobs stuck in processing
        stuck = await check_processing_queue()
        if stuck["count"] > 0:
            alert_pagerduty(f"Stuck jobs detected: {stuck['count']}")
        
        # Warning: Too many failures
        dlq_count = await redisclient.redis.llen("email_failed")
        if dlq_count > 100:
            alert_slack(f"DLQ has {dlq_count} failed jobs")
        
        await asyncio.sleep(60)  # Check every minute
```

### Circuit Breaking (Prevent System Collapse)

When queues are backed up, stop accepting new "heavy" requests:

```python
@app.post("/")
async def create_user(user: User):
    # Check queue health before accepting new jobs
    queue_length = await redisclient.redis.llen("email_queue")
    
    if queue_length > 10000:
        # System is overwhelmed, reject new signups
        raise HTTPException(status_code=503, 
                          detail="Service temporarily unavailable - queue backed up")
    
    # ... proceed with user creation
```

---

## Production Queue Resilience Checklist

- [ ] Use `BRPOPLPUSH` instead of `BLPOP`
- [ ] Implement Dead Letter Queue for failed jobs
- [ ] Add try/catch around job processing
- [ ] Store error metadata (timestamp, error message, attempt count)
- [ ] Monitor queue lengths in real-time
- [ ] Alert if `pending > 1000`
- [ ] Alert if `processing` queue has stuck jobs
- [ ] Create replay endpoint for failed jobs
- [ ] Implement circuit breaking when queue is too long
- [ ] Log all failures for post-mortem analysis

---

## Testing Your Implementation

### Simulate Worker Crash

```bash
# Terminal 1: Start worker
python worker.py

# Terminal 2: Send requests
for i in {1..5}; do
  curl -X POST http://localhost:8000/user/ \
    -H "Content-Type: application/json" \
    -d "{\"username\": \"user$i\", \"role\": \"admin\"}"
done

# Kill the worker (Ctrl+C)
# Check that jobs are in processing queue
curl http://localhost:8000/admin/processing-queue-check

# Restart worker - it will process stuck jobs!
python worker.py
```

### Simulate Failed Job

```bash
# Send a "bad_user" (our code will fail on this)
curl -X POST http://localhost:8000/user/ \
  -H "Content-Type: application/json" \
  -d "{\"username\": \"bad_user\", \"role\": \"admin\"}"

# Check Dead Letter Queue
curl http://localhost:8000/admin/dead-letter-queue

# Replay after fixing
curl -X POST http://localhost:8000/admin/replay-failed-queue
```

---

## Why ARQ Over DIY Queue?

For a quick project, RPOPLPUSH + DLQ works fine. But at scale, use **ARQ**:

```python
# Simple ARQ example
from arq import create_pool

async def welcome_email(ctx, user_id):
    # ARQ handles retries, timeouts, concurrency automatically
    await email_service.send(user_id)

# In your API
@app.post("/")
async def create_user(user: User):
    user_id = await db.insert_one("users", user.dict())
    
    # Enqueue with automatic retry
    await job_queue.enqueue_job(
        'welcome_email',
        user_id,
        _timeout=30,
        _retry=3
    )
    return {"user_id": str(user_id)}
```

**ARQ handles:**
- ✅ Automatic retries with exponential backoff
- ✅ Job timeouts
- ✅ Horizontal scaling
- ✅ Built-in monitoring
- ✅ Results persistence

---

## Key Takeaway

> **"At-Least-Once" delivery means: A job will be delivered and executed at least one time, but possibly more. Your code must be idempotent (safe to run multiple times).**

Always ask in production:
1. **What if the worker dies?** → Use processing queue
2. **What if the job fails?** → Use Dead Letter Queue
3. **What if nobody notices?** → Set up monitoring and alerts
4. **What if the queue explodes?** → Implement circuit breaking

This is what separates hobby projects from production systems.
