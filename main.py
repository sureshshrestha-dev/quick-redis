from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from bson import ObjectId
import redis.asyncio as redis
from schemas import User
from helper import get_database, get_redis_client

app = FastAPI(prefix="/user")


# Lua script for atomic rate limiting (no race conditions)
RATE_LIMIT_LUA_SCRIPT = """
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local seconds = tonumber(ARGV[2])

-- Atomically increment and check limit in a single operation
local current = redis.call("INCR", key)

-- If this is the first increment, set the expiry
if current == 1 then
    redis.call("EXPIRE", key, seconds)
end

-- Return the current count (allows caller to decide if limit exceeded)
return current
"""

async def atomic_rate_limiter(
    user_id: str, 
    redis_client: RedisClient = Depends(get_redis_client)
) -> bool:
    """
    Production-grade rate limiter using Lua scripting for atomicity.
    
    Args:
        user_id: The user identifier for rate limiting
        redis_client: Injected RedisClient instance
        
    Returns:
        False if request is allowed, True if rate limited
        
    Raises:
        HTTPException(429) if rate limit exceeded
    """
    limit = 5
    seconds = 10
    key = f"rate_limit:{user_id}"
    
    try:
        # Register the Lua script once (Redis caches by SHA1)
        lua_script = redis_client.redis.register_script(RATE_LIMIT_LUA_SCRIPT)
        
        # Execute atomically: increment + check + set expiry (all in one)
        current_count = await lua_script(keys=[key], args=[limit, seconds])
        
        # If count exceeds limit, reject the request
        if current_count > limit:
            raise HTTPException(status_code=429, detail="Too many requests")
            
        return False  # Request is allowed
        
    except HTTPException:
        # Re-raise the rate limit exception
        raise
    except Exception as e:
        # Log and fail open (allow request if Redis fails)
        print(f"⚠️  Rate limiter error: {str(e)}")
        return False
    

@app.post("/")
async def create_user(
    user: User,
    db: Database = Depends(get_database)
):
    check = await db.find_one("users", {"username": user.username})
    if check:
        raise HTTPException(status_code=400, detail="Username already exists")

    user_id = await db.insert_one("users", user.dict())
    if user_id:
        await redisclient.redis.rpush("email_queue", str(user_id))
        return {"user_id": str(user_id)}
    else:
        raise HTTPException(status_code=500, detail="User creation failed")





@app.get("/{user_id}")
async def read_user(
    user_id: str,
    db: Database = Depends(get_database),
    redis_client: RedisClient = Depends(get_redis_client)
):
    cache_key = f"user:{user_id}"
    cached_user = await redis_client.get(cache_key)
    if cached_user:
        print("Cache Hit!", cached_user)
        return cached_user
        
    print("Cache Miss, querying DB...")
    user = await db.find_one("users", {"_id": ObjectId(user_id)}, {"username": 1, "role": 1})
    if user:
        user['_id'] = str(user['_id'])
        await redis_client.set(cache_key, user, expire=60)
        return user
    
    raise HTTPException(status_code=404, detail="User not found")

@app.put("/{user_id}")
async def update_user(
    user_id: str,
    user: User,
    db: Database = Depends(get_database),
    redis_client: RedisClient = Depends(get_redis_client)
):
    check = await db.find_one("users", {"_id": ObjectId(user_id)})
    if not check:
        raise HTTPException(status_code=404, detail="User not found")
    result = await db.update_one("users", {"_id": ObjectId(user_id)}, user.dict())
    if result:
        cache_key = f"user:{user_id}"
        await redis_client.delete(cache_key)
        print("Cache Updated!", user.dict())
        return "User cleared successfully"
    raise HTTPException(status_code=404, detail="User not found")

@app.delete("/{user_id}")
async def delete_user(
    user_id: str,
    db: Database = Depends(get_database),
    redis_client: RedisClient = Depends(get_redis_client)
):
    check = await db.find_one("users", {"_id": ObjectId(user_id)})
    if not check:
        return "User not found"
    result = await db.delete_one("users", {"_id": ObjectId(user_id)})
    if result:
        cache_key = f"user:{user_id}"
        await redis_client.redis.delete(cache_key)
        return "User deleted successfully"
    raise HTTPException(status_code=404, detail="User not found")

@app.get("/v2/{user_id}")
async def read_user_v2(
    user_id: str,
    db: Database = Depends(get_database),
    redis_client: RedisClient = Depends(get_redis_client),
    rate_limit: bool = Depends(lambda u=user_id, r=Depends(get_redis_client): atomic_rate_limiter(u, r))
):
    """Read user with atomic rate limiting and real-time view tracking."""
    view_key = f"views:{user_id}"
    view_count = await redis_client.redis.incr(view_key)
    if view_count == 1:
        await redis_client.redis.expire(view_key, 20)

    cache_key = f"user:{user_id}"
    cached_user = await redis_client.get(cache_key)
    if cached_user:
        print(f"Cache Hit Views: {view_count}")
        cached_user["view_count"] = view_count  # Add real-time view count
        return cached_user
        
    print(f"Cache Miss Views: {view_count}. querying DB...")
    user = await db.find_one(
        "users", 
        {"_id": ObjectId(user_id)}, 
        {"username": 1, "role": 1}
    )
    
    if user:
        user['_id'] = str(user['_id'])
        user['view_count'] = view_count
        await redis_client.set(cache_key, user, expire=60)
        return user
    
    raise HTTPException(status_code=404, detail="User not found")


@app.delete("/cache/{user_id}")
async def clear_user_cache(
    user_id: str,
    redis_client: RedisClient = Depends(get_redis_client)
):
    pattern = f"user:{user_id}"
    deleted = await redis_client.delete_by_pattern(pattern)
    return {"message": f"deleted {deleted} cache of {user_id}"}


@app.delete("/cache-clear")
async def clear_all_user_cache(
    redis_client: RedisClient = Depends(get_redis_client)
):
    deleted = await redis_client.delete_by_pattern("user:*")
    return {"message": f"cleared {deleted} user cache"}


@app.delete("/rate-limits")
async def clear_rate_limits(
    redis_client: RedisClient = Depends(get_redis_client)
):
    deleted = await redis_client.delete_by_pattern("rate_limit:*")
    return {"message": f"deleted {deleted} rate limits"}


@app.get("/admin/redis-memory")
async def check_redis_memory(
    redis_client: RedisClient = Depends(get_redis_client)
):
    """Monitor Redis memory usage (alerts if approaching limit)"""
    memory = await redis_client.get_memory_info()
    
    # Calculate utilization percentage if maxmemory is set
    utilization = None
    if isinstance(memory['max_memory_mb'], (int, float)):
        utilization = (memory['used_memory_mb'] / memory['max_memory_mb']) * 100
    
    return {
        "used_mb": round(memory['used_memory_mb'], 2),
        "max_mb": memory['max_memory_mb'],
        "utilization_percent": round(utilization, 2) if utilization else "unlimited",
        "evicted_keys": memory['evicted_keys'],
        "warning": "Redis is approaching memory limit!" if utilization and utilization > 80 else None
    }


# ========== QUEUE HEALTH & MONITORING (PRODUCTION RESILIENCE) ==========

@app.get("/admin/queue-health")
async def queue_health(
    redis_client: RedisClient = Depends(get_redis_client)
):
    """Monitor queue lengths - CRITICAL for production!"""
    import json
    
    pending = await redis_client.redis.llen("email_queue")
    processing = await redis_client.redis.llen("email_processing")
    failed = await redis_client.redis.llen("email_failed")
    
    # Determine system status based on queue lengths
    if pending > 5000:
        status = "critical"
        health_msg = "Queue is critically backed up! Workers may be down."
    elif pending > 1000:
        status = "warning"
        health_msg = "Queue building up. Consider scaling workers."
    else:
        status = "ok"
        health_msg = "Queues operating normally."
    
    return {
        "status": status,
        "message": health_msg,
        "queues": {
            "pending": pending,
            "processing": processing,
            "failed": failed
        },
        "alert": "Consider circuit-breaking new requests" if pending > 5000 else None
    }


@app.get("/admin/dead-letter-queue")
async def inspect_dead_letter_queue(
    limit: int = 10,
    redis_client: RedisClient = Depends(get_redis_client)
):
    """Inspect failed jobs in the Dead Letter Queue"""
    import json
    
    failed_jobs = await redis_client.redis.lrange("email_failed", 0, limit - 1)
    parsed_jobs = []
    
    for job in failed_jobs:
        try:
            parsed_jobs.append(json.loads(job))
        except:
            parsed_jobs.append({"raw": job})
    
    return {
        "total_failed": await redis_client.redis.llen("email_failed"),
        "sample": parsed_jobs,
        "action": "Fix the issue and call /admin/replay-failed-queue to retry"
    }


@app.post("/admin/replay-failed-queue")
async def replay_failed_queue(
    limit: int = 10,
    redis_client: RedisClient = Depends(get_redis_client)
):
    """Replay failed jobs back to the pending queue"""
    import json
    
    replayed = 0
    for _ in range(limit):
        job = await redis_client.redis.rpop("email_failed")
        if not job:
            break
        
        try:
            job_data = json.loads(job)
            user_id = job_data.get("user_id")
            if user_id:
                await redis_client.redis.rpush("email_queue", user_id)
                replayed += 1
        except:
            pass
    
    return {
        "replayed": replayed,
        "message": f"Moved {replayed} jobs back to pending queue"
    }


@app.get("/admin/processing-queue-check")
async def check_processing_queue(
    redis_client: RedisClient = Depends(get_redis_client)
):
    """
    CRITICAL: Check for stuck jobs in processing queue.
    If a job stays in processing for too long, the worker likely crashed.
    """
    processing = await redis_client.redis.lrange("email_processing", 0, -1)
    
    if not processing:
        return {"status": "ok", "message": "No jobs stuck in processing"}
    
    return {
        "status": "warning",
        "stuck_jobs": processing,
        "count": len(processing),
        "action": "Workers may have crashed. Check logs and restart workers."
    }