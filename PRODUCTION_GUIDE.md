# Redis Production Guide

## Three Critical Things That Bite Developers After 6 Months

### 1. **Keyspaces: Smart Namespacing** ✅

Your app already does this correctly!

**Current keyspaces:**
- `user:{id}` - User cache
- `views:{user_id}` - View count tracking  
- `rate_limit:{user_id}` - Rate limiting counters

**Why this matters:**
- Prevents key collisions between different features
- Enables bulk operations on related keys
- Makes debugging and monitoring easier

**Production utilities in this app:**
```bash
# Clear all user caches
DELETE /admin/cache/all-users

# Clear specific user cache
DELETE /admin/cache/user/{user_id}

# Clear rate limits
DELETE /admin/cache/rate-limits
```

**For large-scale systems, use SCAN instead of KEYS:**
```python
# ✅ SAFE for production (iterates in chunks)
cursor = 0
while True:
    cursor, keys = await redis.scan(cursor, match="user:*", count=100)
    if keys:
        await redis.delete(*keys)
    if cursor == 0:
        break

# ❌ NEVER use in production (blocks Redis!)
# keys = await redis.keys("user:*")  # THIS LOCKS REDIS!
```

---

### 2. **Memory Eviction: Automatic Cache Cleanup** 🚨

**The Problem:**
Redis stores everything in RAM. When it runs out of memory, it crashes unless you tell it what to do.

**The Solution:**
Set this in your `redis.conf`:

```conf
maxmemory 256mb
maxmemory-policy allkeys-lru
```

**How it works:**
- `maxmemory 256mb` - Set Redis memory limit
- `allkeys-lru` - When full, delete the **Least Recently Used** key
  - This keeps your cache "hot" (frequently accessed data stays)
  - Similar to browser cache cleanup

**Alternative policies:**
| Policy | Behavior | Use Case |
|--------|----------|----------|
| `allkeys-lru` | Delete least recently used | ✅ Most common for caching |
| `allkeys-lfu` | Delete least frequently used | Better for real-world patterns |
| `volatile-lru` | Only evict keys with TTL | For mixed cache+persistent data |
| `allkeys-random` | Random deletion | When order doesn't matter |

**Monitor memory in production:**
```bash
# Check Redis memory usage anytime
GET /admin/redis-memory

# Response:
{
  "used_mb": 45.2,
  "max_mb": 256,
  "utilization_percent": 17.66,
  "evicted_keys": 0,
  "warning": null
}
```

**Setup Redis with memory config:**
```bash
# Start Redis with custom config
redis-server /path/to/redis.conf

# Or set at runtime
redis-cli CONFIG SET maxmemory 256mb
redis-cli CONFIG SET maxmemory-policy allkeys-lru
```

---

### 3. **Connection Exhaustion: Handle Concurrent Users** 🔌

**The Problem:**
FastAPI is async and handles many requests simultaneously. Each request needs a Redis connection. If you have 1000 concurrent users but only 10 connections available → crash!

**The Solution:**
Use a connection pool with proper sizing:

```python
# ✅ Production-ready connection pool
pool = redis.ConnectionPool(
    host='localhost',
    port=6379,
    db=0,
    max_connections=50,  # Adjust based on your load
    decode_responses=True
)
self.redis = redis.Redis(connection_pool=pool)
```

**How to size your pool:**
- **Development**: 10-20 connections
- **Small production**: 30-50 connections
- **Medium production**: 50-100 connections
- **Large production**: 100+ connections

**Formula:** `max_connections = (peak_concurrent_users * avg_redis_requests_per_request) + buffer`

Example:
- 100 concurrent users
- Each request makes 3 Redis calls (cache check, set, incr)
- Buffer of 10
- **Total: (100 × 3) + 10 = 310 connections**

**Monitor connection usage:**
```bash
# In production, watch this metric
redis-cli INFO stats | grep connected_clients

# Example output:
# connected_clients:45 (out of max_connections=50)
```

---

## Checklist for Production Deployment

- [ ] Set `maxmemory` and `maxmemory-policy` in redis.conf
- [ ] Configure connection pool with appropriate `max_connections`
- [ ] Use keyspace namespacing (you're already doing this!)
- [ ] Use SCAN instead of KEYS for bulk operations
- [ ] Set up monitoring endpoint (`/admin/redis-memory`)
- [ ] Enable persistence (RDB or AOF) if data is important
- [ ] Use slowlog to catch performance issues
- [ ] Set TCP backlog appropriately for your scale
- [ ] Bind Redis to localhost only (security!)
- [ ] Set up alerts when memory usage > 80%

---

## Testing Your Setup

```bash
# Test rate limiting (5 requests per 10 seconds)
curl http://localhost:8000/user/v2/123 -v
curl http://localhost:8000/user/v2/123 -v
curl http://localhost:8000/user/v2/123 -v
curl http://localhost:8000/user/v2/123 -v
curl http://localhost:8000/user/v2/123 -v
curl http://localhost:8000/user/v2/123 -v  # Should hit rate limit

# Monitor memory
curl http://localhost:8000/admin/redis-memory

# Bulk cache clearing
curl -X DELETE http://localhost:8000/admin/cache/all-users
```

---

## Real-World Issue Examples

### Issue 1: Memory Exhaustion (After 6 months in production)
**Symptom:** "Redis stopped responding, crashes randomly"
**Root Cause:** No `maxmemory-policy` set, Redis ran out of RAM
**Fix:** Add `maxmemory-policy allkeys-lru` to redis.conf

### Issue 2: Slow Performance Under Load
**Symptom:** "API works fine with 10 users, but dies at 100 users"
**Root Cause:** Connection pool too small
**Fix:** Increase `max_connections` in RedisClient

### Issue 3: "Redis is full but nothing is getting cleaned up"
**Symptom:** Memory keeps growing, no evictions happening
**Root Cause:** Keys have no TTL and `maxmemory-policy` not set
**Fix:** Always set TTL on cache keys + configure eviction policy

---

## Further Reading

- [Redis Memory Management](https://redis.io/topics/memory-optimization)
- [Redis Eviction Policies](https://redis.io/topics/lru-cache)
- [Redis Connection Pooling](https://redis-py.readthedocs.io/en/stable/connections.html)
- [Redis Persistence](https://redis.io/topics/persistence)
