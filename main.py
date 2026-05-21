from fastapi import FastAPI, HTTPException,Depends
from pydantic import BaseModel
from bson import ObjectId

from database import db, redisclient
app = FastAPI(prefix="/user")

class User(BaseModel):
    username: str
    role: str
    
@app.post("/")
async def create_user(user: User):
    check=await db.find_one("users", {"username": user.username})
    if check:
        raise HTTPException(status_code=400, detail="Username already exists")

    user_id = await db.insert_one("users", user.dict())
    if user_id:
        await redisclient.redis.rpush("email_queue", str(user_id))
        return {"user_id": str(user_id)}
    else:
        raise HTTPException(status_code=500, detail="User creation failed")





@app.get("/{user_id}")
async def read_user(user_id: str):
    cache_key = f"user:{user_id}"
    cached_user = await redisclient.get(cache_key)
    if cached_user:
        print("Cache Hit!", cached_user)
        return cached_user
        
    print("Cache Miss, querying DB...")
    user = await db.find_one("users", {"_id": ObjectId(user_id)}, {"username": 1, "role": 1})
    if user:
        user['_id'] = str(user['_id'])
        await redisclient.set(cache_key, user, expire=60)
        return user
    
    raise HTTPException(status_code=404, detail="User not found")

@app.put("/{user_id}")
async def update_user(user_id: str, user: User):
    check=await db.find_one("users", {"_id": ObjectId(user_id)})
    if not check:
        raise HTTPException(status_code=404, detail="User not found")
    result = await db.update_one("users", {"_id": ObjectId(user_id)}, user.dict())
    if result:
        cache_key = f"user:{user_id}"
        await redisclient.delete(cache_key)
        print("Cache Updated!", user.dict())
        return "User cleared successfully"
    raise HTTPException(status_code=404, detail="User not found")

@app.delete("/{user_id}")
async def delete_user(user_id: str):
    check=await db.find_one("users", {"_id": ObjectId(user_id)})
    if not check:
        return "User not found"
    result = await db.delete_one("users", {"_id": ObjectId(user_id)})
    if result:
        cache_key = f"user:{user_id}"
        await redisclient.redis.delete(cache_key)
        return "User deleted successfully"
    raise HTTPException(status_code=404, detail="User not found")


async def dummye_rate_limiter(user_id: str):
    limit = 5
    seconds = 10
    key = f"rate_limit:{user_id}"
    current = await redisclient.redis.get(key)
    if current and int(current) >= limit:  
        return True
    else:
        pipe = redisclient.redis.pipeline()
        pipe.incr(key, 1)
        pipe.expire(key, seconds)
        await pipe.execute()
        return False
    

@app.get("/v2/{user_id}")
async def read_user(user_id: str ,rate_limit=Depends(dummye_rate_limiter)):
    # if await dummye_rate_limiter(user_id):
    #     raise HTTPException(status_code=429, detail="Too many requests")
    
    view_key = f"views:{user_id}"
    view_count = await redisclient.redis.incr(view_key)
    if view_count == 1:
        await redisclient.redis.expire(view_key, 20)

    cache_key = f"user:{user_id}"
    cached_user = await redisclient.get(cache_key)
    if cached_user:
        print(f"Cache Hited Views: {view_count}")
        cached_user["view_count"] = view_count  # Add real-time view count
        return cached_user
        
    print(f"Cache Missed Views: {view_count}. querying DB...")
    user = await db.find_one(
        "users", 
        {"_id": ObjectId(user_id)}, 
        {"username": 1, "role": 1}
    )
    
    if user:
        user['_id'] = str(user['_id'])
        user['view_count'] = view_count
        await redisclient.set(cache_key, user, expire=60)
        return user
    
    raise HTTPException(status_code=404, detail="User not found")


@app.delete("/cache/{user_id}")
async def clear_user_cache(user_id: str):
    pattern = f"user:{user_id}"
    deleted = await redisclient.delete_by_pattern(pattern)
    return {"message": f"deleted {deleted} cache pf {user_id}"}


@app.delete("/cache-clear")
async def clear_all_user_cache():
    deleted = await redisclient.delete_by_pattern("user:*")
    return {"message": f"cleared {deleted} user cache"}


@app.delete("/rate-limits")
async def clear_rate_limits():
    deleted = await redisclient.delete_by_pattern("rate_limit:*")
    return {"message": f"delete {deleted} rate limit"}


@app.get("/admin/redis-memory")
async def check_redis_memory():
    """Monitor Redis memory usage (alerts if approaching limit)"""
    memory = await redisclient.get_memory_info()
    
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