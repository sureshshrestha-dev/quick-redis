from pymongo import AsyncMongoClient
import redis.asyncio as redis
import json
from typing import Optional

class Database:
    def __init__(self, uri="mongodb://localhost:27017", db_name="eko_setup_db"):
        self.client = AsyncMongoClient(uri)
        self.db = self.client[db_name]

    async def insert_one(self, collection_name: str, document: dict):
        collection = self.db[collection_name]
        result = await collection.insert_one(document)
        return result.inserted_id

    async def find_one(self, collection_name: str, query: dict, projection: dict = None):
        collection = self.db[collection_name]
        if projection:
            document = await collection.find_one(query, projection)
        else:
            document = await collection.find_one(query)
        return document

    async def update_one(self, collection_name: str, query: dict, update: dict):
        collection = self.db[collection_name]
        result = await collection.update_one(query, {'$set': update})
        return result.modified_count

    async def delete_one(self, collection_name: str, query: dict):
        collection = self.db[collection_name]
        result = await collection.delete_one(query)
        return result.deleted_count



class RedisClient:
    def __init__(self, host='localhost', port=6379, db=0, max_connections=50):
        # Connection pool for better concurrency handling
        # max_connections: Limit to prevent connection exhaustion
        pool = redis.ConnectionPool(
            host=host, 
            port=port, 
            db=db, 
            max_connections=max_connections,
            decode_responses=True
        )
        self.redis = redis.Redis(connection_pool=pool)

    async def get(self, key: str):
        data = await self.redis.get(key)
        return json.loads(data) if data else None

    async def set(self, key: str, value: dict, expire: int = 3600):
        await self.redis.set(key, json.dumps(value), ex=expire)
    
    async def delete(self, key: str):
        await self.redis.delete(key)
    
    async def delete_by_pattern(self, pattern: str):
        """Delete all keys matching a pattern using SCAN (production-safe for large keyspaces)"""
        cursor = 0
        deleted_count = 0
        while True:
            cursor, keys = await self.redis.scan(cursor, match=pattern, count=100)
            if keys:
                deleted_count += await self.redis.delete(*keys)
            if cursor == 0:
                break
        return deleted_count
    
    async def get_memory_info(self):
        """Get Redis memory usage for monitoring"""
        info = await self.redis.info('memory')
        return {
            'used_memory_mb': info['used_memory'] / 1024 / 1024,
            'max_memory_mb': info.get('maxmemory', 0) / 1024 / 1024 if info.get('maxmemory') else 'unlimited',
            'evicted_keys': info.get('evicted_keys', 0)
        }

db=Database()
redisclient = RedisClient()