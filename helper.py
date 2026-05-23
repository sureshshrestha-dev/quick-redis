
from database import Database, RedisClient, db, redisclient
def get_redis_client() -> RedisClient:
    return redisclient


def get_database() -> Database:
    return db

