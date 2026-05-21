
import asyncio
import redis.asyncio as redis

async def send_welcome_email(user_id):
    print(f"email  to user: {user_id}")

async def worker():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    print("Worker started. Waiting for jobs...")
    
    while True:
        # BLPOP blocks the connection until a job is available
        # It's highly efficient; it uses 0% CPU while waiting
        job = await r.blpop("email_queue")
        user_id = job[1]
        
        print(f"Processing job for: {user_id}")
        await send_welcome_email(user_id)

if __name__ == "__main__":
    asyncio.run(worker())