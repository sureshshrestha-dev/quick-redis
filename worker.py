
import asyncio
import redis.asyncio as redis
import json
from datetime import datetime

PENDING_QUEUE = "email_queue"         
PROCESSING_QUEUE = "email_processing"   
FAILED_QUEUE = "email_failed"

async def welcome_email(user_id):
    print(f"Email sent to: {user_id}")

async def worker():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    while True:
        try:
            #  Use BRPOPLPUSH for reliable delivery , moves automatically job from "pending" to "processing" and iff the worker crashes, the job is still in "processing"
            job = await r.brpoplpush(PENDING_QUEUE, PROCESSING_QUEUE, timeout=1)
            
            if not job:
                continue  
            
            user_id = job
            print(f"doing Job: {user_id}")
            try:
                await welcome_email(user_id)
                await r.lrem(PROCESSING_QUEUE, 1, user_id)
                print(f" Job {user_id} done")
                
            except Exception as e:
                error_entry = {
                    "user_id": user_id,
                    "error": str(e),
                    "timestamp": datetime.now(),
                    "attempt_count": 1
                }
                
                await r.lpush(FAILED_QUEUE, json.dumps(error_entry))
                await r.lrem(PROCESSING_QUEUE, 1, user_id)
                
                print(f"error Job: {user_id} {e} \n\n {FAILED_QUEUE}")
        
        except asyncio.CancelledError:
            break
        except Exception as e:
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(worker())