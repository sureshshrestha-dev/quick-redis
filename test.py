# test.py
from database import redisclient

await redisclient.set("user:1", {"name": "Alice", "email": "alice@example.com"})
user = await redisclient.get("user:1")
print(user)  # {'name': 'Alice', 'email': 'alice@example.com'}