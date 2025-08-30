import redis
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Connect to Redis
r = redis.Redis(
    host=os.getenv("REDIS_HOST", "127.0.0.1"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD")
)

# Set key with expiry (e.g., 60 seconds)
r.setex("greeting", 60, "Hello, Redis world!")

# Get key back
value = r.get("greeting")
print(value.decode() if value else "Key expired or not found")
