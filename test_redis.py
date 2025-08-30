import redis
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# Connect to Redis
r = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD")
)

# # Set key with expiry (e.g., 60 seconds)
# r.setex("greeting", 60, "Hello, Redis world!")

# # Get key back
# value = r.get("greeting")
# print(value.decode() if value else "Key expired or not found")



# Use pipeline - > avoid race conditions
pipe = r.pipeline()

# Create a hash + set expiry
pipe.hset("user:2000", mapping={
    "username": "eve",
    "email": "eve@site.com",
    "age": "28"
})
pipe.expire("user:2000", 60)

# Read and update inside same pipeline
pipe.hget("user:2000", "email")
pipe.hgetall("user:2000")
pipe.hincrby("user:2000", "age", 1)
pipe.hget("user:2000", "age")

# Execute all at once
results = pipe.execute()

print("Pipeline Results:")
print("hset result:", results[0])  # number of fields added
print("expire result:", results[1])  # True/False
print("email:", results[2].decode())  # decode b'...'
print("all fields:", {k.decode(): v.decode() for k, v in results[3].items()})
print("age after incr:", results[4])  # integer
print("age fetched again:", results[5].decode())
print("(with expiry of 60 seconds)")