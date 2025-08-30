from flask import Flask, jsonify
import mysql.connector
import redis
import time
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)

# MySQL Connection
mysql_conn = mysql.connector.connect(
    host=os.getenv("MYSQL_HOST"),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database=os.getenv("MYSQL_DATABASE")
)
cursor = mysql_conn.cursor()

# Redis Connection
redis_client = redis.StrictRedis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    password=os.getenv("REDIS_PASSWORD"),
    decode_responses=True
)

# --- Setup Table & Sample Data ---
def setup_database():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY AUTO_INCREMENT,
            name VARCHAR(100),
            email VARCHAR(100)
        )
    """)
    mysql_conn.commit()

    # Check if users already exist
    cursor.execute("SELECT COUNT(*) FROM users")
    count = cursor.fetchone()[0]

    if count < 100:  # insert only if not enough data
        print("Inserting sample users...")
        cursor.executemany(
            "INSERT INTO users (name, email) VALUES (%s, %s)",
            [(f"User{i}", f"user{i}@example.com") for i in range(1, 101)]
        )
        mysql_conn.commit()
        print("Inserted 100 users.")

setup_database()  # run at startup

# --- API Route ---
@app.route("/user/<int:user_id>")
def get_user(user_id):
    start_time = time.time()

    cache_key = f"user:{user_id}"

    # Step 1: Try Redis Cache
    user_data = redis_client.get(cache_key)
    if user_data:
        latency = (time.time() - start_time) * 1000
        return jsonify({
            "source": "redis_cache",
            "data": json.loads(user_data),
            "latency_ms": latency
        })

    # Step 2: If not in cache, get from MySQL
    cursor = mysql_conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
    row = cursor.fetchone()

    if row:
        # Store in Redis for 60s
        redis_client.setex(cache_key, 60, json.dumps(row))
        latency = (time.time() - start_time) * 1000
        return jsonify({
            "source": "mysql_db",
            "data": row,
            "latency_ms": latency
        })
    else:
        latency = (time.time() - start_time) * 1000
        return jsonify({"error": "User not found", "latency_ms": latency})


@app.route("/users", methods=["GET"])
def get_all_users():
    start_time = time.time()

    cache_key = "all_users"

    # Step 1: Try Redis Cache
    users_data = redis_client.get(cache_key)
    if users_data:
        latency = (time.time() - start_time) * 1000
        return jsonify({
            "source": "redis_cache",
            "data": json.loads(users_data),
            "latency_ms": latency
        })

    # Step 2: If not in cache, get from MySQL
    cursor = mysql_conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM users")
    rows = cursor.fetchall()

    if rows:
        # Store in Redis for 60s
        redis_client.setex(cache_key, 60, json.dumps(rows))
        latency = (time.time() - start_time) * 1000
        return jsonify({
            "source": "mysql_db",
            "data": rows,
            "latency_ms": latency
        })
    else:
        latency = (time.time() - start_time) * 1000
        return jsonify({"error": "No users found", "latency_ms": latency})




if __name__ == "__main__":
    app.run(debug=True)
