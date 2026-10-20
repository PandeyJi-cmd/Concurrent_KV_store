# High-Concurrency TCP Key-Value Store

A zero-dependency, high-performance in-memory database built from scratch in **C++20** for Linux systems. It is engineered to handle thousands of simultaneous client connections with ultra-low latency, strict memory limits, and multi-threaded scaling.

## Core Architecture

* **Multi-Threaded Event-Driven Design:** Utilizes concurrent worker loops to manage thousands of active client connections simultaneously. It processes network reads and writes asynchronously, ensuring that slow clients never block or slow down the system.
* **16-Way Hash-Striped Storage:** Splits the database keyspace across 16 independent sections (shards), each with its own dedicated lock. This dramatically reduces waiting time and allows multiple threads to read and write data in parallel.
* **Smart Memory Capping (LRU Eviction & TTL):** Tracks exact RAM usage per shard (strictly capped at 5 MB per shard / 80 MB total). When capacity is reached, it automatically removes the least-recently-used items. It also supports automatic time-based expiration (TTL) for keys.
* **Safe Stream Framing & Security:** Safely processes fragmented network packets and command batching (pipelining) while enforcing strict per-client buffer limits to protect against memory-exhaustion attacks.
* **Resilient Connection Handling:** Designed to gracefully handle abrupt client disconnects and network interruptions without crashing the server.

## Supported Protocol Commands

| Command | Format | Description |
| :--- | :--- | :--- |
| **`SET`** | `SET <key> <val> [ttl_ms]` | Inserts or updates a key with an optional millisecond expiration time. |
| **`GET`** | `GET <key>` | Retrieves a key's value (if not expired) and refreshes its priority. |
| **`DEL`** | `DEL <key>` | Immediately removes and deletes a key from the database. |
| **`STATS`** | `STATS` | Returns live telemetry, including total active keys and memory usage. |
| **`PING`** | `PING` | Health-check command returning `+PONG`. |

## Build & Run

### 1. Native Linux Compilation
```bash
g++ -std=c++20 -O3 -Wall -Wextra Server.cpp Database.cpp -o server -pthread
./server
```

### 2. Multi-Stage Docker Deployment
Build the lightweight production container image and publish port `9004`:
```bash
docker build -t tcp-kv-store:latest .
docker run -d -p 9004:9004 --name kv-server tcp-kv-store:latest
```

### 3. Interactive Client Test (`nc`)
Connect using Netcat on port `9004`:
```bash
nc 127.0.0.1 9004
SET session_1 harsh 10000
+OK
GET session_1
$harsh
STATS
+STATS keys = 1 used_bytes = 102
PING
+PONG
DEL session_1
+OK
```

### 4. Multi-Threaded Load Benchmark
Compile and run the multi-threaded load generator (`bench.cpp`) to test performance:
```bash
g++ -std=c++20 -O3 bench.cpp -o bench -pthread
./bench