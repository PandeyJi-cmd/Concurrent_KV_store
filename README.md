# High-Concurrency Epoll TCP Key-Value Store

A zero-dependency, event-driven in-memory TCP Key-Value Store engineered from scratch in **C++20** for Linux systems. Designed with **Edge-Triggered `epoll` (`EPOLLET`)**, **16-Way Hash-Striped Lock Sharding**, **Byte-Bounded $O(1)$ LRU Eviction**, and **Lazy TTL Expiration**.

## Key Systems Architecture

* **Event-Driven Non-Blocking I/O (`epoll`):** Multiplexes 1,000+ concurrent client connections on a single event loop using `epoll_create1`, `EPOLLET` (Edge-Triggered readiness), and `O_NONBLOCK` POSIX sockets—eliminating the 8 MB per-thread stack overhead of thread-per-connection servers.
* **Stateful TCP Stream Framing & DoS Guard:** Maintains per-descriptor accumulation buffers to safely handle fragmented TCP packets and pipelined command bursts while enforcing a strict `64 KB` per-client buffer ceiling against unbounded memory exhaustion attacks.
* **16-Way Hash-Striped Sharded Storage:** Partitions the keyspace across 16 independent shards (`std::array<Shard, 16>`) using single-cycle bitwise hash masking (`hash & 15`). Eliminates global lock contention during $O(1)$ doubly-linked list LRU pointer splicing on `GET` and `SET` operations.
* **Byte-Bounded LRU Eviction & Lazy TTL:** Tracks exact heap footprints (`sizeof(Node) + key.size() + value.size()`) per shard (`5 MB` per shard / `80 MB` total limit), evicting least-recently-used tail nodes in $O(1)$ time when capacity is reached, alongside monotonic clock (`std::chrono::steady_clock`) lazy TTL expiration.
* **Signal-Safe Network I/O:** Ignores kernel `SIGPIPE` signals (`SIG_IGN`) at startup to prevent abrupt client disconnects (`TCP RST`) from terminating the server process during batched socket writes.

## Supported Protocol Commands

| Command | Format | Description |
| :--- | :--- | :--- |
| **`SET`** | `SET <key> <val> [ttl_ms]` | Inserts or updates a key in $O(1)$ time with optional millisecond TTL. |
| **`GET`** | `GET <key>` | Retrieves a key's value (if not expired) and promotes it to the LRU head. |
| **`DEL`** | `DEL <key>` | Immediately unlinks and deallocates the key from its shard. |
| **`STATS`** | `STATS` | Returns live telemetry (`total_keys` and `used_bytes` across all 16 shards). |
| **`PING`** | `PING` | Health-check command returning `PONG`. |

## Build & Run

### 1. Native Linux Compilation
```bash
g++ -std=c++20 -O3 Server.cpp Database.cpp -o server -pthread
./server
```

### 2. Multi-Stage Docker Deployment
Build the lightweight production container image (`debian:bookworm-slim`) and expose port `6379`:
```bash
docker build -t tcp-kv-store:latest .
docker run -d -p 6379:6379 --name kv-server tcp-kv-store:latest
```

### 3. Interactive Client & Pipelining Test (`nc`)
Connect using Netcat on port `9004`:
```bash
nc 127.0.0.1 9004
SET session_1 harsh 10000
+OK
GET session_1
$harsh
STATS
+STATS keys = 1 used_bytes =102
PING
PONG
DEL session_1
+OK
```

Test TCP command pipelining in a single packet burst:
```bash
printf "SET k1 100\r\nSET k2 200\r\nGET k1\r\nSTATS\r\n" | nc -w 1 127.0.0.1 6379
```

### 4. Multi-Threaded Load Benchmark
Compile and run the multi-threaded benchmark generator (`bench.cpp`):
```bash
g++ -std=c++20 -O3 bench.cpp -o bench -pthread
./bench
```