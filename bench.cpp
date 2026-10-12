#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <string>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <signal.h>

constexpr int PORT = 9004;
constexpr int NUM_THREADS = 16;          // 16 concurrent client threads
constexpr int REQS_PER_THREAD = 10000;   // 160,000 total operations
constexpr int PIPELINE_BATCH = 50;       // 50 pipelined commands per TCP write

std::atomic<uint64_t> completed_ops{0};

void client_worker(int thread_id) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("connect (make sure ./server is running first!)");
        close(fd);
        return;
    }

    char recv_buf[65536];

    for (int i = 0; i < REQS_PER_THREAD; i += PIPELINE_BATCH) {
        std::string batch;
        batch.reserve(PIPELINE_BATCH * 32);

        for (int b = 0; b < PIPELINE_BATCH; ++b) {
            int key_id = (thread_id * REQS_PER_THREAD) + i + b;
            if (b % 2 == 0) {
                batch += "SET key_" + std::to_string(key_id) + " val_" + std::to_string(key_id) + " 60000\r\n";
            } else {
                batch += "GET key_" + std::to_string(key_id - 1) + "\r\n";
            }
        }

        // Handle partial TCP writes safely
        size_t total_sent = 0;
        while (total_sent < batch.size()) {
            ssize_t sent = send(fd, batch.data() + total_sent, batch.size() - total_sent, 0);
            if (sent <= 0) break;
            total_sent += sent;
        }

        // Read responses until all PIPELINE_BATCH newline-delimited replies arrive
        int newlines = 0;
        while (newlines < PIPELINE_BATCH) {
            ssize_t n = recv(fd, recv_buf, sizeof(recv_buf), 0);
            if (n <= 0) break;
            for (ssize_t k = 0; k < n; ++k) {
                if (recv_buf[k] == '\n') newlines++;
            }
        }
        completed_ops.fetch_add(PIPELINE_BATCH, std::memory_order_relaxed);
    }

    close(fd);
}

int main() {
    signal(SIGPIPE, SIG_IGN);

    std::cout << "[*] Starting benchmark: " << NUM_THREADS << " threads, "
              << (NUM_THREADS * REQS_PER_THREAD) << " total operations (50% SET, 50% GET)...\n";

    auto start = std::chrono::steady_clock::now();

    std::vector<std::thread> threads;
    threads.reserve(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back(client_worker, i);
    }
    for (auto& t : threads) {
        t.join();
    }

    auto end = std::chrono::steady_clock::now();
    double elapsed_sec = std::chrono::duration<double>(end - start).count();
    uint64_t total = completed_ops.load();
    double throughput = total / elapsed_sec;
    double avg_latency_us = (elapsed_sec * 1e6) / (total / NUM_THREADS);

    std::cout << "========================================\n";
    std::cout << "Total Operations : " << total << "\n";
    std::cout << "Elapsed Time     : " << elapsed_sec << " sec\n";
    std::cout << "Throughput       : " << static_cast<uint64_t>(throughput) << " ops/sec\n";
    std::cout << "Avg Latency/Op   : " << avg_latency_us << " microseconds\n";
    std::cout << "========================================\n";
    return 0;
}