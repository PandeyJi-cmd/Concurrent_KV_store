# Stage 1: Build the C++20 binary with -O3 optimizations
FROM gcc:13 AS builder
WORKDIR /build
COPY Database.h Database.cpp Server.cpp ./
RUN g++ -std=c++20 -O3 Server.cpp Database.cpp -o server -pthread

# Stage 2: Minimal production runtime image (strips out the 1.2GB compiler)
FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /build/server ./server
EXPOSE 9004
CMD ["./server"]
