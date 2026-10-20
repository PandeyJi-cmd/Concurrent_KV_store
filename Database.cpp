#include "Database.h"
#include <chrono>
std::array<Shard,NUM_SHARDS> shards;

struct Shard& get_shard(std::string &key){
    size_t  h= std::hash<std::string>{}(key);
    return shards[h&(NUM_SHARDS -1)];
} 

uint64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

void set(std::string& key,std::string value,uint64_t ttl_ms){
    Shard& s = get_shard(key);
    std::lock_guard<std::mutex> lock(s.mtx);
    uint64_t exp = ttl_ms > 0 ? now_ms() + ttl_ms : 0;

    auto it = s.map.find(key);
    if (it == s.map.end()) {
        Node* n = new Node(key, value, exp);
        s.map.emplace(n->key, n);
        s.push_front(n);
    } else {
        Node* n1 = it->second;
        s.unlink(n1);
        n1->value = value;
        n1->expire_ms = exp;
        s.push_front(n1);
    }
    while (s.used_bytes > s.max_bytes && s.tail) {
        s.evict_node(s.tail);
    }
}

bool get(std::string &key,std::string&value){
    Shard& s = get_shard(key);
    std::lock_guard<std::mutex> lock(s.mtx);

    auto it = s.map.find(key);
    if (it == s.map.end()) {
        return false;
    }

    Node* n = it->second;
    if (n->expire_ms != 0 && now_ms() > n->expire_ms) {
        s.evict_node(n);
        return false;
    }
    s.unlink(n);
    s.push_front(n);
    value = n->value;
    return true;
}
bool del(std::string &key){
    Shard& s = get_shard(key);
    std::lock_guard<std::mutex> lock(s.mtx);

    auto it = s.map.find(key);
    if (it != s.map.end()) {
        s.evict_node(it->second);
        return true;
    }
    return false;
}

std::string stats(){
    uint64_t total_keys =0,total_bytes=0;
    for(int i=0;i<NUM_SHARDS;i++){
        std::lock_guard<std::mutex> lock(shards[i].mtx);
        total_keys+=shards[i].map.size();
        total_bytes+=shards[i].used_bytes;
    }
    return "+STATS keys = " + std::to_string(total_keys)+ " used_bytes =" +std::to_string(total_bytes) + "\r\n";
}