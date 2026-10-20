#ifndef DATABASE
#define DATABASE

constexpr int NUM_SHARDS = 16;
constexpr uint64_t MAX_SIZE_PER_SHARD = 5ULL * 1024 * 1024;

#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <mutex>
#include <array>
#include <cstdint>
struct Node {
    std::string key;
    std::string value;
    uint64_t expire_ms;
    Node *prev =nullptr;
    Node *next =nullptr;
    Node(std::string k1,std::string v,Node* p=nullptr,Node*r=nullptr){
        key=k1;
        value=v;
        prev=p;
        next=r;
    }
    size_t byte_size() const{
        return sizeof(Node) + key.size() + value.size();
    }
}; // node

struct alignas(64) Shard{
    std::mutex mtx;
    struct Node* head=nullptr;
    struct Node* tail=nullptr;
    std::unordered_map<std::string,Node*>map;
    uint64_t max_bytes=MAX_SIZE_PER_SHARD;
    uint64_t used_bytes=0;
    ~Shard(){
        while(head){
            Node* tmp=head;
            head=head->next;
            delete tmp;
        }
    }
    void unlink(Node *n){
        if(n->prev) n->prev->next = n->next;
        else head =n->next;
        if(n->next) n->next->prev = n->prev;
        else tail=n->prev;
        n->prev=n->next=nullptr;
        used_bytes-=n->byte_size();        
    }
    void push_front(Node* n){
        n->next=head;
        n->prev=nullptr;
        if(head) head->prev = n;
        head = n;
        if(!tail) tail=n;
        used_bytes+=n->byte_size();
    }
    void evict_node(Node *n){
        unlink(n);
        map.erase(n->key);
        delete n;
    }
};




void set(std::string& key, std::string value, uint64_t ttl_ms = 0);
bool get(std::string &key, std::string &value);
bool del(std::string&);
std::string stats();

#endif  // DATABASE