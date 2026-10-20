#include "Database.h"
#include <sys/epoll.h>
#include <sys/socket.h>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <arpa/inet.h>
#include <unistd.h> // for close
#include <errno.h>
#include <string>
#include <sstream>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <signal.h>
#include <atomic>

#define MAX_EVENTS 1024 
#define PORT_NU 9004
#define MAX_CLIENT_BUF 65536

std::atomic<bool> stop_server{false};


struct ClientState{
    std::string in_buf;
    std::string out_buf;
    bool want_write = false;
};




void set_nonblocking(int sock_fd){
    int flags = fcntl(sock_fd,F_GETFL,0);
    if(flags!=-1){
        fcntl(sock_fd,F_SETFL,flags|O_NONBLOCK);
    }
}

std::string execute_command(const std::string& line) {
    std::istringstream iss(line);
    std::string cmd;
    if (!(iss >> cmd)) return "";

    if (cmd == "SET") {
        std::string key, value;
        uint64_t ttl = 0;
        if (!(iss >> key >> value)) return "-Err usage: SET <key> <value> [ttl_ms]\r\n";
        iss >> ttl;
        set(key, value, ttl);
        return "+OK\r\n";
    } else if (cmd == "GET") {
        std::string key, out_val;
        if (!(iss >> key)) return "-Err usage: GET <key>\r\n";
        if (get(key, out_val)) {
            return "$" + out_val + "\r\n";
        }
        return "$-1 (NOT FOUND)\r\n";
    } else if (cmd == "DEL") {
        std::string key;
        if (!(iss >> key)) return "-Err usage: DEL <key>\r\n";
        return del(key) ? "+OK\r\n" : "-OK key not found\r\n";
    } else if (cmd == "PING") {
        return "+PONG\r\n";
    } else if (cmd == "STATS") {
        return stats();
    }
    return "-Err unknown command\r\n";
}

bool flush_client_output(int epollfd, int c_fd, ClientState& st) {
    size_t sent = 0;
    while (sent < st.out_buf.size()) {
        ssize_t n = write(c_fd, st.out_buf.data() + sent, st.out_buf.size() - sent);
        if (n > 0) {
            sent += static_cast<size_t>(n);
        } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            break;
        } else {
            return false; // Write error, close connection
        }
    }
    if (sent > 0) {
        st.out_buf.erase(0, sent);
    }

    bool need_write = !st.out_buf.empty();
    if (need_write != st.want_write) {
        st.want_write = need_write;
        struct epoll_event ev{};
        ev.events = EPOLLIN | EPOLLET | (need_write ? EPOLLOUT : 0);
        ev.data.fd = c_fd;
        epoll_ctl(epollfd, EPOLL_CTL_MOD, c_fd, &ev);
    }
    return true;
}



void worker_event(){
    // create a socket for the server the epoll instances.
    int listen_fd = socket(AF_INET,SOCK_STREAM,0);
    if(listen_fd <0){
        perror("Socket was not created");
        return;
    }
    int val = 1;
    setsockopt(listen_fd,SOL_SOCKET,SO_REUSEADDR,&val,sizeof(val));
    setsockopt(listen_fd,SOL_SOCKET,SO_REUSEPORT,&val,sizeof(val));
    
    set_nonblocking(listen_fd);

    struct sockaddr_in addr{};
    addr.sin_family=AF_INET;
    addr.sin_port=htons(PORT_NU);
    addr.sin_addr.s_addr=htonl(INADDR_ANY);


    if(bind(listen_fd,reinterpret_cast<const sockaddr*>(&addr),sizeof(addr))<0){
        perror("Server was not bounded to the Port.");
        close(listen_fd);
        return;
    }
    if (listen(listen_fd, SOMAXCONN) < 0) {
        perror("listen");
        close(listen_fd);
        return;
    }

    int epollfd = epoll_create1(EPOLL_CLOEXEC);
    if (epollfd < 0) {
        perror("epoll_create1");
        close(listen_fd);
        return;
    }


    struct epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = listen_fd;
    epoll_ctl(epollfd, EPOLL_CTL_ADD, listen_fd, &ev);

    struct epoll_event events[MAX_EVENTS];
    std::unordered_map<int, ClientState> clients;

    auto close_client=[&](int fd){
        epoll_ctl(epollfd,EPOLL_CTL_DEL,fd,nullptr);
        close(fd);
        clients.erase(fd);
    };
    while(!stop_server){
        int num = epoll_wait(epollfd,events,MAX_EVENTS,200);
        if(num<0){
            if(errno == EINTR) continue;
            perror("epoll wait failed");
            break;
        }
        for(int i=0;i<num;i++){
            int c_fd = events[i].data.fd;
            uint32_t mask = events[i].events;

            if(c_fd == listen_fd){
                while(true){
                    // new client 
                    struct sockaddr_in c_addr{};
                    socklen_t client_len = sizeof(c_addr);
                    int conn_fd;
                    if((conn_fd =accept(listen_fd,reinterpret_cast<struct sockaddr*>(&c_addr),&client_len))<0){
                        if(errno == EAGAIN || errno == EWOULDBLOCK){
                            break;
                        }
                        perror("Cant connect");
                        break;
                    }
                    set_nonblocking(conn_fd);
                    struct epoll_event client_ev{};
                    client_ev.events = EPOLLIN | EPOLLET;
                    client_ev.data.fd = conn_fd;
                    epoll_ctl(epollfd, EPOLL_CTL_ADD, conn_fd, &client_ev);
                    clients.try_emplace(conn_fd);
                }
                continue;
            }
            if(mask & (EPOLLERR | EPOLLHUP)){
                close_client(c_fd);
                continue;
            }
            
            // else it is a client that wants some action to be done.
            auto it = clients.find(c_fd);
            if(it == clients.end()) continue;
            ClientState& st = it->second;
            bool close_conn = false;
            bool overflow = false;
            

            if(mask & EPOLLIN){
                char buffer[4096];
                while(true){
                    ssize_t bytes = read(c_fd, buffer, sizeof(buffer));
                    if(bytes > 0){
                        st.in_buf.append(buffer, static_cast<size_t>(bytes));
                        if (st.in_buf.size() > MAX_CLIENT_BUF) {
                            overflow = true;
                            break;
                        }
                        continue;
                    }
                    if(bytes == 0){
                        close_conn = true;
                        break;
                    }
                    if(errno == EINTR) continue;
                    if(errno == EAGAIN || errno == EWOULDBLOCK) break;
                    close_conn = true;
                    break;
                }
            }
            if(overflow) {
                close_client(c_fd);
                continue;
            }

            size_t start = 0,pos=0;
            while((pos = st.in_buf.find('\n',start)) != std::string::npos){
                std::string line = st.in_buf.substr(start,pos-start);
                if(!line.empty() && line.back()=='\r') line.pop_back();
                st.out_buf += execute_command(line);
                start = pos+1;
            }
            if(start >0){
                st.in_buf.erase(0,start);
            }

            if(!st.out_buf.empty() || (mask & EPOLLOUT)){
                if(!flush_client_output(epollfd,c_fd,st)){
                    close_conn=true;
                }
            }
            if(close_conn){
                close_client(c_fd);
            }
            
        }
    }



    for(auto& c:clients){
        close(c.first);
    }
    close(epollfd);
    close(listen_fd);




}


void handle_shutdown(int){
    stop_server=1;
}

int main(){
    // create the thread pool to execute the worker loop.
    signal(SIGPIPE,SIG_IGN);
    signal(SIGINT,handle_shutdown);
    signal(SIGTERM,handle_shutdown);

    unsigned int num_workers = 4;
    std::vector<std::thread>workers;
    for(unsigned int i=0;i<num_workers;i++){
        workers.emplace_back(worker_event);
    }
    for(unsigned int j=0;j<num_workers;j++){
        workers[j].join();
    }
    
    return EXIT_SUCCESS;
}