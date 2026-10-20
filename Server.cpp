#include "Database.h"
#include <sys/epoll.h>
#include <sys/socket.h>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <arpa/inet.h>
#include <unistd.h> // for close


#define MAX_EVENTS 1024 
#define PORT_NU 9004




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


void worker_event(){
    // create a socket for the server the epoll instances.
    int listen_fd = socket(AF_INET,SOCK_STREAM,0);
    if(listen_fd <0){
        perror("Socket was not created");
        return;
    }


    int val = 1;
    setsockopt(listen_fd,SOL_SOCKET,SO_REUSEADDR | SO_REUSEPORT,&val,sizeof(val));
    set_nonblocking(listen_fd);

    struct sockaddr_in addr{};
    addr.sin_family=AF_INET;
    addr.sin_port=htons(PORT_NU);
    addr.sin_addr.s_addr=htonl(INADDR_ANY);


    if(bind(listen_fd,reinterpret_cast<const sockaddr*>(&addr),sizeof(addr))<0){
        perror("Server was not bounded to the Port.");
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









}
int main(){
    // create the thread pool to execute the worker loop.

    std::vector<std::thread>workers;
    for(int i=0;i<NUM_SHARDS;i++){
        workers.emplace_back(worker_event);
    }
    for(int j=0;j<NUM_SHARDS;j++){
        workers[j].join();
    }
    return EXIT_SUCCESS;
}