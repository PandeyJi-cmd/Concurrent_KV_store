#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h> // for error
#include <unistd.h>
#include <arpa/inet.h> //for Networking
#include <sys/socket.h> // for Networking
#include <netinet/ip.h> // for Networking
#include <string> // for string
#include <sstream> // for sstringstream parsing
#include "Database.h" //the thread safe database that was created
#include <thread> // to handle multiple clients
#include <functional> // to use reference to create reference.
#include <error.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <signal.h>


#define SA struct sockaddr

#define MAX_CLIENT 1024 

#define PORT_NU 9004
void die(const char* msg){
    perror(msg);
    exit(EXIT_FAILURE);
}
std::string execute_command(std::string line){
    // saved in shards;
    std::istringstream iss(line);
    std::string cmd;
    if(!(iss>>cmd)) return "";
    if(cmd == "SET"){
        std::string key,value;
        uint64_t ttl =0;
        if(!(iss>>key>>value)) return "-Err give the arguments of Set: SET <key> <value> [ttl_ms]\r\n";
        iss>>ttl;
        set(key,value,ttl);
        return "+OK\r\n";
    }
    else if(cmd =="GET"){
        std::string key,out_val;
        if(!(iss>>key)) return "-Err give the arguments of Get: GET <key> \r\n";
        if(get(key,out_val)){
            return "$" + out_val +"\r\n";
        }
        return "$-1 (NOT FOUND) \r\n";
    }
    else if(cmd=="DEL"){
        std::string key;
        if(!(iss>>key)) return "-Err give the arguments of Del : DEL <key>\r\n";
        if(del(key)){
            return "+OK\r\n";
        }
        else{
            return "-OK but not found the key\r\n";
        }
    }
    else if(cmd == "PING"){
        return "PONG\r\n";
    }
    else if(cmd == "STATS"){
        return stats();
    }
    return "-Err unkown command\r\n";
    
    
}
void set_nonblocking(int fd){
    int flags = fcntl(fd,F_GETFL,0);
    fcntl(fd,F_SETFL,flags|O_NONBLOCK);
}
int main(){
    signal(SIGPIPE,SIG_IGN);
    int fd=socket(AF_INET,SOCK_STREAM,0); // IPv4 + TCP
    if(fd < 0){
        die("SOCKET IS NOT FORMED");
    }

    int val=1;
    setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&val,sizeof(val)); // SO_REUSEADDR MAKES SURE THAT WE CAN USE THE PORT AFTER THE OPRATION OR THE MAIN CODE ENDS.
    set_nonblocking(fd);

    struct sockaddr_in addr={};
    addr.sin_family= AF_INET;
    addr.sin_port=htons(PORT_NU); // for port number .. in big endian
    addr.sin_addr.s_addr=htonl(0); //for ip in big endian most significant first network byte order

    int rv=bind(fd,(const struct sockaddr* )&addr,sizeof(addr));
    if(rv) { die("NOT BOUNDED");}
    int rv_listen = listen(fd,SOMAXCONN);
    printf("The server is Listening \n");
    int epollfd = epoll_create1(0);
    struct epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = fd;

    int ex = epoll_ctl(epollfd,EPOLL_CTL_ADD,fd,&ev); // add the listening socket to the epoll instance
    if(ex < 0 ) die("Error");

    struct epoll_event events[MAX_CLIENT];
    std::unordered_map<int,std::string>client_buffers;


    while(true){
        int nfds = epoll_wait(epollfd,events,MAX_CLIENT,-1);
        if(nfds<0){
            if(errno == EINTR) continue;
            die("epoll_wait()");
        }
        for(int i=0;i<nfds;i++){
            int c_fd = events[i].data.fd;
            if(c_fd == fd){
                // maybe a new connection.
                while(true){
                    struct sockaddr_in client{};
                    socklen_t client_size = sizeof(client);
                    int conn_fd = accept(fd,(SA*)&client,&client_size);
                    if(conn_fd <0 ){
                        if(errno == EAGAIN || errno == EWOULDBLOCK) break;
                        perror("accept");
                        break;
                    }
                    set_nonblocking(conn_fd);


                    struct epoll_event client_ev{};
                    client_ev.events = EPOLLIN | EPOLLET;
                    client_ev.data.fd= conn_fd;
                    epoll_ctl(epollfd,EPOLL_CTL_ADD,conn_fd,&client_ev);
                }
            }
            else{
                // the old socket may be listening
                char buff[4096]; // read 4kb in one go.

                bool close_conn = false;
                while(true){
                    if(client_buffers[c_fd].size()>65536){
                        close_conn=true;
                        break;
                    }
                    ssize_t bytes =read(c_fd,buff,sizeof(buff));
                    if(bytes> 0){
                        client_buffers[c_fd].append(buff,bytes);
                    }
                    else  if(bytes == 0){
                        close_conn =true;
                        break;
                    }
                    else{
                        if(errno == EAGAIN || errno == EWOULDBLOCK) break;
                        close_conn = true;
                        break;
                    }
                }
                std::string& in_buf = client_buffers[c_fd];
                size_t pos = 0;
                std::string out_batch;
                while((pos=in_buf.find('\n'))!=std::string::npos){
                    std::string line = in_buf.substr(0,pos);
                    in_buf.erase(0,pos+1);
                    if(!line.empty() && line.back()=='\r') line.pop_back();
                    out_batch += execute_command(line);
                }
                if(!out_batch.empty()){
                    ssize_t written = write(c_fd,out_batch.data(),out_batch.size());
                }
                if(close_conn){
                    epoll_ctl(epollfd,EPOLL_CTL_DEL,c_fd,nullptr);
                    close(c_fd);
                    client_buffers.erase(c_fd);
                }
            }
        }
    }
    close(fd);
    return 0;
}

//tokenizer
//
