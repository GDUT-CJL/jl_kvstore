#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <sys/time.h>
#define ENABLE_ARRAY_TEST   1
#define ENABLE_RBTREE_TEST   1
#define ENABLE_HASHTABLE_TEST   1

#define MAX_CONNECT_NUM			100000
#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)

#define MAX_BUFFER_SIZE     1024
char buffer[MAX_BUFFER_SIZE] = {0};
int Connect_Server(const char* ip,int port){
    int connfd = socket(AF_INET,SOCK_STREAM,0);

    struct sockaddr_in kvs_addr;
    memset(&kvs_addr,0,sizeof(kvs_addr));
    kvs_addr.sin_family = AF_INET;
    kvs_addr.sin_addr.s_addr = inet_addr(ip);
    kvs_addr.sin_port = htons(port);

    int ret = connect(connfd,(struct sockaddr*)&kvs_addr,sizeof(struct sockaddr));
    if(ret != 0){
        perror("connect");
        return -1;
    }
    return connfd;
}

int send_msg(int connfd,char* msg){
    int ret = send(connfd,msg,strlen(msg),0);
    if(ret == -1){
        perror("send:");
        exit(-1);
    }
    return ret;
}

int recv_msg(int connfd, char* msg){
    int ret = recv(connfd,msg,MAX_BUFFER_SIZE,0);
    if(ret == -1){
        perror("recv:");
        exit(-1);
    }
    msg[ret] = '\0';
    return ret;
}

int equal(char* res,char* pattern,char* casename){
    if(strcmp(res,pattern) == 0){
        //printf("PASS -----> '%s'\n",casename);
    }else{
        printf("NO PASS -----> %s != %s\n",res,pattern);
    }
}

int test_case(int connfd,char* cmd, char* pattern,char* casename){
    int ret = send_msg(connfd,cmd);
    if(ret == -1) return -1;
    recv_msg(connfd,buffer);
    if(ret == -1) return -1;

    equal(buffer,pattern,casename);
}

void array_connect_10w(int connfd){
    int i;
    for(i = 0; i < MAX_CONNECT_NUM ; ++i){
        char msg[512] = {0};
        snprintf(msg,512,"set key%d value%d",i,i);
        test_case(connfd,msg,"OK\n","SetName");
    }
}

void rbtree_connect_10w(int connfd){
    int i;
    for(i = 0; i < MAX_CONNECT_NUM ; ++i){
        char msg[512] = {0};
        snprintf(msg,512,"rset key%d value%d",i,i);
        test_case(connfd,msg,"OK\n","SetName");
    }
}

void hashtable_connect_10w(int connfd){
    int i;
    for(i = 0; i < MAX_CONNECT_NUM ; ++i){
        char msg[512] = {0};
        snprintf(msg,512,"hset key%d value%d",i,i);
        test_case(connfd,msg,"OK\n","SetName");
    }
}

int main(int argc,char *argv[])
{
    assert(argc == 3);
    if(argc < 3) {
        printf("argc < 3\n");
        return -1;
    }

    const char* ip = argv[1];
    int port = atoi(argv[2]);
    int connfd = Connect_Server(ip,port);
    
#if 0
    // array
    printf("------array------\n");
    test_case(connfd,"set k2 v2","OK\n","set name");
    test_case(connfd,"count","1\n","count");
    test_case(connfd,"get k2","v2\n","get name");
    test_case(connfd,"delete k2","OK\n","delete name");
    test_case(connfd,"exist k2","NO EXIST\n","exist name");

    // rbtree
    printf("------rbtree------\n");
    test_case(connfd,"rset k2 v2","OK\n","set name");
    test_case(connfd,"rcount","1\n","count");
    test_case(connfd,"rget k2","v2\n","get name");
    test_case(connfd,"rdelete k2","OK\n","delete name");
    test_case(connfd,"rexist k2","NO EXIST\n","exist name");

    printf("------hashtable------\n");
    test_case(connfd,"hset k2 v2","OK\n","set name");
    test_case(connfd,"hcount","1\n","count");
    test_case(connfd,"hget k2","v2\n","get name");
    test_case(connfd,"hdelete k2","OK\n","delete name");
    test_case(connfd,"hexist k2","NO EXIST\n","exist name");
#endif
#if ENABLE_ARRAY_TEST
    struct timeval array_begin;
    gettimeofday(&array_begin,NULL);
    array_connect_10w(connfd);
    struct timeval array_end;
    gettimeofday(&array_end,NULL);
    int array_time_used = TIME_SUB_MS(array_end,array_begin);
    printf("array used time:%d\n",array_time_used);
#endif 
#if ENABLE_RBTREE_TEST
    struct timeval rb_begin;
    gettimeofday(&rb_begin,NULL);
    rbtree_connect_10w(connfd);
    struct timeval rb_end;
    gettimeofday(&rb_end,NULL);
    int rb_time_used = TIME_SUB_MS(rb_end,rb_begin);
    printf("rbtree used time:%d\n",rb_time_used);
#endif

#if ENABLE_HASHTABLE_TEST
    struct timeval hash_begin;
    gettimeofday(&hash_begin,NULL);
    hashtable_connect_10w(connfd);
    struct timeval hash_end;
    gettimeofday(&hash_end,NULL);
    int hash_time_used = TIME_SUB_MS(hash_end,hash_begin);
    printf("hash_time_used used time:%d\n",hash_time_used);
#endif
    close(connfd);
}