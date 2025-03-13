#include "kv_store.h"
#include "nty_coroutine.h"
#include "kv_protocol.h"
// ---------------------------网络层------------------------------- //
void server_reader(void *arg) {
	int fd = *(int *)arg;
	free(arg);
	int ret = 0;

	while (1) {
		
		char buf[MAX_MSGBUFFER_LENGTH] = {0};
		ret = recv(fd, buf, MAX_MSGBUFFER_LENGTH, 0);
		if (ret > 0) {
			kvs_protocol(buf,ret);
    		//kv_flush_to_disk();
			// pthread_t flush_tid;  
            // if (pthread_create(&flush_tid, NULL, kv_flush_thread, NULL) != 0) {  
            //     perror("Failed to create flush thread");  
            //     close(fd);  
            //     break;  
            // }  
            // // 可以选择在这里分离线程，以便避免管理  
            // pthread_detach(flush_tid);   

			
			ret = send(fd, buf, strlen(buf), 0);
			if (ret == -1) {
				close(fd);
				break;
			}
		} else if (ret == 0) {	
			close(fd);
			break;
		}

	}
}


void server(void *arg) {

	unsigned short port = *(unsigned short *)arg;
	free(arg);

	int fd = socket(AF_INET, SOCK_STREAM, 0);
	if (fd < 0) return ;

	struct sockaddr_in local, remote;
	local.sin_family = AF_INET;
	local.sin_port = htons(port);
	local.sin_addr.s_addr = INADDR_ANY;
	bind(fd, (struct sockaddr*)&local, sizeof(struct sockaddr_in));

	listen(fd, 20);
	LOG("listen port : %d\n", port);
	
	struct timeval tv_begin;
	gettimeofday(&tv_begin, NULL);

	while (1) {
		socklen_t len = sizeof(struct sockaddr_in);
		int cli_fd = accept(fd, (struct sockaddr*)&remote, &len);
		if (cli_fd % 1000 == 999) {

			struct timeval tv_cur;
			memcpy(&tv_cur, &tv_begin, sizeof(struct timeval));
			
			gettimeofday(&tv_begin, NULL);
			int time_used = TIME_SUB_MS(tv_begin, tv_cur);
			
			printf("client fd : %d, time_used: %d\n", cli_fd, time_used);
		}
	//	printf("new client comming\n");

		nty_coroutine *read_co;
		int *arg = malloc(sizeof(int));
		*arg = cli_fd;
		nty_coroutine_create(&read_co, server_reader, arg);

	}
	
}

int start_coroutine(){
	nty_coroutine *co = NULL;
	int i = 0;
	//unsigned short base_port = 9096;
	unsigned short *port = calloc(1, sizeof(unsigned short));
	*port = 8000;
	nty_coroutine_create(&co, server, port); ////////no run

	nty_schedule_run(); //run

}
