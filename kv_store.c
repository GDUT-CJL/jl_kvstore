// gcc kv_store.c kv_array.c -o kvstore -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -ldl
// gcc kv_store.c kv_array.c kv_rbtree.c -o kvstore -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -ldl
// gcc kv_store.c kv_array.c kv_rbtree.c kv_hash.c -o kvstore -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -ldl
// gcc kv_store.c kv_array.c kv_rbtree.c kv_hash.c kv_btree.c kv_skiplist.c -o kvstore -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -ldl
// gcc kv_store.c kv_array.c kv_rbtree.c kv_hash.c kv_btree.c kv_skiplist.c kv_dhash.c -o kvstore -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -ldl

// gcc kv_store.c kv_array.c kv_rbtree.c kv_hash.c kv_btree.c kv_skiplist.c kv_dhash.c jl_Mempool.c kv_flush.c -o kvstore -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -ldl
#include <arpa/inet.h>
#include "nty_coroutine.h"
#include "kv_store.h"

#define MAX_CLIENT_NUM			1000000
#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)
#define JL_MEMPOOL_SIZE		1 << 12
void* kvs_malloc(size_t size){
	return jl_alloc(p,size);
}

void kvs_free(void* ptr){
	return jl_free(p,ptr);
}

typedef enum kvs_cmd_e{
	KV_CMD_START = 0,
	// array
	KV_CMD_SET = KV_CMD_START,
	KV_CMD_GET,
	KV_CMD_COUNT,
	KV_CMD_DELETE,
	KV_CMD_EXIST,
	// rbtree
	KV_CMD_RSET,
	KV_CMD_RGET,
	KV_CMD_RCOUNT,
	KV_CMD_CMD_RDELETE,
	KV_CMD_REXIST,
	// btree
	KV_CMD_BSET,
	KV_CMD_BGET,
	KV_CMD_BCOUNT,
	KV_CMD_BDELETE,
	KV_CMD_BEXIST,
	// hash
	KV_CMD_HSET,
	KV_CMD_HGET,
	KV_CMD_HCOUNT,
	KV_CMD_HDELETE,
	KV_CMD_HEXIST,

	// skiptable
	KV_CMD_ZSET,
	KV_CMD_ZGET,
	KV_CMD_ZCOUNT,
	KV_CMD_ZDELETE,
	KV_CMD_ZEXIST,

	// skiptable
	KV_CMD_DSET,
	KV_CMD_DGET,
	KV_CMD_DCOUNT,
	KV_CMD_DDELETE,
	KV_CMD_DEXIST,

	KV_CMD_ERROR,
	KV_CMD_QUIT,
	KV_CMD_END	
}kvs_cmd_t;

const char* commands[] = {
	"SET","GET","COUNT","DELETE","EXIST",
	"RSET","RGET","RCOUNT","RDELETE","REXIST",
	"BSET","BGET","BCOUNT","BDELETE","BEXIST",
	"HSET","HGET","HCOUNT","HDELETE","HEXIST",
	"ZSET","ZGET","ZCOUNT","ZDELETE","ZEXIST",
	"DSET","DGET","DCOUNT","DDELETE","DEXIST",
};
#define MAX_TOKENS 		32

int kvs_parser_protocol(char *msg,char**buf,int count){
	if(buf == NULL || buf[0] == NULL || count == 0) return KV_CMD_ERROR;
	int cmd = KV_CMD_START;
	for(cmd = KV_CMD_START;cmd <= KV_CMD_DEXIST;cmd++){
		if(0 == strcasecmp(buf[0],commands[cmd])){
			//printf("%d %s\n",cmd,commands[cmd]);
			break;
		}
	}
	switch (cmd)
	{
	case KV_CMD_SET:
	{
		if(count == 3){
			int ret = kvs_array_set(buf[1],buf[2]);
			memset(msg,0,MAX_MSGBUFFER_LENGTH);
			if(ret == 0){
				snprintf(msg,MAX_MSGBUFFER_LENGTH,"OK\n");
			}
			else{
				snprintf(msg,MAX_MSGBUFFER_LENGTH,"FAILED\n");
			}
			break;
		}else if(count > 3){
			int time = atoi(buf[4]);
			int ret = kvs_set_array_expired(buf[1],buf[2],buf[3],time);
			memset(msg,0,MAX_MSGBUFFER_LENGTH);
			if(ret == 0){
				snprintf(msg,MAX_MSGBUFFER_LENGTH,"OK(ttl)\n");
			}
			else{
				snprintf(msg,MAX_MSGBUFFER_LENGTH,"FAILED(ttl)\n");
			}
			break;
		}
		
	}

	case KV_CMD_GET:
	{
		assert(count == 2);
		char* value = kvs_array_get(buf[1]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(value){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"%s\n",value);
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}

	case KV_CMD_COUNT:
	{
		assert(count == 1);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		int count = kvs_array_count();
		snprintf(msg,MAX_MSGBUFFER_LENGTH,"%d\n",count);
		break;
	}

	case KV_CMD_DELETE:
	{
		assert(count == 2);
		int ret = kvs_array_delete(buf[1]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(ret == 0){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"OK\n");
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}

	case KV_CMD_EXIST:
	{
		assert(count == 2);
		int ret = kvs_array_exist(buf[1]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(ret == 0){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"EXIST\n");
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}
	// rbtree
	case KV_CMD_RSET:
	{
		assert(count == 3);
		int ret = kvs_rbtree_set(buf[1],buf[2]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(ret == 0){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"OK\n");
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"FAILED\n");
		}
		break;
	}
	case KV_CMD_RGET:
	{
		assert(count == 2);
		char* value = kvs_rbtree_get(buf[1]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(value){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"%s\n",value);
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}
	case KV_CMD_RCOUNT:
	{
		assert(count == 1);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		int count = kvs_rbtree_count();
		snprintf(msg,MAX_MSGBUFFER_LENGTH,"%d\n",count);
		break;
	}
	case KV_CMD_CMD_RDELETE:
	{
		assert(count == 2);
		int ret = kvs_rbtree_delete(buf[1]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(ret == 0){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"OK\n");
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}
	case KV_CMD_REXIST:
	{
		assert(count == 2);
		int ret = kvs_rbtree_exist(buf[1]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(ret == 0){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"EXIST\n");
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}
	// btree
	case KV_CMD_BSET:
	{
		assert(count == 3);
		int ret = kvs_btree_set(&kv_b,buf);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(ret == 0){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"OK\n");
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"FAILED\n");
		}
		break;
	}
	case KV_CMD_BGET:
	{
		assert(count == 2);
		char* value = kvs_btree_get(&kv_b,buf);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(value){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"%s\n",value);
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}
	case KV_CMD_BCOUNT:
	{
		assert(count == 1);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		int count = kvs_btree_count(&kv_b);
		snprintf(msg,MAX_MSGBUFFER_LENGTH,"%d\n",count);
		break;
	}
	case KV_CMD_BDELETE:
	{
		assert(count == 2);
		int ret = kvs_btree_delete(&kv_b,buf);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(ret == 0){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"OK\n");
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}
	case KV_CMD_BEXIST:
	{
		assert(count == 2);
		int ret = kvs_btree_exist(&kv_b,buf);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(ret == 0){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"EXIST\n");
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}
	case KV_CMD_HSET:
	{
		assert(count == 3);
		int ret = kvs_hash_set(buf[1],buf[2]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(ret == 0){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"OK\n");
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"FAILED\n");
		}
		break;
	}
	case KV_CMD_HGET:
	{
		assert(count == 2);
		char* value = kvs_hash_get(buf[1]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(value){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"%s\n",value);
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}
	case KV_CMD_HCOUNT:
	{
		assert(count == 1);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		int count = kvs_hash_count();
		snprintf(msg,MAX_MSGBUFFER_LENGTH,"%d\n",count);
		break;
	}
	case KV_CMD_HDELETE:
	{
		assert(count == 2);
		int ret = kvs_hash_delete(buf[1]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(ret == 0){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"OK\n");
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}
	case KV_CMD_HEXIST:
	{
		assert(count == 2);
		int ret = kvs_hash_exist(buf[1]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(ret == 0){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"EXIST\n");
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}
	// skiptable
	case KV_CMD_ZSET:
	{
		assert(count == 3);
		int ret = kvs_skiplist_set(buf[1],buf[2]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(ret == 0){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"OK\n");
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"FAILED\n");
		}
		break;
	}
	case KV_CMD_ZGET:
	{
		assert(count == 2);
		char* value = kvs_skiplist_get(buf[1]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(value){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"%s\n",value);
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}
	case KV_CMD_ZCOUNT:
	{
		assert(count == 1);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		int count = kvs_skiplist_count();
		snprintf(msg,MAX_MSGBUFFER_LENGTH,"%d\n",count);
		break;
	}
	case KV_CMD_ZDELETE:
	{		
		assert(count == 2);
		int ret = kvs_skiplist_delete(buf[1]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(ret == 0){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"OK\n");
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}
	case KV_CMD_ZEXIST:
	{
		assert(count == 2);
		int ret = kvs_skiplist_exist(buf[1]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(ret == 0){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"EXIST\n");
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}
	case KV_CMD_DSET:
	{
		assert(count == 3);
		int ret = kvs_dhash_set(&dhash,buf[1],buf[2]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(ret == 0){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"OK\n");
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"FAILED\n");
		}
		break;
	}
	case KV_CMD_DGET:
	{
		assert(count == 2);
		char* value = kvs_dhash_get(&dhash,buf[1]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(value){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"%s\n",value);
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}
	case KV_CMD_DCOUNT:
	{
		assert(count == 1);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		int count = kvs_dhash_count(&dhash);
		snprintf(msg,MAX_MSGBUFFER_LENGTH,"%d\n",count);
		break;
	}
	case KV_CMD_DDELETE:
	{
		assert(count == 2);
		int ret = kvs_dhash_delete(&dhash,buf[1]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(ret == 0){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"OK\n");
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}
	case KV_CMD_DEXIST:
	{
		assert(count == 2);
		int ret = kvs_dhash_exist(&dhash,buf[1]);
		memset(msg,0,MAX_MSGBUFFER_LENGTH);
		if(ret == 0){
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"EXIST\n");
		}
		else{
			snprintf(msg,MAX_MSGBUFFER_LENGTH,"NO EXIST\n");
		}
		break;
	}
	default:
		break;
	}
}


int kvs_split_str(char** tokens,char* msg){
	int count = 0;
	char* token = strtok(msg," ");
	while(token != NULL){
		tokens[count++] = token;
		token = strtok(NULL, " ");
	}	
	return count;
}

int kvs_protocol(char* msg,int length){
	// 分割
	char* tokens[MAX_TOKENS] = {0};
	int count = kvs_split_str(tokens,msg);
	// for(int i = 0; i < count; i++){
	// 	printf("%s\n",tokens[i]);
	// }
	// 解析
	return kvs_parser_protocol(msg,tokens,count);
}

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
    		kv_flush_to_disk();
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

int InitEngine(){
	p = jl_create_pool(JL_MEMPOOL_SIZE);
	if(p == NULL)	return -1;

	initRbtree();
	init_hashtable();
	initSkipTable();
	initBtree(&kv_b,6);
	dhash_table_init(&dhash,DHASH_INIT_TABLE_SIZE);
}

void destoryEngine(){
	dest_hashtable();
	destRbtree();
	skiplist_desy();
	btree_destroy(&kv_b);
	dhash_table_desy(&dhash);

	jl_destory_pool(p);
}

int main(int argc, char *argv[]) {

	InitEngine();
	nty_coroutine *co = NULL;
	int i = 0;
	//unsigned short base_port = 9096;
	unsigned short *port = calloc(1, sizeof(unsigned short));
	*port = 8000;

	nty_coroutine_create(&co, server, port); ////////no run
	nty_schedule_run(); //run
	destoryEngine();


	return 0;
}



