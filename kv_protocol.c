#include <assert.h>
#include "kv_store.h"
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

	// dhash
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