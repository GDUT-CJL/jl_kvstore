// gcc kv_store.c kv_array.c -o kvstore -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -ldl
// gcc kv_store.c kv_array.c kv_rbtree.c -o kvstore -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -ldl
// gcc kv_store.c kv_array.c kv_rbtree.c kv_hash.c -o kvstore -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -ldl
// gcc kv_store.c kv_array.c kv_rbtree.c kv_hash.c kv_btree.c kv_skiplist.c -o kvstore -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -ldl
// gcc kv_store.c kv_array.c kv_rbtree.c kv_hash.c kv_btree.c kv_skiplist.c kv_dhash.c -o kvstore -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -ldl
// gcc kv_store.c kv_array.c kv_rbtree.c kv_hash.c kv_btree.c kv_skiplist.c kv_dhash.c jl_Mempool.c kv_flush.c -o kvstore -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -ldl
// gcc kv_store.c kv_array.c kv_rbtree.c kv_hash.c kv_btree.c kv_skiplist.c kv_dhash.c jl_Mempool.c kv_flush.c jl_Thrdpool.c -o kvstore -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -ldl

// gcc kv_store.c kv_array.c kv_rbtree.c kv_hash.c kv_btree.c kv_skiplist.c kv_dhash.c jl_Mempool.c kv_flush.c jl_Thrdpool.c kv_net.c kv_protocol.c -o kvstore -I ./NtyCo/core/ -L ./NtyCo/ -lntyco -lpthread -ldl

#include "kv_store.h"
#define JL_MEMPOOL_SIZE		1 << 12
void* kvs_malloc(size_t size){
	return jl_alloc(p,size);
}
void kvs_free(void* ptr){
	return jl_free(p,ptr);
}

int initPool(){
#if ENABLE_THRDPOOL
	thrdpool = create_thrdpool(1);
	if(thrdpool == NULL){
		return -1;
	}
	post_threadTask(thrdpool,kv_flush_thread,NULL);
#endif
	p = jl_create_mempool(JL_MEMPOOL_SIZE);
	if(p == NULL){
		free(thrdpool);
		return -1;
	}
}

int destoryPool(){
	jl_destory_mempool(p);
	destroy_thrdpool(thrdpool);
}

int InitEngine(){
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
}

int main(int argc, char *argv[]) {
	initPool();
	InitEngine();
	start_coroutine();
	destoryEngine();
	destoryPool();
	return 0;
}



