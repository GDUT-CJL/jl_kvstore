#include "kv_store.h"
#define MAX_KEY_LEN 20  
#define MAX_VALUE_LEN 20 

typedef enum kvs_type_s{
    KVS_TYPE_START = 0,
    KVS_TYPE_SET = KVS_TYPE_START,
    KVS_TYPE_BSET,
    KVS_TYPE_RSET,
    KVS_TYPE_HSET,
    KVS_TYPE_ZSET,
    KVS_TYPE_DSET,
    
    KVS_TYPE_END
}kvs_type_t;


const char* types[] = {
    "set","bset","rset","hset","zset","dset"
};

int kvs_reload_message(){
    FILE* file;
    char com[10];
    char key[MAX_KEY_LEN];
    char value[MAX_VALUE_LEN];

    file = fopen("redo.log", "r");  
    if (file == NULL) {  
        perror("无法打开文件");  
        return EXIT_FAILURE;  
    }

    while(fscanf(file,"%s %s %s",com,key,value) == 3){
        int cmd;
        for(cmd = KVS_TYPE_START; cmd < KVS_TYPE_END; cmd++){
            if(strcasecmp(com,types[cmd]) == 0){
                break;
            }
        }
        switch (cmd)
        {
        case KVS_TYPE_SET:
            kvs_array_set(key,value);
            break;
        case KVS_TYPE_BSET:
            btree_insert_key(&kv_b,key,value);
            break;
        case KVS_TYPE_RSET:
            kvs_rbtree_set(key,value);
            break;
        case KVS_TYPE_HSET:
            kvs_hash_set(key,value);    
            break;
        case KVS_TYPE_ZSET:
            kvs_skiplist_set(key,value);
            break;
        case KVS_TYPE_DSET:
            kvs_dhash_set(&dhash,key,value);
            break;
        default:
            break;
        }
    }

}
