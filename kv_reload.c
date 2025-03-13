#include "kv_store.h"
#define MAX_KEY_LEN 20480  
#define MAX_VALUE_LEN 20480 

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

#if ENABLE_RELOAD_TXT
int kvs_reload_message(){
    FILE* file;
    char com[20480];
    char key[MAX_KEY_LEN];
    char value[MAX_VALUE_LEN];

    file = fopen(PATH_TO_FLUSH_DISK_TXT, "r");  
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
#elif ENABLE_RELOAD_BIN
int read_from_disk(FILE* file, char** cmd, char** key, char** value) {  
    size_t cmd_len, key_len, value_len;  

    // 读取命令长度  
    if (fread(&cmd_len, sizeof(size_t), 1, file) != 1) {  
        return -1; // 表示读取失败或文件结束  
    }  
    *cmd = (char*)malloc(cmd_len + 1);  
    if (*cmd == NULL) return -1;  
    if (fread(*cmd, sizeof(char), cmd_len, file) != cmd_len) {  
        free(*cmd);  
        return -1; // 读取命令失败  
    }  
    (*cmd)[cmd_len] = '\0';  

    // 读取键长度  
    if (fread(&key_len, sizeof(size_t), 1, file) != 1) {  
        free(*cmd);  
        return -1;  
    }  
    *key = (char*)malloc(key_len + 1);  
    if (*key == NULL) return -1;  
    if (fread(*key, sizeof(char), key_len, file) != key_len) {  
        free(*cmd);  
        free(*key);  
        return -1;  
    }  
    (*key)[key_len] = '\0';  

    // 读取值长度  
    if (fread(&value_len, sizeof(size_t), 1, file) != 1) {  
        free(*cmd);  
        free(*key);  
        return -1;  
    }  
    *value = (char*)malloc(value_len + 1);  
    if (*value == NULL) return -1;  
    if (fread(*value, sizeof(char), value_len, file) != value_len) {  
        free(*cmd);  
        free(*key);  
        free(*value);  
        return -1;  
    }  
    (*value)[value_len] = '\0';  

    return 0; // 表示读取成功  
}  

int kvs_reload_bin(){
    FILE* file;
    char* com = NULL;
    char* key = NULL;
    char* value = NULL;

    file = fopen(PATH_TO_FLUSH_DISK_BIN, "rb");  
    if (file == NULL) {  
        perror("无法打开文件");  
        return EXIT_FAILURE;  
    }

    while(1){
        if(read_from_disk(file,&com,&key,&value) != 0){
            break;//读取失败，退出循环
        }
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
            //printf("%s %s\n",key,value);
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
        // 清理已读取的内存  
        free(com);  // 释放命令字符串  
        free(key);  // 释放键  
        free(value); // 释放值 
    }
    fclose(file); // 在结束时关闭文件  
    return EXIT_SUCCESS; // 返回成功  
}

#endif
