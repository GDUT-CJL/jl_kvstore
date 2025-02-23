#pragma once
// hash
#define MAX_HASHSIZE    102400
typedef struct hashnode_s{
    char* key;
    char* value;
    struct hashnode_s* next;
}hashnode_t;
typedef struct hashtable_s{
    hashnode_t** nodes;
    int max_slots;  // hash表的最大槽位
    int count;  // hash表当前的数目
}hashtable_t;
hashtable_t* Hash;
int init_hashtable();
void dest_hashtable();
int kvs_hash_exist(char* key);
int kvs_hash_set(char* key,char* value);
char* kvs_hash_get(char* key);
int kvs_hash_delete(char* key);
int kvs_hash_count();

/*--------------------------------------------------------------------*/