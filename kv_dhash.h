#pragma once
#define     DHASH_GROW_FACTOR   2
#define     DHASH_INIT_TABLE_SIZE   102400
typedef struct dhash_node_s{
    char *key;
    char* value;
}dhash_node_t;

typedef struct dhash_table_s{
    dhash_node_t** buckets;
    int count;         // 当前哈希表中的数据个数
    int capacity;     // 哈希表的总的存储容量
}dhash_table_t;

dhash_table_t dhash;
int dhash_table_init(dhash_table_t* dhash, int capacity);
int dhash_table_desy(dhash_table_t* dhash);
int kvs_dhash_set(dhash_table_t* dhash,char* key, char* value);
int kvs_dhash_exist(dhash_table_t* dhash,char* key);
char* kvs_dhash_get(dhash_table_t* dhash,char* key);
int kvs_dhash_delete(dhash_table_t* dhash,char* key);
int kvs_dhash_count(dhash_table_t* dhash);