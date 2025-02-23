#pragma once
// skiplist
#define     SKIPTABLE_MAX_LEVEL     5
typedef struct skipnode_s{
    char* key;
    char* value;
    struct skipnode_s** next;
}skipnode_t;

typedef struct skiplist_s{
    int nodeNum;
    int cur_level;
    int max_level;
    skipnode_t* head;
}skiplist_t;
skiplist_t* sklist;
int initSkipTable();
int skiplist_desy();
int kvs_skiplist_set(char* key, char* value);
char* kvs_skiplist_get(char* key);
int kvs_skiplist_delete(char* key);
int kvs_skiplist_count();
int kvs_skiplist_exist(char* key);