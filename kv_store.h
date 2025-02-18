#pragma once
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
void* kvs_malloc(size_t size);
void kvs_free(void* ptr);

#define ENABLE_LOG      1
// LOG
#if ENABLE_LOG
#define LOG(_fmt, ...) fprintf(stdout, "[%s:%d] " _fmt, __FILE__, __LINE__, __VA_ARGS__)
#endif

// array
#define MAX_MSGBUFFER_LENGTH	1024
#define MAX_ARRAY_NUMS	102400
typedef struct kvs_array_item_s{
	char* key;
	char* value;
    long long expired;
}kvs_array_item_t;
struct kvs_array_item_s array_table[MAX_ARRAY_NUMS];

kvs_array_item_t* kvs_array_search_item(const char* key);
int kvs_array_exist(const char* key);
int kvs_array_set(char* key,char* value);
int kvs_set_array_expired(char* key,char* value,char* cmd,int expired);
char* kvs_array_get(const char* key);
int kvs_array_delete(const char* key);
int kvs_array_count();
/*--------------------------------------------------------------------*/

// rbtree
// 定义颜色枚举
typedef enum { RED, BLACK } Color;
// 红黑树节点结构
typedef struct RBNode {
    char* key;
    char* value;
    Color color;
    struct RBNode *left;
    struct RBNode *right;
    struct RBNode *parent;
} RBNode;
// 全局NIL节点，表示叶子节点
RBNode *NIL;
RBNode* root;
int initRbtree();
void destRbtree();
int kvs_rbtree_exist(const char* key);
int kvs_rbtree_set(char* key,char* value);
char* kvs_rbtree_get(const char* key);
int kvs_rbtree_delete(char* key);
int kvs_rbtree_count();
/*--------------------------------------------------------------------*/

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


#define KV_BTYPE_INT_INT    0    // int ket, void* value
#define KV_BTYPE_CHAR_CHAR  1    // char* key, char* value

#define ENABLE_BTREE_DEBUG 1  // 是否运行测试代码

// 定义键值对的类型
#if KV_BTYPE_INT_INT
    typedef int*  B_KEY_TYPE;   // key类型
    typedef int*  B_VALUE_TYPE; // value类型
    typedef int   B_KEY_SUB_TYPE;   // key的实际类型
    typedef int   B_VALUE_SUB_TYPE; // value的实际类型
#elif KV_BTYPE_CHAR_CHAR
    typedef char** B_KEY_TYPE;    // key类型
    typedef char** B_VALUE_TYPE;  // value类型
    typedef char*  B_KEY_SUB_TYPE;   // key的实际类型
    typedef char*  B_VALUE_SUB_TYPE; // value的实际类型
#endif
typedef struct _btree_node{
    B_KEY_TYPE keys;
    B_VALUE_TYPE values;
    struct _btree_node **children;
    int num;  // 当前节点的实际元素数量
    int leaf; // 当前节点是否为叶子节点
}btree_node;
// 实际上，B树的阶数由用户初始化时定义
// #define BTREE_M 6         // M阶
// #define SUB_M BTREE_M>>1  // M/2
typedef struct _btree{
    int m;      // m阶B树
    int count;  // B树所有的元素数量
    struct _btree_node *root_node;
}btree;
typedef btree kv_btree_t;

kv_btree_t kv_b;
int initBtree(kv_btree_t* kv_b, int m);
int kvs_btree_desy(kv_btree_t* kv_b);
int kvs_btree_set(kv_btree_t* kv_b, char** tokens);
char* kvs_btree_get(kv_btree_t* kv_b, char** tokens);
int kvs_btree_delete(kv_btree_t* kv_b, char** tokens);
int kvs_btree_count(kv_btree_t* kv_b);
int kvs_btree_exist(kv_btree_t* kv_b, char** tokens);
int btree_destroy(btree *T);