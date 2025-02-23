#pragma once
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