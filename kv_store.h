#pragma once
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
// array
#define MAX_MSGBUFFER_LENGTH	1024
#define MAX_ARRAY_NUMS	100000
typedef struct kvs_array_item_s{
	char* key;
	char* value;
}kvs_array_item_t;
void* kvs_malloc(size_t size);

void kvs_free(void* ptr);

kvs_array_item_t* kvs_array_search_item(const char* key);
int kvs_array_exist(const char* key);
int kvs_array_set(char* key,char* value);
char* kvs_array_get(const char* key);
int kvs_array_delete(const char* key);
int kvs_array_count();


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
int kvs_rbtree_exist(const char* key);
int kvs_rbtree_set(char* key,char* value);
char* kvs_rbtree_get(const char* key);
int kvs_rbtree_delete(char* key);
int kvs_rbtree_count();
