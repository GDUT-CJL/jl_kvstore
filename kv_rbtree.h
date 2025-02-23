#pragma once
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
