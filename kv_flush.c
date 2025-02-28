#include "kv_store.h"
#include <unistd.h>

// 统一的刷盘函数，减少文件打开和关闭的次数
static int flush_to_disk(FILE* file, const char* format, const char* key, const char* value) {
    if (key != NULL && value != NULL) {
        fprintf(file, format, key, value);
    }
    return 0;
}

int flush_array(FILE* file) {
    for (int i = 0; i < MAX_ARRAY_NUMS; ++i) {
        flush_to_disk(file, "set %s %s\n", array_table[i].key, array_table[i].value);
    }
    return 0;
}

void flush_hash(FILE* file) {  
    for(int i = 0; i < Hash->max_slots; ++i) { // 遍历到最大槽位  
        hashnode_t* node = Hash->nodes[i];  
        while(node != NULL) {  
            flush_to_disk(file, "hset %s %s\n", node->key, node->value);
            node = node->next;  
        }  
    }  
}

void btree_flush_node(btree_node *cur, FILE* file) {
    if (cur == NULL) return;

    // 写入当前节点的键值对
    for (int i = 0; i < cur->num; i++) {
        flush_to_disk(file, "bset %s %s\n", cur->keys[i], cur->values[i]);
    }

    // 递归遍历子节点
    if (cur->leaf == 0) {
        for (int i = 0; i < cur->num + 1; i++) {
            btree_flush_node(cur->children[i], file);
        }
    }
}

void flush_btree(FILE* file) {
    if (kv_b.root_node != NULL) {
        btree_flush_node(kv_b.root_node, file);
    }
}

void flush_rbtree(RBNode* root, FILE* file) {
    if (root != NIL) {
        flush_to_disk(file, "rset %s %s\n", root->key, root->value);
        flush_rbtree(root->left, file);
        flush_rbtree(root->right, file);
    }
}

void flush_skiplist(FILE* file){
    skipnode_t* cur_node = sklist->head->next[0];
    while (cur_node != NULL) {
        flush_to_disk(file, "zset %s %s\n", cur_node->key, cur_node->value);
        cur_node = cur_node->next[0];
    }
}

void flush_dhash(FILE* file){
    for(int i = 0; i < dhash.capacity; ++i){
        if(dhash.buckets[i] != NULL){
            flush_to_disk(file,"dset %s %s\n",dhash.buckets[i]->key,dhash.buckets[i]->value);
        }
    }
}

void kv_flush_to_disk(){
    FILE* file = fopen(PATH_TO_FLUSH_DISK, "w");
    if(file == NULL){
        perror("fopen");
        return;
    }

    flush_array(file);
    flush_skiplist(file);
    flush_hash(file);
    flush_btree(file);
    flush_rbtree(root, file);
    flush_dhash(file);
    fclose(file);
}

void kv_flush_thread(void* arg){
    while(1){
        sleep(1);
        kv_flush_to_disk();
    }
}