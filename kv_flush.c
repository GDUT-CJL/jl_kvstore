#include "kv_store.h"
#include <unistd.h>
#define     KV_FLUSH_TXT    1       //文本格式刷盘
#define     KV_FLUSH_BIN    0       //二进制文件格式刷盘
#if KV_FLUSH_TXT
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
    FILE* file = fopen(PATH_TO_FLUSH_DISK_TXT, "w");
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
    fflush(file);
    fclose(file);
}

void kv_flush_thread(void* arg){
    while(1){
        sleep(1);
        kv_flush_to_disk();
    }
}

#elif KV_FLUSH_BIN
#include "kv_store.h"  
#include <unistd.h>  
#include <string.h> // 包含字符串操作的函数  

// 将数据写入文件，使用 fwrite  
static int flush_to_disk(FILE* file, const char* cmd, const char* key, const char* value) {  
    // 计算键和值的长度
    size_t cmd_len = strlen(cmd);  
    size_t key_len = strlen(key);  
    size_t value_len = strlen(value);  

    // 写入键的长度和键  写入长度方便读取恢复数据
    fwrite(&cmd_len,sizeof(size_t),1,file);
    fwrite(cmd,sizeof(char),cmd_len,file);

    fwrite(&key_len, sizeof(size_t), 1, file); 
    fwrite(key, sizeof(char), key_len, file);  

    // 写入值的长度和值  
    fwrite(&value_len, sizeof(size_t), 1, file);  
    fwrite(value, sizeof(char), value_len, file);  

    return 0;  
}  

int flush_array(FILE* file) {  
    for (int i = 0; i < MAX_ARRAY_NUMS; ++i) {  
        if (array_table[i].key != NULL && array_table[i].value != NULL) {  
            flush_to_disk(file, "set",array_table[i].key, array_table[i].value);  
        }  
    }  
    return 0;  
}  

void flush_hash(FILE* file) {  
    for (int i = 0; i < Hash->max_slots; ++i) { // 遍历到最大槽位  
        hashnode_t* node = Hash->nodes[i];  
        while (node != NULL) {  
            flush_to_disk(file, "hset",node->key, node->value);  
            node = node->next;  
        }  
    }  
}  

void btree_flush_node(btree_node *cur, FILE* file) {  
    if (cur == NULL) return;  

    // 写入当前节点的键值对  
    for (int i = 0; i < cur->num; i++) {  
        flush_to_disk(file,"bset", cur->keys[i], cur->values[i]);  
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
        flush_to_disk(file, "rset", root->key, root->value);  
        flush_rbtree(root->left, file);  
        flush_rbtree(root->right, file);  
    }  
}  

void flush_skiplist(FILE* file) {  
    skipnode_t* cur_node = sklist->head->next[0];  
    while (cur_node != NULL) {  
        flush_to_disk(file, "zset", cur_node->key, cur_node->value);  
        cur_node = cur_node->next[0];  
    }  
}  

void flush_dhash(FILE* file) {  
    for (int i = 0; i < dhash.capacity; ++i) {  
        if (dhash.buckets[i] != NULL) {  
            flush_to_disk(file, "dset", dhash.buckets[i]->key, dhash.buckets[i]->value);  
        }  
    }  
}  

void kv_flush_to_disk() {  
    FILE* file = fopen(PATH_TO_FLUSH_DISK_BIN, "wb"); // 以二进制写入模式打开文件  
    if (file == NULL) {  
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

void kv_flush_thread(void* arg) {  
    while (1) {  
        sleep(1);  
        kv_flush_to_disk();  
    }  
}
#endif