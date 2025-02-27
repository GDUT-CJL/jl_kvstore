#include "kv_store.h"
int flush_array() {
    FILE* file = fopen(PATH_TO_FLUSH_DISK, "a");
    if (!file) {
        perror("Failed to open file");
        return -1;
    }

    // 遍历 array_table，将键值对写入文件
    for (int i = 0; i < MAX_ARRAY_NUMS; ++i) {
        if (array_table[i].key != NULL && array_table[i].value != NULL) {
            // 以文本形式写入键值对
            fprintf(file, "set %s %s\n",
                    array_table[i].key, array_table[i].value);
        }
    }

    fclose(file);
    return 0;
}

void flush_hash() {  
    FILE* file = fopen(PATH_TO_FLUSH_DISK,"a");
    for(int i = 0; i < Hash->max_slots; ++i) { // 遍历到最大槽位  
        hashnode_t* node = Hash->nodes[i];  
        while(node != NULL) {  
            if(node->key != NULL) {  
                fprintf(file,"hset %s %s\n", node->key, node->value); // 打印key-value对  
            }  
            node = node->next;  
        }  
    }  
    fclose(file);
}

// 递归遍历节点并写入文件
void btree_flush_node(btree_node *cur, FILE* file) {
    if (cur == NULL) return;

    // 写入当前节点的键值对
    for (int i = 0; i < cur->num; i++) {
        fprintf(file, "bset %s %s\n", cur->keys[i], cur->values[i]);
    }

    // 递归遍历子节点
    if (cur->leaf == 0) {
        for (int i = 0; i < cur->num + 1; i++) {
            btree_flush_node(cur->children[i], file);
        }
    }
}
void flush_btree() {
    FILE* file = fopen(PATH_TO_FLUSH_DISK, "a");
    if (!file) {
        perror("Failed to open file");
        return;
    }

    // 从根节点开始遍历 B 树
    if (kv_b.root_node != NULL) {
        btree_flush_node(kv_b.root_node, file);
    } else {
        //printf("btree_flush_to_disk(): B-tree is empty!\n");
    }
    fclose(file);
}

void flush_rbtree(RBNode* root) {
    FILE* file = fopen(PATH_TO_FLUSH_DISK, "a");
    if (root != NIL) {
        fprintf(file,"rset %s %s\n", root->key,root->value);
        flush_rbtree(root->left);
        flush_rbtree(root->right);
    }
    fclose(file);
}

int flush_skiplist(){
    FILE* file = fopen(PATH_TO_FLUSH_DISK,"a");
    // 从跳表的最底层（第 0 层）开始遍历
    skipnode_t* cur_node = sklist->head->next[0];
    while (cur_node != NULL) {
        // 写入当前节点的键值对
        fprintf(file, "set %s %s\n", cur_node->key, cur_node->value);

        // 移动到下一个节点
        cur_node = cur_node->next[0];
    }

    fclose(file);
    return 0;
}

void kv_flush_to_disk(){
    FILE* file = fopen(PATH_TO_FLUSH_DISK, "w");
    if(file == NULL){
        perror("fopen");
        return;
    }
    if(file)
    flush_array();
    flush_hash();
    flush_btree();
    flush_rbtree(root);
    flush_skiplist();
}
