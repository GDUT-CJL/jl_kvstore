// dhash 保证一个槽位只有一个元素，不成链表。如有哈希冲突就进行扩容
#include "kv_store.h"

// djb2哈希算法
static unsigned long _hash(const char *key, int capacity) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++))
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    return hash % capacity;
}

static dhash_node_t* _create_node(char* key, char* value){
    dhash_node_t* node = (dhash_node_t*)malloc(sizeof(dhash_node_t));
    if(node == NULL)    return NULL;
    char* kcopy = (char*)malloc(sizeof(char));
    if(kcopy == NULL){
        free(node);
        node = NULL;
        return NULL;
    }
    char* vcopy = (char*)malloc(sizeof(char));
    if(vcopy == NULL){
        free(kcopy);
        free(node);
        kcopy = NULL;
        node = NULL;
        return NULL;
    }

    strncpy(kcopy,key,strlen(key)+1);
    strncpy(vcopy,value,strlen(value)+1);

    node->key = kcopy;
    node->value = vcopy;

    return node;
}

static int dhash_node_desy(dhash_node_t* node){
    if(node == NULL) return -1;
    if(node->key){
        free(node->key);
        node->key = NULL;
    }
    if(node->value){
        free(node->value);
        node->value = NULL;
    }
    free(node);
    node = NULL;
    return 0;
}

int dhash_table_init(dhash_table_t* dhash, int capacity){
    if(dhash == NULL)   return -1;
    dhash->buckets = (dhash_node_t**)calloc(capacity,sizeof(dhash_node_t));
    if(dhash->buckets == NULL)  return -1;
    dhash->capacity = capacity;
    dhash->size = 0;
    return 0;
}

int dhash_table_desy(dhash_table_t* dhash){
    if(dhash == NULL)   return -1;
    for(int i = 0; i < dhash->capacity; ++i){
        while(dhash->buckets[i] != NULL){
            int ret = dhash_node_desy(dhash->buckets[i]);
            if(ret == -1)   return -1;
            dhash->size--;
        }
    }
    if(dhash->buckets) {
        free(dhash->buckets);
        dhash->buckets = NULL;
    }
    dhash->capacity = 0;
    dhash->size = 0;
    free(dhash);
}

int kvs_dhash_set(dhash_table_t* dhash, char* key, char* value) {  
    if (dhash == NULL || key == NULL || value == NULL) return -1;  

    // 计算负载因子并扩容  
    if (dhash->size >= (dhash->capacity * 0.75)) {  
        dhash_table_t new_table;  
        if (dhash_table_init(&new_table, dhash->capacity * DHASH_GROW_FACTOR) != 0) return -1;  

        // 迁移现有节点到新表  
        for (int i = 0; i < dhash->capacity; ++i) {  
            if (dhash->buckets[i] != NULL) {  
                int idx = _hash(dhash->buckets[i]->key, new_table.capacity);  
                while (new_table.buckets[idx] != NULL) {  
                    // 碰撞处理  
                    idx = (idx + 1) % new_table.capacity;  
                }  
                new_table.buckets[idx] = dhash->buckets[i]; // 直接引用，不重新创建  
            }  
        }  

        // 释放旧的桶并更新当前哈希表的结构  
        free(dhash->buckets);  
        dhash->buckets = new_table.buckets;  
        dhash->capacity = new_table.capacity;  
        dhash->size = new_table.size;  
        new_table.buckets = NULL; // 避免释放时重复释放  
    }  

    // 计算插入索引  
    int idx = _hash(key, dhash->capacity);  
    while (dhash->buckets[idx] != NULL) {  
        if (strcmp(dhash->buckets[idx]->key, key) == 0) {  
            return -1; // 已经存在  
        }  
        idx = (idx + 1) % dhash->capacity; // 线性探测  
    }  

    // 创建新节点并插入  
    dhash->buckets[idx] = _create_node(key, value);  
    if (dhash->buckets[idx] == NULL) {  
        return -1; // 节点创建失败  
    }  

    dhash->size++; // 增加大小计数  
    return 0; // 成功  
}

dhash_node_t* kvs_dhash_search(dhash_table_t* dhash, char* key) {
    if (key == NULL || dhash == NULL) {
        return NULL;
    }

    int idx = _hash(key, dhash->capacity);
    for (int i = 0; i < dhash->capacity; i++) { // 线性探测法
        if (dhash->buckets[idx] != NULL && strcmp(dhash->buckets[idx]->key, key) == 0) {
            return dhash->buckets[idx]; // 找到目标键，直接返回
        }
        idx = (idx + 1) % dhash->capacity; // 更新索引
    }

    return NULL; // 未找到目标键
}

int kvs_dhash_exist(dhash_table_t* dhash,char* key){
    dhash_node_t* node = kvs_dhash_search(dhash,key);
    if(node == NULL)    return -1;
    return 0;
}

char* kvs_dhash_get(dhash_table_t* dhash,char* key){
    if(key == NULL) return NULL;
    dhash_node_t* node = kvs_dhash_search(dhash,key);
    if(node == NULL)    return NULL;
    return node->value;
}

int kvs_dhash_delete(dhash_table_t* dhash,char* key){
    // 首先看看是否需要缩减哈希表
    // 存储元素小于1/4空间，按照“增长因子DHASH_GROW_FACTOR”缩减
    int idx = _hash(key,dhash->capacity);
    if(dhash->capacity > DHASH_INIT_TABLE_SIZE && dhash->size < (dhash->capacity>>4)){
        dhash_table_t new_table;
        int ret = dhash_table_init(&new_table, dhash->capacity/DHASH_GROW_FACTOR);
        if(ret != 0) return -1;
        for(int i = 0; i < dhash->capacity; ++i){
            if(dhash->buckets[i] != NULL){  // 将数据进行搬运,如果不为空说明存在数据再进行搬运操作
                int ret = kvs_dhash_set(&new_table,dhash->buckets[i]->key,dhash->buckets[i]->value);
                if(ret != 0)    return -1;
            }
        }
        dhash->capacity = dhash->capacity / DHASH_GROW_FACTOR;
        dhash->size = new_table.size;
        dhash_node_t** tmp = dhash->buckets;
        dhash->buckets = new_table.buckets;
        new_table.buckets = tmp;
        ret = dhash_table_desy(&new_table);
        if(ret != 0)    return -1; 
    }

    dhash_node_t* node = kvs_dhash_search(dhash,key);
    if(node){
        int ret = dhash_node_desy(node);
        if(ret != 0)    return -1;
        dhash->size--;
        return 0;
    }
    return -1;
} 

int kvs_dhash_count(dhash_table_t* dhash){
    return dhash->size;
}

#if 0
int dhash_printf(dhash_table_t* table){
    if(table == NULL)   return -1;
    for(int i = 0; i < table->capacity; ++i){
        if(table->buckets[i] != NULL){
            printf("key:%s\nvalue:%s\n",table->buckets[i]->key,table->buckets[i]->value);
        }
    }
    return -1;
}


int main(){
    dhash_table_t* dhash = (dhash_table_t*)malloc(sizeof(dhash_node_t));
    int ret = dhash_table_init(dhash,DHASH_INIT_TABLE_SIZE);
    if(ret != 0)    return -1;

    ret = kvs_dhash_set(dhash,"k1","v1");
    ret = kvs_dhash_set(dhash,"k2","v2");
    if(ret != 0)    return -1;
    dhash_printf(dhash);

    ret = kvs_dhash_exist(dhash,"k1");
    if(ret == 0){
        printf("exist\n");
    }else{
        printf("no exist\n");
    }
    kvs_dhash_delete(dhash,"k1");
    ret = kvs_dhash_exist(dhash,"k1");
    if(ret == 0){
        printf("exist\n");
    }else{
        printf("no exist\n");
    }

    char* value = kvs_dhash_get(dhash,"k2");
    printf("value:%s\n",value);
    return 0;
}
#endif