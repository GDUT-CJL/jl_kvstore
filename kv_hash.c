#include "kv_store.h"
#define MAX_HASHSIZE    1024
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
int _hash(char* key,int size){
    if(!key)    return -1;
    int sum = 0;
    int i = 0;
    while(key[i] != 0){
        sum += key[i];
        i++;
    }

    return sum % size;
}

hashnode_t* createNode(char* key,char* value){
    hashnode_t* node = (hashnode_t*)malloc(sizeof(hashnode_t));
    if(!node) return NULL;

    node->key = (char*)malloc(sizeof(char));
    if(!node->key){
        free(node);
        return NULL;
    } 

    node->value = (char*)malloc(sizeof(char));
    if(!node->value){
        free(node->key);
        free(node);
        return NULL;
    }
    strcmp(node->key,key); 
    strcmp(node->value,value);

    node->next = NULL;// 作为链表节点，这一步也很重要不要遗忘，方便以后添加
    return node; 
}

int init_hashtable(){
    Hash = (hashtable_t*)malloc(sizeof(hashtable_t));
    if(!Hash)   return -1;
    Hash->nodes = (hashnode_t**)malloc(sizeof(hashnode_t*) * MAX_HASHSIZE);
    if(!Hash->nodes)    return -1;
    
    Hash->max_slots = MAX_HASHSIZE;
    Hash->count = 0;
    return 0;
}

void dest_hashtable(){
    if(!Hash)   return;
    for(int i = 0 ; i < Hash->count; ++i){
        hashnode_t* node = Hash->nodes[i];
        while (node != NULL)
        {
            hashnode_t* temp = node;
            node = node->next;
            Hash->nodes[i] = node;
            free(temp);
        }
    }

    free(Hash);
}

int kvs_hash_exist(const char* key){

}

int kvs_hash_set(char* key,char* value){
    if(key == NULL || value == NULL) return -1;
    int idx = _hash(key,MAX_HASHSIZE);
    hashnode_t* node = Hash->nodes[idx];
    // 遍历整个链表
    while(node != NULL){
        if(strcmp(node->key,key) == 0){//exist
            return 1;
        }
        node = node->next;
    }
    // 头插法
    hashnode_t *new_node = createNode(key,value);
    new_node->next = Hash->nodes[idx];
    Hash->nodes[idx] = new_node;

    Hash->count++;
} 

char* kvs_hash_get(const char* key){
    
} 

int kvs_hash_delete(const char* key){

}

int kvs_hash_count(){
    return Hash->count;
}

void print_hash(){
    for(int i = 0; i < Hash->count; ++i){
        hashnode_t* node = Hash->nodes[i];
        while(node != NULL){
            if(node->key != NULL){
                printf("%s\n",node->value);
            }
            node = node->next;
        }
    }
}

int main(){
    init_hashtable();
    kvs_hash_set("k1","v1");
    print_hash();
    return 0;
}