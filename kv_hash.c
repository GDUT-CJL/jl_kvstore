#include "kv_store.h"
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

hashnode_t* _createNode(char* key,char* value){
    hashnode_t* node = (hashnode_t*)kvs_malloc(sizeof(hashnode_t));
    if(!node) return NULL;

    node->key = (char*)kvs_malloc(sizeof(char));
    if(!node->key){
        kvs_free(node);
        return NULL;
    } 

    node->value = (char*)kvs_malloc(sizeof(char));
    if(!node->value){
        kvs_free(node->key);
        kvs_free(node);
        return NULL;
    }
    strcpy(node->key,key); 
    strcpy(node->value,value);

    node->next = NULL;// 作为链表节点，这一步也很重要不要遗忘，方便以后添加
    return node; 
}

int init_hashtable(){
    Hash = (hashtable_t*)kvs_malloc(sizeof(hashtable_t));
    if(!Hash)   return -1;
    Hash->nodes = (hashnode_t**)kvs_malloc(sizeof(hashnode_t*) * MAX_HASHSIZE);
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
            kvs_free(temp);
        }
    }
    kvs_free(Hash);
}

int kvs_hash_exist(char* key){
    if(key == NULL) return -1;
    int idx = _hash(key,MAX_HASHSIZE);
    hashnode_t* node = Hash->nodes[idx];
    while (node != NULL)
    {
        if(strcmp(node->key,key) == 0){
            return 0;
        }
        node = node->next;
    }
    return -1;   
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
    hashnode_t *new_node = _createNode(key,value);
    new_node->next = Hash->nodes[idx];
    Hash->nodes[idx] = new_node;

    Hash->count++;
    return 0;
} 

char* kvs_hash_get(char* key){
    if(key == NULL) return NULL;
    int idx = _hash(key,MAX_HASHSIZE);
    hashnode_t* node = Hash->nodes[idx];
    while (node != NULL)
    {
        if(strcmp(node->key,key) == 0){
            return node->value;
        }
        node = node->next;
    }
    
    return NULL;

} 

// hash删除需要区分两种情况，头节点和非同节点
int kvs_hash_delete(char* key){
    if (key == NULL) return -1;
    int idx = _hash(key,MAX_HASHSIZE);
    hashnode_t* head = Hash->nodes[idx];
    if(strcmp(head->key,key) == 0){
        hashnode_t* new_head = head->next;
        Hash->nodes[idx] = new_head;

        if(head->key != NULL)   kvs_free(head->key);
        if(head->value != NULL)  kvs_free(head->value);

        kvs_free(head);

        Hash->count--;
        return 0;
    }

    hashnode_t* cur = head;
    while(cur->next != NULL){
        if(strcmp(cur->next->key,key))    break;
        cur = cur->next;
    }

    if(cur->next == NULL){
        return -1;
    }

    hashnode_t* tmp = cur->next;
    cur->next = cur->next->next;

    if(tmp->key != NULL)    kvs_free(tmp->key);
    if(tmp->value != NULL)  kvs_free(tmp->value);

    Hash->count--;
    return 0;

    
}

int kvs_hash_count(){
    return Hash->count;
}

#if 0
void print_hash() {  
    for(int i = 0; i < Hash->max_slots; ++i) { // 遍历到最大槽位  
        hashnode_t* node = Hash->nodes[i];  
        while(node != NULL) {  
            if(node->key != NULL) {  
                printf("%s: %s\n", node->key, node->value); // 打印key-value对  
            }  
            node = node->next;  
        }  
    }  
}

int main(){
    init_hashtable();
    kvs_hash_set("k1","v1");
    kvs_hash_set("k2","v2");
    kvs_hash_set("k3","v3");
    print_hash();
    kvs_hash_get("k1");
    kvs_hash_get("k3");
    int count = kvs_hash_count();
    printf("count1:%d\n",count);

    kvs_hash_delete("k1");
    if(kvs_hash_exist("k1") == 0){
        printf("exist\n");
    }else{
        printf("no exist\n");
    }

    if(kvs_hash_exist("k2") == 0){
        printf("exist\n");
    }else{
        printf("no exist\n");
    }
    count = kvs_hash_count();
    printf("count2:%d\n",count);

    kvs_hash_delete("k2");

    if(kvs_hash_exist("k2") == 0){
        printf("exist\n");
    }else{
        printf("no exist\n");
    }
    count = kvs_hash_count();
    printf("count3:%d\n",count);
    return 0;
}
#endif