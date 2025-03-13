#include "kv_store.h"
#include <sys/time.h>
int array_count = 0;
static void _clean_expired_task(){
#if 1
	struct timeval tv;
	gettimeofday(&tv,NULL);
	long long cur_time = (tv.tv_sec * 1000LL) + (tv.tv_usec / 1000);
	for(int i = 0; i < MAX_ARRAY_NUMS; ++i){
		kvs_array_item_t* enter = &array_table[i];
		if(enter->key != NULL && enter->value != NULL){
			if(enter->expired != 0 && cur_time > enter->expired){
				kvs_free(enter->key);
				kvs_free(enter->value);
				enter->key = NULL;
				enter->expired = 0;
				enter->value = NULL;
				array_count--;
			}
		}
	}
#endif
}

kvs_array_item_t* kvs_array_search_item(const char* key){
	if(!key) return NULL;
	_clean_expired_task();
	for(int idx = 0; idx < MAX_ARRAY_NUMS;idx++){
		if (array_table[idx].key == NULL) {
			continue;
		}
		if((strcmp(array_table[idx].key,key) == 0)){
			return &array_table[idx];
		}			
	}
	return NULL;
}

// array exist
int kvs_array_exist(const char* key){
	kvs_array_item_t* get = kvs_array_search_item(key);
	if(get){
		return 0;
	}
	return -1;
}

int kvs_array_insert_ttl(char* key,char* value,long long expired_time){
	if(key == NULL || value == NULL || array_count == MAX_ARRAY_NUMS - 1) return -1;
	_clean_expired_task();
	char* kcopy = (char*)kvs_malloc(strlen(key)+1);
	if(!kcopy) return -1;
	char* vcopy = (char*)kvs_malloc(strlen(value)+1);
	if(!vcopy){
		free(kcopy);
		return -1;
	}
	strncpy(kcopy,key,strlen(key)+1);
	strncpy(vcopy,value,strlen(value)+1);

	//int* time_copy = (int*)malloc(sizeof(int));
	//*time_copy = expired_time;
#if 0
	// 有问题，不能和delete配合，会发现delete完后count--数据会被覆盖
	array_table[array_count].key = kcopy;
	array_table[array_count].value = vcopy;
	array_count++;
#endif
	int i = 0;
	for(i = 0; i < MAX_ARRAY_NUMS;++i){
		if(array_table[i].key == NULL && array_table[i].value == NULL)
			break;
	}
	array_table[i].key = kcopy;
	array_table[i].value = vcopy;
	array_table[i].expired = expired_time;
	array_count++;
	return 0;
}

// array set 
int kvs_array_set(char* key,char* value){
	return kvs_array_insert_ttl(key,value,0);
}

int kvs_set_array_expired(char* key,char* value,char* cmd,int expired){
	if(key == NULL && value == NULL && cmd== NULL && expired <= 0)	return -1;
	long long time;
	if(strcasecmp(cmd,"px") == 0){
		time = expired;
	}else if(strcasecmp(cmd,"ex") == 0){
		time = (long long)expired * 1000;
	}else{
		return -1;
	}

	struct timeval cur_time;
	gettimeofday(&cur_time,NULL);
	long long alltime = (cur_time.tv_sec * 1000LL) + (cur_time.tv_usec / 1000) + time;
	return kvs_array_insert_ttl(key,value,alltime);
}

// array get 
char* kvs_array_get(const char* key){
	kvs_array_item_t* get = kvs_array_search_item(key);
	if(get){
		return get->value;
	}
	return NULL;
}
// array delete
int kvs_array_delete(const char* key){  
    if (!key) return -1; // 检查 key 是否为空  
	
    _clean_expired_task();  
    for (int i = 0; i < MAX_ARRAY_NUMS; i++) {  
        // 检查 array_table[i].key 是否为 NULL，只有在不为 NULL 的情况下才进行比较  
        if (array_table[i].key != NULL && strcmp(array_table[i].key, key) == 0) {  
            // 释放内存
            kvs_free(array_table[i].key);  
            array_table[i].key = NULL;  

            if (array_table[i].value != NULL) {  
                kvs_free(array_table[i].value);  
                array_table[i].value = NULL;  
            }  
            array_count--;  // 减少元素计数  
            return 0;  // 成功删除  
        }  
    }  
    return -1; // 未找到要删除的 key  
}  
int kvs_array_count(){
    return array_count;
}