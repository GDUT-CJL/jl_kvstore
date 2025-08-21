#include "kv_store.h"
#include <sys/time.h>
static void _clean_expired_task(){
#if 1
	struct timeval tv;
	gettimeofday(&tv,NULL);
	long long cur_time = (tv.tv_sec * 1000LL) + (tv.tv_usec / 1000);
	for(int i = 0; i < MAX_ARRAY_NUMS; ++i){
		kvs_array_item_t* enter = &array_table->array[i];
		if(enter->key != NULL && enter->value != NULL){
			if(enter->expired != 0 && cur_time > enter->expired){
				kvs_free(enter->key);
				kvs_free(enter->value);
				enter->key = NULL;
				enter->expired = 0;
				enter->value = NULL;
				array_table->array_count--;
			}
		}
	}
#endif
}

int init_array(){
	array_table = (kvs_array_t*)kvs_malloc(sizeof(kvs_array_t));
    if(!array_table)   return -1;

	pthread_mutex_init(&array_table->array_mutex,NULL);

	array_table->array = (kvs_array_item_t*)kvs_malloc(sizeof(kvs_array_item_t)*MAX_ARRAY_NUMS);
	if(array_table->array){
		pthread_mutex_destroy(&array_table->array_mutex);
		return -1;
	}

	array_table->array->key = (char*)kvs_malloc(sizeof(char));
	if(!array_table->array->key){
		pthread_mutex_destroy(&array_table->array_mutex);
		kvs_free(array_table->array);
		return -1;
	}

	array_table->array->value = (char*)kvs_malloc(sizeof(char));
	if(!array_table->array->value){
		pthread_mutex_destroy(&array_table->array_mutex);
		kvs_free(array_table->array);
		kvs_free(array_table->array->key);
		return -1;
	}

    return 0;
}

kvs_array_item_t* kvs_array_search_item(const char* key){
	if(!key) return NULL;
	_clean_expired_task();
	for(int idx = 0; idx < MAX_ARRAY_NUMS;idx++){
		if (array_table->array[idx].key == NULL) {
			continue;
		}
		if((strcmp(array_table->array[idx].key,key) == 0)){
			return &array_table->array[idx];
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
	if(key == NULL || value == NULL || array_table->array_count == MAX_ARRAY_NUMS - 1) return -1;
	//_clean_expired_task();
	pthread_mutex_lock(&array_table->array_mutex);
	char* kcopy = (char*)kvs_malloc(strlen(key)+1);
	if(!kcopy){
		pthread_mutex_unlock(&array_table->array_mutex);
		return -1;
	} 

	char* vcopy = (char*)kvs_malloc(strlen(value)+1);
	if(!vcopy){
		pthread_mutex_unlock(&array_table->array_mutex);
		kvs_free(kcopy);
		return -1;
	}
	strncpy(kcopy,key,strlen(key)+1);
	strncpy(vcopy,value,strlen(value)+1);

	//int* time_copy = (int*)kvs_malloc(sizeof(int));
	//*time_copy = expired_time;
#if 0
	// 有问题，不能和delete配合，会发现delete完后count--数据会被覆盖
	array_table[array_count].key = kcopy;
	array_table[array_count].value = vcopy;
	array_count++;
#endif
	int i = 0;
	for(i = 0; i < MAX_ARRAY_NUMS;++i){
		if(array_table->array[i].key == NULL && array_table->array[i].value == NULL)
			break;
		if(strcmp(array_table->array[i].key, kcopy) == 0)
			break;
	}
	array_table->array[i].key = kcopy;
	array_table->array[i].value = vcopy;
	array_table->array[i].expired = expired_time;
	array_table->array_count++;
	pthread_mutex_unlock(&array_table->array_mutex);
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
        if (array_table->array[i].key != NULL && strcmp(array_table->array[i].key, key) == 0) {  
            // 释放内存
            kvs_free(array_table->array[i].key);  
            array_table->array[i].key = NULL;  

            if (array_table->array[i].value != NULL) {  
                kvs_free(array_table->array[i].value);  
                array_table->array[i].value = NULL;    
            }  
            array_table->array_count--;  // 减少元素计数  
            return 0;  // 成功删除  
        }  
    }  
    return -1; // 未找到要删除的 key  
}  
int kvs_array_count(){
    return array_table->array_count;
}