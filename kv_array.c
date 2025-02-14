#include "kv_store.h"
int array_count = 0;
// struct kvs_array_item_s* array_table;


// int initial_array(){
// 	array_table = (struct kvs_array_item_s*)kvs_malloc(MAX_ARRAY_NUMS);
// 	if(!array_table) return -1;
// 	memset(array_table,0,MAX_ARRAY_NUMS * (sizeof(struct kvs_array_item_s)));
// 	return 0;
// }

kvs_array_item_t* kvs_array_search_item(const char* key){
	if(!key) return NULL;
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

// array set 
int kvs_array_set(char* key,char* value){
	if(key == NULL || value == NULL || array_count == MAX_ARRAY_NUMS - 1) return -1;

	char* kcopy = (char*)kvs_malloc(strlen(key)+1);
	if(!kcopy) return -1;
	char* vcopy = (char*)kvs_malloc(strlen(value)+1);
	if(!vcopy){
		free(kcopy);
		return -1;
	}
	strncpy(kcopy,key,strlen(key)+1);
	strncpy(vcopy,value,strlen(value)+1);
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
	array_count++;
	return 0;
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
	if(!key) return -1;
	for(int i = 0; i < MAX_ARRAY_NUMS; i++){
		if(strcmp(array_table[i].key,key) == 0){
			if(array_table[i].key != NULL){
				kvs_free(array_table[i].key);
				array_table[i].key = NULL;
			}

			if(array_table[i].value != NULL){
				kvs_free(array_table[i].value);
				array_table[i].value = NULL;
			}
			array_count--;
			return 0;
		}
	}
	return -1;
}

int kvs_array_count(){
    return array_count;
}


