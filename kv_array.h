// array
#pragma once
#define MAX_MSGBUFFER_LENGTH	1024
#define MAX_ARRAY_NUMS	102400
typedef struct kvs_array_item_s{
	char* key;
	char* value;
    long long expired;
}kvs_array_item_t;
struct kvs_array_item_s array_table[MAX_ARRAY_NUMS];

kvs_array_item_t* kvs_array_search_item(const char* key);
int kvs_array_exist(const char* key);
int kvs_array_set(char* key,char* value);
int kvs_set_array_expired(char* key,char* value,char* cmd,int expired);
char* kvs_array_get(const char* key);
int kvs_array_delete(const char* key);
int kvs_array_count();
/*--------------------------------------------------------------------*/
