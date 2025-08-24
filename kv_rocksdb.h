#pragma once
struct rocks_obj* rocksdb_obj;
// 初始化 RocksDB
int init_Rocksdb();
// 关闭并清理 RocksDB 资源
void close_Rocksdb(struct rocks_obj* obj);
// 设置键值对
int kvs_rocksdb_set(struct rocks_obj* obj, const char* key, const char* value);
// 获取键值对
char* kvs_rocksdb_get(struct rocks_obj* obj, const char* key);
// 删除键值对
int kvs_rocksdb_delete(struct rocks_obj* obj, const char* key);
// 创建备份
int kvs_rocksdb_create_backup(struct rocks_obj* obj) ;
// 批量写入接口
int kvs_rocksdb_batch_set(struct rocks_obj* obj, const char** keys, const char** values, int count);
