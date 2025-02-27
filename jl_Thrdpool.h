#pragma once
#include "kv_store.h"
typedef void(*callback)(void* arg);

typedef struct thrdpool_s thrdpool_t;

// 创建线程池
thrdpool_t* create_thrdpool(int thrd_count);
// 销毁线程池
void destory_pool(thrdpool_t* pool);
// 添加任务
int post_threadTask(thrdpool_t* pool,callback cb,void* arg);
// 等待回收
void thrdpool_waitdone(thrdpool_t *pool);

