#pragma once
#include <pthread.h>
#include "kv_store.h"
#include "spinlock.h"
typedef void(*callback)(void* );

typedef struct task_s{
    void* next;
    callback cb;
    void* arg;
}task_t;

typedef struct queue_s{
    void* head;
    void** tail;
    int block;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    struct spinlock splock;
}queue_t;

typedef struct thrdpool_s{
    queue_t* queue;
    int count;
    atomic_int quit;
    pthread_t* pthreads;
}thrdpool_t;

// 创建线程池
thrdpool_t* create_thrdpool(int thrd_count);
// 销毁线程池
void destroy_thrdpool(thrdpool_t* pool);
// 添加任务
int post_threadTask(thrdpool_t* pool,callback cb,void* arg);
// 等待回收
void thrdpool_waitdone(thrdpool_t *pool);

