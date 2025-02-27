#include "jl_Thrdpool.h"
#include <pthread.h>
#include "spinlock.h"

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

queue_t* _create_taskQ(){
    queue_t* queue = (queue_t*)malloc(sizeof(queue_t));
    if(queue){
        int ret;
        ret = pthread_mutex_init(&queue->mutex,NULL);
        if(ret == 0){
            ret = pthread_cond_init(&queue->cond,NULL);
            if(ret == 0){
                spinlock_init(&queue->splock);
                queue->block = 1;
                queue->head = NULL;
                queue->tail = &queue->head;
                return queue;
            }
            pthread_mutex_destroy(&queue->mutex);
        }
    }
    return NULL;
}

void _set_nonblock(queue_t* queue){
    pthread_mutex_lock(&queue->mutex);
    queue->block = 0;
    pthread_mutex_unlock(&queue->mutex);
    pthread_cond_broadcast(&queue->cond);// 唤醒所有阻塞在cond的线程
}

void _push_task(queue_t* queue,void* task){
    void** link = (void**) task;
    *link = NULL;
    spinlock_lock(&queue->splock);
    queue->tail = link;
    *queue->tail = link;
    spinlock_unlock(&queue->splock);
    pthread_cond_signal(&queue->cond);
}

task_t* _pop_task(queue_t* queue){
    spinlock_lock(&queue->splock);
    if(queue->head == NULL){
        spinlock_unlock(&queue->splock);
    }
    task_t* task;
    task = queue->head;
    void** link = (void**)task;
    queue->head = task;
    if(queue->head == NULL){
        queue->tail = &queue->head;
    }
    spinlock_unlock(&queue->splock);
    return task;
}

task_t* _get_task(queue_t* queue){
    task_t* task;
    while ((task = _pop_task(queue)) == NULL)
    {
        pthread_mutex_lock(&queue->mutex);
        if(queue->block == 0){
            pthread_mutex_unlock(&queue->mutex);
            return NULL;
        }
        pthread_cond_wait(&queue->cond,&queue->mutex);
        pthread_mutex_unlock(&queue->mutex);
    }
    return task;
}

void _taskqueue_destroy(queue_t* queue){
    task_t* task;
    while((task = _pop_task(queue)) != NULL){
        free(task);
    }
    pthread_mutex_destroy(&queue->mutex);
    pthread_cond_destroy(&queue->cond);
    spinlock_destroy(&queue->splock);
    free(queue);
}

void* do_work(void* arg){
    thrdpool_t* pool = (thrdpool_t*)arg;
    while(atomic_load(&pool->quit) == 0){
        task_t* task = _get_task(pool->queue);
        task->cb(task->arg);
    }
}

void _destory_threads(thrdpool_t* pool){
    atomic_store(&pool->quit,1);
    _set_nonblock(pool->queue);
    for(int i = 0; i < pool->count; ++i){
        pthread_join(pool->pthreads[i],NULL);
    }
}

int _create_threads(thrdpool_t* pool,int count){
    pool->pthreads = (pthread_t*)malloc(count * sizeof(pthread_t));
    if(!pool->pthreads){
        return -1;
    }
    int i;
    for(i = 0; i < count; ++i){
       if(pthread_create(&pool->pthreads[i],NULL,do_work,pool)!=0){
        break;
       }
    }

    pool->count = i;
    if(pool->count == count){
        return 0;
    }else{
        _destory_threads(pool);
        free(pool->queue);
    }
}

void
destory_pool(thrdpool_t * pool) {
    atomic_store(&pool->quit, 1);
    _set_nonblock(pool->queue);
}

thrdpool_t* create_thrdpool(int thrd_count){
    thrdpool_t* pool = (thrdpool_t*)malloc(sizeof(thrdpool_t));
    if(pool){
        pool->queue = _create_taskQ();
        if(pool->queue){
            atomic_store(&pool->quit,0);
            if(_create_threads(pool,thrd_count) == -1){
                _destory_threads(pool);
            }
            return pool;
        }
        free(pool);
    }
    return NULL;
}

int post_threadTask(thrdpool_t* pool,callback cb,void* arg){
    if(atomic_load(&pool->quit) == 1)
        return -1;
    task_t* task = (task_t*)malloc(sizeof(task_t));
    if(task){
        task->cb = cb;
        task->arg = arg;
        _push_task(pool->queue,task);
        return 0;
    }
    return -1;
}

void thrdpool_waitdone(thrdpool_t *pool){
    int i;
    for(i = 0; i < pool->count; ++i){
        pthread_join(pool->pthreads[i],NULL);
    }
    _taskqueue_destroy(pool->queue);
    free(pool->pthreads);
    free(pool);
}

int main(){
    thrdpool_t* pool = create_thrdpool(2);

    return 0;
}
