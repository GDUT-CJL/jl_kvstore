#include "jl_Thrdpool.h"

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
    // 下面这两个顺序不能搞反了
    *queue->tail = link; //先让最后一位指向空
    queue->tail = link; // 再让尾指针指向最后一位
    spinlock_unlock(&queue->splock);
    pthread_cond_signal(&queue->cond);
}

task_t* _pop_task(queue_t* queue){
    spinlock_lock(&queue->splock);
    if(queue->head == NULL){
        spinlock_unlock(&queue->splock);
        return NULL;
    }
    task_t* task;
    task = queue->head;
    void** link = (void**)task;
    queue->head = *link;
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
        if (task) {  // 确保 task 不为 NULL
            task->cb(task->arg);
            free(task);  // 任务执行完毕后释放内存
        }
    }
    return NULL;
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
        free(pool->pthreads);
        free(pool->queue);
    }
}

void
destroy_thrdpool(thrdpool_t * pool) {
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

/*-------------------------------------------------------------------------------------------*/

#if 0
int done = 0;

pthread_mutex_t lock;

void do_taskA(void *arg) {
    thrdpool_t *pool = (thrdpool_t*)arg;
    pthread_mutex_lock(&lock);
    done++;
    printf("doing A task\n");
    pthread_mutex_unlock(&lock);
    if (done >= 1000) {
        destory_pool(pool);
    }
}
void do_taskB(void *arg) {
    thrdpool_t *pool = (thrdpool_t*)arg;
    pthread_mutex_lock(&lock);
    done++;
    printf("doing B task\n");
    pthread_mutex_unlock(&lock);
    if (done >= 1000) {
        destory_pool(pool);
    }
}

int main(){
    int threads = 8;
    pthread_mutex_init(&lock, NULL);// 初始化
    thrdpool_t *pool = create_thrdpool(threads);
    if (pool == NULL) {
        perror("thread pool create error!\n");
        exit(-1);
    }

    // for (int i = 0; i < 1000; i++) {  // 限制任务数量
    //     if (post_threadTask(pool, &do_task, pool) != 0) {
    //         break;
    //     }
    // }
        if (post_threadTask(pool, &do_taskA, pool) != 0) {
            return -1;
        }    
        if (post_threadTask(pool, &do_taskB, pool) != 0) {
            return -1;
        }   
    thrdpool_waitdone(pool);
    pthread_mutex_destroy(&lock);
}
#endif
