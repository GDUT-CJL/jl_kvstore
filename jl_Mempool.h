#pragma once
#include <stdint.h>

// mempool
#define JL_MP_ALIGNMENT     32
#define JL_MAX_POOLSIZE     4096
#define FLUSH_THRESHOLD 1024 // 刷盘阈值：1MB

#define JL_MAX_ALLOC_FROM_POOL     (JL_MAX_POOLSIZE - 1)
#define jl_align(n, alignment) (((n)+(alignment-1)) & ~(alignment-1))
#define jl_align_ptr(p, alignment) (void *)((((size_t)p)+(alignment-1)) & ~(alignment-1))
struct jl_pool_s *p;    //全局内存池  

typedef struct jl_large_s
{
    void* alloc;
    struct jl_large_s* next;
}jl_large_t;

typedef struct jl_data_s
{
    unsigned char* end;
    unsigned char* last;
    struct jl_pool_s* next;
    unsigned int failed;
    size_t data_size;         // 有效数据大小
}jl_data_t;

typedef struct jl_pool_s
{
    struct jl_data_s d ;
    struct jl_large_s* large;
    struct jl_pool_s* current;
    size_t max;

    size_t allocated_size;       // 当前已分配的内存大小（用于刷盘判断）
    //pthread_mutex_t lock;        // 互斥锁，用于线程安全
}jl_pool_t;

struct jl_pool_s* jl_create_pool(size_t size);
void jl_destory_pool(struct jl_pool_s *pool);
void jl_reset_pool(struct jl_pool_s *pool);
void* jl_alloc_block(struct jl_pool_s* pool,int size);
void* jl_alloc_large(struct jl_pool_s* pool,int size);
void* jl_alloc(struct jl_pool_s* pool,int size);
void* jl_calloc(jl_pool_t* pool,int size);
void jl_free(jl_pool_t* pool,void* p);
int jl_flush_to_disk(jl_pool_t* pool, const char* filename);