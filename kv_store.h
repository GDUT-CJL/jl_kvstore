#pragma once
#include <arpa/inet.h>
#include <sys/time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "kv_protocol.h"
#include "kv_net.h"
#include "kv_array.h"
#include "kv_btree.h"
#include "kv_rbtree.h"
#include "kv_hash.h"
#include "kv_skiplist.h"
#include "kv_dhash.h"
#include "jl_Mempool.h"
#include "jl_Thrdpool.h"
#include "kv_reload.h"
#define     PATH_TO_FLUSH_DISK_TXT      "redo.log"
#define     PATH_TO_FLUSH_DISK_BIN      "redo.bin"
#define     ENABLE_RELOAD_BIN   0
#define     ENABLE_RELOAD_TXT   1
#define MAX_CLIENT_NUM			1000000
#define TIME_SUB_MS(tv1, tv2)  ((tv1.tv_sec - tv2.tv_sec) * 1000 + (tv1.tv_usec - tv2.tv_usec) / 1000)
void* kvs_malloc(size_t size);
void kvs_free(void* ptr);
void kv_flush_thread(void* arg);
#define ENABLE_LOG      1
#define ENABLE_THRDPOOL 0
// LOG
#if ENABLE_LOG
#define LOG(_fmt, ...) fprintf(stdout, "[%s:%d] " _fmt, __FILE__, __LINE__, __VA_ARGS__)
#endif




