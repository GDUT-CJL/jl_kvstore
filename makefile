# Makefile for kvstore  

# 源文件  
SRC = kv_store.c kv_array.c kv_rbtree.c kv_hash.c kv_btree.c kv_skiplist.c kv_dhash.c jl_Mempool.c kv_flush.c jl_Thrdpool.c kv_net.c kv_protocol.c  

# 目标文件  
OBJ = $(patsubst %.c, objs/%.o, $(SRC))  

# 输出可执行文件名  
TARGET = kvstore  

# 编译器和链接器设置  
CC = gcc  

# 编译选项  
CFLAGS = -I ./NtyCo/core/  

# 链接选项  
LDFLAGS = -L ./NtyCo/ -lntyco -lpthread -ldl  

# 默认目标  
all: $(TARGET)  

# 生成可执行文件  
$(TARGET): $(OBJ)  
	$(CC) -o $@ $^ $(LDFLAGS)  

# 生成中间目标文件  
objs/%.o: %.c  
	@mkdir -p objs
	$(CC) $(CFLAGS) -c $< -o $@  

# 清理生成的文件  
clean:  
	rm -f $(OBJ) $(TARGET)  
	rmdir objs || true

.PHONY: all clean