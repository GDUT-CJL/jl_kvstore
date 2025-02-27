# jl_kvstore
Author:GDUT-CJL
a kvstore start from 2025-2-8

# Time used and QPS for each engine about 10w "set" command:
array used time:56928.000000 ms,qps: 1756.60
rbtree used time:8814.000000 ms,qps: 11345.59
hash used time:8762.000000 ms,qps: 11412.92 （Zipper method）
skiplist used time:53811.000000 ms,qps: 1858.36
btree used time:53017.000000 ms,qps: 1886.19
dhash used time:54314.000000 ms,qps: 1841.15 （Linear exploration）


# 1w "set" command Time used and QPS among no using disk flashing,Synchronize disk flashing and Asynchronous disk flashing
no using disk flashing: array used time:3741.000000 ms,qps: 2673.08
using Synchronize disk flashing: array used time:40227.000000 ms,qps: 248.59
using thrdpool Asynchronous disk flashing:

