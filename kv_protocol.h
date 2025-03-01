#pragma once
int kvs_parser_protocol(char *msg,char**buf,int count);
int kvs_split_str(char** tokens,char* msg);
int kvs_protocol(char* msg,int length);
