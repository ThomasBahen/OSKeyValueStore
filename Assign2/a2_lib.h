#ifndef __A2_LIB_HEADER__
#define __A2_LIB_HEADER__

#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <sys/mman.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <time.h>
#include <string.h>
#include <stdbool.h>
#include <string.h>

/* -------------------------------------
	Define your own globals here
----------------------------------------



---------------------------------------- */
#define __KV_STORE_NAME__ "/THOMASBAHEN"
#define __KEY_VALUE_STORE_SIZE__ 4+8000*64   //50MB
#define __KV_READERS_SEMAPHORE__ "/reader_THOMASBAHHEN"
#define __KV_WRITERS_SEMAPHORE__ "/writer_THOMASBAHHEN"
#define KV_EXIT_SUCCESS	0
#define KV_EXIT_FAILURE	-1
#define MAX_KEY_SIZE__ 32
#define KEY_INFO_SIZE 72
#define MAX_DATA_LENGTH__ 256
#define MAX_POD_ENTRY__ 64
#define POD_INFO_SIZE 128
#define KEYS_PER_POD 6
#define VALUES_PER_POD 30
#define VALUES_PER_KEY 5
#define POD_SIZE 8000
#define INT_SIZE 4

unsigned long generate_hash(unsigned char *str);

int kv_store_create(char *kv_store_name);
int kv_store_write(char *key, char *value);
char *kv_store_read(char *key);
char **kv_store_read_all(char *key);

#endif
