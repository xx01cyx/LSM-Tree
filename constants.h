#ifndef LSM_TREE_CONSTANTS_H
#define LSM_TREE_CONSTANTS_H

#include <string>

typedef uint64_t LsmKey;
typedef std::string LsmValue;

#define DELETE_SIGN "~DELETED~"

#define HEADER_SIZE 32
#define BLOOM_FILTER_SIZE 10240
#define DATA_INDEX_SIZE 12
#define MAX_SSTABLE_SIZE 2097152

#define DATA_DIR "data/"

#endif //LSM_TREE_CONSTANTS_H
