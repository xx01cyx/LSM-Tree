#ifndef LSM_TREE_CONSTANTS_H
#define LSM_TREE_CONSTANTS_H

#include <string>

typedef uint64_t LsmKey;
typedef std::string LsmValue;

#define DELETE_SIGN "~DELETED~"

#define BLOOM_FILTER_BYTE_LENGTH 10240

#endif //LSM_TREE_CONSTANTS_H
