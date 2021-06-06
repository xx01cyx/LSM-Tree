#ifndef LSM_TREE_BLOOMFILTER_H
#define LSM_TREE_BLOOMFILTER_H

#include <cstring>
#include "constants.h"
#include "MurmurHash3.h"

class BloomFilter {
public:
    bool* byteArray;

    BloomFilter();
    explicit BloomFilter(bool* byteArray);
    ~BloomFilter();

    bool hasKey(LsmKey k) const;
    void insert(LsmKey k);
};


#endif //LSM_TREE_BLOOMFILTER_H
