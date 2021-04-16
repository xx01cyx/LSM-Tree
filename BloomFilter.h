#ifndef LSM_TREE_BLOOMFILTER_H
#define LSM_TREE_BLOOMFILTER_H

#include "constants.h"
#include "MurmurHash3.h"

class BloomFilter {
private:
    bool* bitArray;

public:
    BloomFilter();
    ~BloomFilter();

    bool hasKey(LsmKey k);
    void insert(LsmKey k);
};


#endif //LSM_TREE_BLOOMFILTER_H
