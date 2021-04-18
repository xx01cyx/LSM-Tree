#ifndef LSM_TREE_SSTABLE_H
#define LSM_TREE_SSTABLE_H

#include <iostream>
#include <fstream>
#include <vector>
#include <memory>
#include "BloomFilter.h"
#include "constants.h"

using namespace std;

struct SSTHeader {
    uint64_t timeToken;
    uint64_t keyNumber;
    LsmKey minKey;
    LsmKey maxKey;

    SSTHeader() {}
    SSTHeader(uint64_t timeToken, uint64_t keyNumber,
              LsmKey minKey, LsmKey maxKey)
            : timeToken(timeToken), keyNumber(keyNumber),
              minKey(minKey), maxKey(maxKey) {}
};

struct DataIndex {
    LsmKey key;
    uint32_t offset;

    DataIndex() {}
    DataIndex(LsmKey key, uint32_t offset)
            : key(key), offset(offset) {}
};

typedef shared_ptr<DataIndex> DataIndexPtr;

class SSTable {

private:
    SSTHeader header;
    BloomFilter bloomFilter;
    vector<DataIndexPtr> dataIndexes;

    int64_t find(LsmKey k, vector<DataIndexPtr> arr, int64_t start, int64_t end) const;
    LsmValue getFromDisk(int64_t index, string filename) const;

public:
    SSTable();
    SSTable(SSTHeader sstHeader,
            BloomFilter bloomFilter,
            vector<DataIndexPtr> dataIndexes);

    LsmValue get(LsmKey k, string filename) const;
    uint64_t getTimeToken() const;

};

typedef shared_ptr<SSTable> SSTPtr;

#endif //LSM_TREE_SSTABLE_H
