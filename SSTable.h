#ifndef LSM_TREE_SSTABLE_H
#define LSM_TREE_SSTABLE_H

#include <iostream>
#include <fstream>
#include <vector>
#include <memory>
#include <unordered_map>
#include "BloomFilter.h"
#include "constants.h"

using namespace std;

struct SSTHeader {
    TimeToken timeToken;
    size_t keyNumber;
    LsmKey minKey;
    LsmKey maxKey;

    SSTHeader() {}
    SSTHeader(TimeToken timeToken, size_t keyNumber,
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

typedef shared_ptr<unordered_map<LsmKey, LsmValue>> DataPtr;

class SSTable {

private:
    const size_t level;
    const SSTHeader header;
    const BloomFilter bloomFilter;
    const vector<DataIndex> dataIndexes;

    int64_t find(LsmKey k, vector<DataIndex> arr, int64_t start, int64_t end) const;
    LsmValue getValueFromDisk(size_t index) const;
    LsmValue readValueFromFile(ifstream& table, uint32_t startOffset, uint32_t endOffset, bool multiValue) const;

public:
    SSTable(size_t level,
            SSTHeader sstHeader,
            BloomFilter bloomFilter,
            vector<DataIndex> dataIndexes);

    LsmValue get(LsmKey k) const;
    size_t getLevel() const;
    TimeToken getTimeToken() const;
    LsmKey getMinKey() const;
    LsmKey getMaxKey() const;
    size_t getKeyNumber() const;
    vector<DataIndex> getDataIndexes() const;
    string getFilename() const;
    void getValuesFromDisk(KVPair& sstData) const;

};

typedef shared_ptr<SSTable> SSTPtr;

#endif //LSM_TREE_SSTABLE_H
