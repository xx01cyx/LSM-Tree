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
    TimeStamp timeStamp;
    size_t keyNumber;
    LsmKey minKey;
    LsmKey maxKey;

    SSTHeader() {}
    SSTHeader(TimeStamp timeStamp, size_t keyNumber,
              LsmKey minKey, LsmKey maxKey)
            : timeStamp(timeStamp), keyNumber(keyNumber),
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
    static LsmValue readValueFromFile(ifstream& table, uint32_t startOffset, uint32_t endOffset, bool multiValue) ;

public:
    SSTable(size_t level,
            SSTHeader sstHeader,
            BloomFilter bloomFilter,
            vector<DataIndex> dataIndexes);

    LsmValue get(LsmKey k) const;
    size_t getLevel() const;
    TimeStamp getTimeStamp() const;
    LsmKey getMinKey() const;
    LsmKey getMaxKey() const;
    size_t getKeyNumber() const;
    vector<DataIndex> getDataIndexes() const;
    string getFilename() const;
    vector<LsmKey> getKeys() const;
    void getValuesFromDisk(KVPair& sstData) const;
};

typedef shared_ptr<SSTable> SSTPtr;
typedef pair<SSTPtr, size_t> KeyRef;

struct SSTTimeStampPriorComparator {
    bool operator() (const SSTPtr& sst1, const SSTPtr& sst2) {
        TimeStamp stamp1 = sst1->getTimeStamp();
        TimeStamp stamp2 = sst2->getTimeStamp();
        LsmKey minKey1 = sst1->getMinKey();
        LsmKey minKey2 = sst2->getMinKey();
        if (stamp1 != stamp2)
            return stamp1 < stamp2;
        return minKey1 < minKey2;
    }
};

struct SSTKeyPriorComparator {
    bool operator() (const SSTPtr& sst1, const SSTPtr& sst2) {
        return sst1->getMinKey() < sst2->getMinKey();
    }
};

struct KeyRefGreaterThan {
    bool operator() (const KeyRef& ref1, const KeyRef& ref2) {
        LsmKey key1 = (ref1.first)->getDataIndexes()[ref1.second].key;
        LsmKey key2 = (ref2.first)->getDataIndexes()[ref2.second].key;
        return key1 > key2;
    }
};

struct KeyRefLessThan {
    bool operator() (const KeyRef& ref1, const KeyRef& ref2) {
        LsmKey key1 = (ref1.first)->getDataIndexes()[ref1.second].key;
        LsmKey key2 = (ref2.first)->getDataIndexes()[ref2.second].key;
        return key1 < key2;
    }
};

#endif //LSM_TREE_SSTABLE_H
