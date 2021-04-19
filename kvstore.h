#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <queue>
#include <algorithm>
#include "kvstore_api.h"
#include "MemTable.h"
#include "SSTable.h"
#include "constants.h"

class KVStore : public KVStoreAPI {

private:
    shared_ptr<MemTable> memTable;
    uint32_t memTableSize;
    vector<shared_ptr<vector<SSTPtr>>> ssTables;
    TimeToken timeToken;
    uint64_t lsmLevel;

    bool memTableOverflow(LsmValue v);
    void memToDisk();
    LsmValue getValueFromDisk(LsmKey key);

    bool levelOverflow(uint64_t level);
    void compact0();
    void getCompact0Range(LsmKey& minKey, LsmKey& maxKey);
    vector<SSTPtr> getCompact0SSTs(LsmKey minKey, LsmKey maxKey);

    KVPair readDataFromDisk(const vector<SSTPtr>& SSTs);
    vector<SSTPtr> mergeSort(const vector<SSTPtr>& SSTs);

public:
    KVStore(const std::string &dir);
    ~KVStore();

    void put(uint64_t key, const std::string &s) override;
    std::string get(uint64_t key) override;
    bool del(uint64_t key) override;
    void reset() override;

};
