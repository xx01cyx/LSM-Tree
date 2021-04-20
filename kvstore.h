#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <queue>
#include <utility>
#include <algorithm>
#include "kvstore_api.h"
#include "MemTable.h"
#include "SSTable.h"
#include "constants.h"
#include "utils.h"

class KVStore : public KVStoreAPI {

private:
    shared_ptr<MemTable> memTable;
    uint32_t memTableSize;
    unordered_map<size_t, shared_ptr<vector<SSTPtr>>> ssTables;
    TimeStamp timeStamp;

    bool memTableOverflow(LsmValue v);
    void memToDisk();
    LsmValue getValueFromDisk(LsmKey key);
    void getNewestValue(const SSTPtr sst, LsmKey key, TimeStamp& maxTimeStamp, LsmValue& newestValue);
    uint32_t levelOverflow(size_t level);

    void compact0();
    void compact(size_t upperLevel, uint32_t compactNumber);
    void compactOneSST(SSTPtr sst, size_t lowerLevel);
    void compactWithoutMerging(SSTPtr upperLevelSST, size_t lowerLevel);
    void getCompact0Range(LsmKey& minKey, LsmKey& maxKey);
    vector<SSTPtr> getOverlapSSTs(LsmKey minKey, LsmKey maxKey, size_t level,
                                  int64_t& minOverlapIndex, int64_t& maxOverlapIndex);
    void reconstructLowerLevel(uint32_t minOverlapIndex, uint32_t maxOverlapIndex,
                               const vector<SSTPtr>& newSSTs, size_t lowerLevel);
    void reconstructUpperLevel(size_t upperLevel);
    KVPair readDataFromDisk(const vector<SSTPtr>& SSTs);
    TimeStamp getMaxTimeStamp(const vector<SSTPtr>& SSTs);
    vector<SSTPtr> merge0AndWriteToDisk(const vector<SSTPtr>& SSTs);
    vector<SSTPtr> mergeAndWriteToDisk(SSTPtr upperLevelSST, const vector<SSTPtr>& lowerLevelSSTs);
    void conditionalPushAndWrite(LsmKey key, vector<LsmKey>& sortedKeys, size_t& currentSize, const KVPair& data,
                                 vector<SSTPtr>& newSSTs, size_t lowerLevel);
    SSTPtr generateNewSST(const vector<LsmKey>& keys, const KVPair& data, size_t level);
    uint32_t sstBinarySearch(const vector<SSTPtr>& SSTs, LsmKey key, uint32_t left, uint32_t right);
    void clearL0();

public:
    KVStore(const std::string &dir);
    ~KVStore();

    void put(uint64_t key, const std::string &s) override;
    std::string get(uint64_t key) override;
    bool del(uint64_t key) override;
    void reset() override;

};
