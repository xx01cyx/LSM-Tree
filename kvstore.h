#include <__bit_reference>

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

    void readAllSSTsFromDisk();
    static SSTPtr readSSTFromDisk(const string& filename, size_t level);
    void clearDisk();

    bool memTableOverflow(const LsmValue& v) const;
    void memToDisk();
    LsmValue getValueFromDisk(LsmKey key);
    uint32_t levelOverflow(size_t level);
    void detectAndHandleOverflow();

    void compact0();
    void compact(size_t upperLevel, uint32_t overflowNumber);
    void compactOneSST(const SSTPtr& sst, size_t lowerLevel);

    void getCompact0Range(LsmKey& minKey, LsmKey& maxKey);
    vector<SSTPtr> getOverlapSSTs(LsmKey minKey, LsmKey maxKey, size_t level,
                                  int64_t& minOverlapIndex, int64_t& maxOverlapIndex);
    vector<SSTPtr> getCompactSSTs(size_t upperLevel, uint32_t overflowNumber);

    vector<SSTPtr> merge0AndWriteToDisk(const vector<SSTPtr>& SSTs, TimeStamp maxTimeStamp, const KVPair& data);
    vector<SSTPtr> mergeAndWriteToDisk(const SSTPtr& upperLevelSST, const vector<SSTPtr>& lowerLevelSSTs,
                                              TimeStamp maxTimeStamp, const KVPair& data);

    // Reconstruction
    void reconstructL0();
    void reconstructUpperLevel(size_t upperLevel, const vector<SSTPtr>& compactSSTs);
    void reconstructLowerLevelDisk(int64_t minOverlapIndex, int64_t maxOverlapIndex, size_t lowerLevel);
    void reconstructLowerLevelMemory(int64_t minOverlapIndex, int64_t maxOverlapIndex,
                                     const vector<SSTPtr>& newSSTs, size_t lowerLevel);

    // Compaction utils
    static uint32_t sstBinarySearch(const vector<SSTPtr>& SSTs, LsmKey key, uint32_t left, uint32_t right);
    static TimeStamp getMaxTimeStamp(const vector<SSTPtr>& SSTs);
    static TimeStamp getMaxTimeStamp(const SSTPtr& oneSST, const vector<SSTPtr>& SSTs);
    static KVPair getCompactionData(const vector<SSTPtr>& SSTs);
    static KVPair getCompactionData(const SSTPtr& sst, const vector<SSTPtr>& SSTs);
    static SSTPtr generateNewSST(const vector<LsmKey>& keys, const KVPair& data, size_t level, TimeStamp maxTimeStamp);
    static void removeSSTFromDisk(const SSTPtr& delSST);


public:
    explicit KVStore(const std::string &dir);
    ~KVStore();

    void put(uint64_t key, const std::string &s) override;
    std::string get(uint64_t key) override;
    bool del(uint64_t key) override;
    void reset() override;

};
