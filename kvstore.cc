#include "kvstore.h"

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    memTable = make_shared<MemTable>();
    memTableSize = HEADER_SIZE + BLOOM_FILTER_SIZE;
    ssTables = vector<shared_ptr<vector<SSTPtr>>>();
    ssTables.push_back(make_shared<vector<SSTPtr>>());  // level 0
    timeToken = 0;
    lsmLevel = 0;
}

KVStore::~KVStore() {}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if (memTableOverflow(s)) {
        timeToken++;
        memToDisk();

//        uint64_t compactLevel = 0;
//        while (levelOverflow(compactLevel))
//            compact(compactLevel++);
//        if (compactLevel > lsmLevel)
//            lsmLevel++;
    }

    memTable->put(key, s);
    memTableSize += (DATA_INDEX_SIZE + s.size());
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    LsmValue memValue = memTable->get(key);
    if (memValue == DELETE_SIGN)
        return "";
    if (memValue.length() != 0)
        return memValue;
    return getValueFromDisk(key);
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    LsmValue value = get(key);
    bool find = (value.length() != 0) && (value != DELETE_SIGN);
    if (find)
        memTable->put(key, DELETE_SIGN);
    return find;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    memTable->reset();
}

bool KVStore::memTableOverflow(LsmValue v) {
    return memTableSize + DATA_INDEX_SIZE + v.size() > MAX_SSTABLE_SIZE;
}

/**
 * Write the data in memTable to the disk on overflowing.
 * Truncate memTable.
 */
void KVStore::memToDisk() {
    SSTPtr sst = memTable->writeToDisk(timeToken);   // Write the data into disk (level 0)
    ssTables[0]->push_back(sst);    // Append to level 0 cache
    memTable->reset();
    memTableSize = HEADER_SIZE + BLOOM_FILTER_SIZE;
}

LsmValue KVStore::getValueFromDisk(LsmKey key) {
//    uint64_t level = 0;
//    std::string directory = "/data/level" + to_string(level);
//    while (utils::dirExists(directory)) {
//        level++;
//    }

    uint64_t levels = ssTables.size();
    if (levels == 0)   // All the data are stored in memTable till now,
        return "";

    TimeToken maxTimeToken = 0;
    LsmValue newestValue = "";

    // Read from level 0.
    shared_ptr<vector<SSTPtr>> level0Tables = ssTables[0];
    int level0Number = level0Tables->size();

    for (uint64_t i = 0; i < level0Number; ++i) {
        SSTPtr sst = level0Tables->at(i);
        std::string filename = sst->getFilename();
        LsmValue value = sst->get(key);

        if (value != DELETE_SIGN && value.length() != 0) {
            TimeToken timeToken = sst->getTimeToken();
            if (timeToken > maxTimeToken) {
                newestValue = value;
                maxTimeToken = timeToken;
            }
        }
    }

    // Read from the rest levels.
    for (int level = 1; level < levels; level++) {

    }

    return newestValue;

}

/**
 * @return Whether the number of SSTables in a certain level overflows.
 */
bool KVStore::levelOverflow(uint64_t level) {
    return ssTables[level]->size() > (int)pow(2, level + 1);
}

/**
 * Compact level 0 into level 1. Clear level 0.
 */
void KVStore::compact0() {
    LsmKey minKey;
    LsmKey maxKey;

    getCompact0Range(minKey, maxKey);
    vector<SSTPtr> overlapSSTs = getCompact0SSTs(minKey, maxKey);

    vector<SSTPtr> SSTs = *ssTables[0];
    SSTs.insert(SSTs.end(), overlapSSTs.begin(), overlapSSTs.end());

    // Merge sort.

    // Write the sorted data into the disk.

    // Delete the overlap SSTs.

}

void KVStore::getCompact0Range(LsmKey& minKey, LsmKey& maxKey) {
    vector<SSTPtr> L0SSTs = *ssTables[0];
    minKey = min(min(L0SSTs[0]->getMinKey(), L0SSTs[1]->getMinKey()),
                 L0SSTs[2]->getMinKey());
    maxKey = max(max(L0SSTs[0]->getMaxKey(), L0SSTs[1]->getMaxKey()),
                 L0SSTs[2]->getMaxKey());
}

/**
 * Find overlap SSTables in L1 for L0 compaction.
 * @param minKey: Minimum key in L0.
 * @param maxKey: Maximum key in L0.
 * @param overlapSSTs: Overlap SSTables in L1.
 */
vector<SSTPtr> KVStore::getCompact0SSTs(LsmKey minKey, LsmKey maxKey) {

    vector<SSTPtr> overlapSSTs;

    if (ssTables.size() == 1)   // No L1
        return overlapSSTs;

    vector<SSTPtr> L1SSTs = *ssTables[1];
    bool overlap = false;
    for (auto it = L1SSTs.cbegin(); it != L1SSTs.cend(); ++it) {
        if (!overlap && (*it)->getMaxKey() >= minKey && (*it)->getMinKey() <= minKey)
            overlap = true;
        if ((*it)->getMinKey() < maxKey)
            break;
        if (overlap)
            overlapSSTs.push_back(*it);
    }

    return overlapSSTs;
}

//vector<SSTPtr> KVStore::getCompactSSTs(uint64_t level) {
//
//    vector<SSTPtr> levelSSTs = *ssTables[level];
//    vector<SSTPtr> compactSSTs;
//
//    if (level == 0)
//        compactSSTs = levelSSTs;
//    else {
//        size_t currentLevelNumber = levelSSTs.size();
//        size_t maxLevelNumber = (int)pow(2, level + 1);
//        size_t overflowNumber = currentLevelNumber - maxLevelNumber;
//        while (overflowNumber-- > 0)
//            compactSSTs.push_back(levelSSTs[currentLevelNumber - overflowNumber]);
//    }
//
//    return compactSSTs;
//}

/**
 * @param SSTs: SSTables to retrieve key-value pairs.
 * @return Pairs of key and its latest value stored in an unordered map.
 */
KVPair KVStore::readDataFromDisk(const vector<SSTPtr>& SSTs) {

    // Sort the SSTs according to their time tokens so that the value of the same key
    // in a newer SST will always replace the previous one.

    auto sortByTimeToken = [](const SSTPtr sstA, const SSTPtr sstB) {
        return sstA->getTimeToken() < sstB->getTimeToken();
    };

    vector<SSTPtr> tempSSTs(SSTs);
    sort(tempSSTs.begin(), tempSSTs.end(), sortByTimeToken);

    KVPair sstData;
    for (auto sst : tempSSTs)
        sst->getValuesFromDisk(sstData);

    return sstData;
}

