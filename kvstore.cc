#include "kvstore.h"

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    if (!utils::dirExists(dir))
        utils::mkdir(dir.c_str());

    memTable = make_shared<MemTable>();
    memTableSize = HEADER_SIZE + BLOOM_FILTER_SIZE;
    ssTables = unordered_map<size_t, shared_ptr<vector<SSTPtr>>>();
    ssTables[0] = make_shared<vector<SSTPtr>>();
    timeStamp = 1;

    readAllSSTsFromDisk();
    detectAndHandleOverflow();

}

KVStore::~KVStore() {
    if (!memTable->empty()) {
        memToDisk();
        detectAndHandleOverflow();
    }
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if (memTableOverflow(s)) {
        memToDisk();

        // Update states
        memTable->reset();
        memTableSize = HEADER_SIZE + BLOOM_FILTER_SIZE;

        detectAndHandleOverflow();
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

    LsmValue sstValue = getValueFromDisk(key);
    if (sstValue == DELETE_SIGN)
        return "";
    return sstValue;
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    LsmValue value = get(key);
    bool find = (value.length() != 0) && (value != DELETE_SIGN);
    memTable->put(key, DELETE_SIGN);
    return find;
}

/**
 * Reset the LSM Tree. All key-value pairs should be removed, including
 * memtable and all SST files.
 */
void KVStore::reset()
{
    clearDisk();
    memTable->reset();
    memTableSize = HEADER_SIZE + BLOOM_FILTER_SIZE;
    ssTables.clear();
    ssTables[0] = make_shared<vector<SSTPtr>>();
    timeStamp = 1;
}

void KVStore::readAllSSTsFromDisk() {
    size_t level = 0;
    string levelDir = "./data/level-" + to_string(level) + "/";
    vector<string> filenames;
    bool emptyL0;

    while (utils::dirExists(levelDir)) {
        utils::scanDir(levelDir, filenames);
        vector<SSTPtr> levelSSTs;
        for (const auto& filename : filenames) {
            string sstName = levelDir + filename;
            SSTPtr sst = readSSTFromDisk(sstName, level);
            levelSSTs.push_back(sst);
        }

        if (level == 0) {
            if (levelSSTs.empty())
                emptyL0 = true;
            else if (levelSSTs.size() == 1)
                timeStamp = levelSSTs.front()->getTimeStamp() + 1;
            else {
                SSTTimeStampPriorComparator timeStampLessThan;
                SSTPtr firstSST = levelSSTs[0];
                SSTPtr secondSST = levelSSTs[1];
                timeStamp = timeStampLessThan(firstSST, secondSST) ?
                            secondSST->getTimeStamp() + 1 : firstSST->getTimeStamp() + 1;
            }
        } else {
            if (level == 1 && emptyL0)
                timeStamp = getMaxTimeStamp(levelSSTs) + 1;
            SSTKeyPriorComparator sstComparator;
            sort(levelSSTs.begin(), levelSSTs.end(), sstComparator);
        }
        ssTables[level] = make_shared<vector<SSTPtr>>(levelSSTs);

        ++level;
        levelDir = "./data/level-" + to_string(level) + "/";
        filenames.clear();
    }
}

SSTPtr KVStore::readSSTFromDisk(const string& filename, size_t level) {
    SSTHeader sstHeader;
    bool* byteArray = new bool[BLOOM_FILTER_SIZE];
    vector<DataIndex> dataIndexes;

    ifstream sstFile(filename, ios::binary | ios::in);
    if (!sstFile) {
        cerr << "Cannot open file `" << filename << "`." << endl;
        exit(-1);
    }

    sstFile.read((char*)&sstHeader, HEADER_SIZE);
    sstFile.read((char*)byteArray, BLOOM_FILTER_SIZE);

    uint64_t keyNumber = sstHeader.keyNumber;
    for (uint32_t i = 0; i < keyNumber; ++i) {
        DataIndex dataIndex;
        sstFile.read((char*)&dataIndex, DATA_INDEX_SIZE);
        dataIndexes.push_back(dataIndex);
    }

    BloomFilter bloomFilter(byteArray);
    SSTPtr sst = make_shared<SSTable>(level, sstHeader, bloomFilter, dataIndexes);
    return sst;
}

/**
 * Remove all the SST files and corresponding directories in the disk.
 */
void KVStore::clearDisk() {
    size_t levelNumber = ssTables.size();
    for (size_t level = 0; level < levelNumber; ++level) {
        vector<SSTPtr> levelSSTs = *ssTables[level];
        for (const auto& sst : levelSSTs)
            utils::rmfile(sst->getFilename().c_str());
        string dir = "./data/level-" + to_string(level);
        utils::rmdir(dir.c_str());
    }
}

bool KVStore::memTableOverflow(const LsmValue& v) const {
    return memTableSize + DATA_INDEX_SIZE + v.size() > MAX_SSTABLE_SIZE;
}

void KVStore::detectAndHandleOverflow() {
    if (levelOverflow(0))
        compact0();

    size_t levelNumber = ssTables.size();
    for (size_t level = 1; level < levelNumber; ++level) {
        uint32_t overflowNumber = levelOverflow(level);
        if (!overflowNumber)
            break;
        compact(level, overflowNumber);
    }
}

/**
 * Write the data in memTable to the disk on overflowing.
 * Truncate memTable.
 */
void KVStore::memToDisk() {
    SSTPtr sst = memTable->writeToDisk(timeStamp);   // Write the data into disk (level 0)
    ssTables[0]->push_back(sst);    // Append to level 0 cache
    timeStamp++;
}

/**
 * @return The value corresponding with the key in the SST files in the disk
 * Retain "~DELETED~".
 */
LsmValue KVStore::getValueFromDisk(LsmKey key) {

    size_t levelNumber = ssTables.size();
    if (levelNumber == 0)   // All data are stored in memTable.
        return "";

    TimeStamp maxTimeStamp = 0;
    LsmValue newestValue;

    auto getNewestValue = [&](const SSTPtr& sst, LsmKey key) {
        LsmValue newValue = sst->get(key);
        TimeStamp newTimeStamp = sst->getTimeStamp();
        if (newValue.length() != 0 && newTimeStamp > maxTimeStamp) {
            newestValue = newValue;
            maxTimeStamp = newTimeStamp;
        }
    };

    // Read from L0.
    vector<SSTPtr> L0SSTs = *ssTables[0];
    uint32_t L0SSTNumber = L0SSTs.size();
    for (int i = L0SSTNumber - 1; i >= 0; --i) {
        SSTPtr sst = L0SSTs[i];
        getNewestValue(sst, key);

        if (newestValue.length() != 0)
            return newestValue;
    }

    // Read from the rest levels.
    for (size_t n = 1; n < levelNumber; n++) {
        vector<SSTPtr> levelSSTs = *ssTables[n];
        if (key < levelSSTs.front()->getMinKey() || key > levelSSTs.back()->getMaxKey())
            continue;
        uint32_t sstIndex = sstBinarySearch(levelSSTs, key, 0, levelSSTs.size() - 1);
        SSTPtr targetSST = levelSSTs[sstIndex];
        getNewestValue(targetSST, key);

        if (newestValue.length() != 0)
            return newestValue;
    }

    return newestValue;

}

/**
 * @return Number of overflowing SSTs in the level.
 */
uint32_t KVStore::levelOverflow(size_t level) {
    int currentNumber = ssTables[level]->size();
    int maxNumber = (uint32_t)pow(2, level + 1);
    int overflowNumber = currentNumber - maxNumber;
    return overflowNumber > 0 ? overflowNumber : 0;
}

/**
 * Compact all SSTs in L0 into L1. Clear L0.
 */
void KVStore::compact0() {

    vector<SSTPtr> SSTs = *ssTables[0];
    if (SSTs.size() != 3) {
        cerr << "Number of SSTs in L0 is not 3. L0-compaction error." << endl;
        exit(-1);
    }

    if (!ssTables.count(1))
        ssTables[1] = make_shared<vector<SSTPtr>>();

    // Get the overall interval of SSTs in L0.
    LsmKey minKey, maxKey;
    getCompact0Range(minKey, maxKey);

    // Get all the SSTs in L0 and L1 that need merge.
    int64_t minOverlapIndex = -1;
    int64_t maxOverlapIndex = -1;
    vector<SSTPtr> overlapSSTs = getOverlapSSTs(minKey, maxKey, 1,
                                                minOverlapIndex, maxOverlapIndex);

    // Get all k-v pairs from the disk.
    SSTs.insert(SSTs.end(), overlapSSTs.begin(), overlapSSTs.end());
    KVPair data = getCompactionData(SSTs);

    // Remove the overlapping SST files in the disk.
    reconstructLowerLevelDisk( minOverlapIndex, maxOverlapIndex, 1);

    // Sort the keys and write the data into the disk.
    TimeStamp maxTimeStamp = getMaxTimeStamp(SSTs);
    vector<SSTPtr> mergedSSTs = merge0AndWriteToDisk(SSTs, maxTimeStamp, data);

    // Reconstruct L1 in memory.
    reconstructLowerLevelMemory(minOverlapIndex, maxOverlapIndex, mergedSSTs, 1);

    // Clear L0 in memory and delete corresponding files in the disk.
    reconstructL0();

}


/**
 * Compact SSTs from an upper level to a lower level one by one.
 * @param level: The upper level that overflows.
 * @param compactNumber: Number of SSTs in the upper level to be compacted.
 */
void KVStore::compact(size_t upperLevel, uint32_t overflowNumber) {

    // New the lower level if it does not exist.
    size_t lowerLevel = upperLevel + 1;
    if (!ssTables.count(lowerLevel)) {
        ssTables[lowerLevel] = make_shared<vector<SSTPtr>>();
    }

    // Find the SSTs possessing the smallest time stamps or minimum keys.
    vector<SSTPtr> compactSSTs = getCompactSSTs(upperLevel, overflowNumber);

    // Compact the SSTs one by one.
    for (const auto& compactSST : compactSSTs)
        compactOneSST(compactSST, lowerLevel);

    // Reconstruct the upper level.
    reconstructUpperLevel(upperLevel, compactSSTs);

}

/**
 * Compact one SST from the upper level to the lower level.
 * Reconstruct the lower level in both the memory and the disk.
 * @param sst: The upper level SST need compact.
 * @param lowerLevel: The lower level where the SST is to compact into.
 */
void KVStore::compactOneSST(const SSTPtr& sst, size_t lowerLevel) {

    int64_t minOverlapIndex = -1;
    int64_t maxOverlapIndex = -1;
    vector<SSTPtr> overlapSSTs = getOverlapSSTs(sst->getMinKey(), sst->getMaxKey(), lowerLevel,
                                                minOverlapIndex, maxOverlapIndex);

    KVPair data = getCompactionData(sst, overlapSSTs);

    reconstructLowerLevelDisk(minOverlapIndex, maxOverlapIndex, lowerLevel);

    TimeStamp maxTimeStamp = getMaxTimeStamp(sst, overlapSSTs);
    vector<SSTPtr> mergedSSTs;

    if (overlapSSTs.empty())                 // no overlapping
        mergedSSTs.push_back(generateNewSST(sst->getKeys(), data, lowerLevel, maxTimeStamp));
    else                                     // does overlap
        mergedSSTs = mergeAndWriteToDisk(sst, overlapSSTs, maxTimeStamp, data);

    reconstructLowerLevelMemory(minOverlapIndex, maxOverlapIndex, mergedSSTs, lowerLevel);

}

void KVStore::getCompact0Range(LsmKey& minKey, LsmKey& maxKey) {
    vector<SSTPtr> L0SSTs = *ssTables[0];
    minKey = min(min(L0SSTs[0]->getMinKey(), L0SSTs[1]->getMinKey()),
                 L0SSTs[2]->getMinKey());
    maxKey = max(max(L0SSTs[0]->getMaxKey(), L0SSTs[1]->getMaxKey()),
                 L0SSTs[2]->getMaxKey());
}

/**
 * Follow the rule that compactions occurs on SSTs possessing the smallest
 * time stamps. If time stamps are equal, select SSTs with smallest
 * minimum keys.
 * @param level: The level that a number of SSTs overflow.
 * @param overflowNumber: The number of the overflowing SSTs. Always greater
 * than 0.
 * @return An array of the SSTs that need compaction.
 */
vector<SSTPtr> KVStore::getCompactSSTs(size_t level, uint32_t overflowNumber) {

    vector<SSTPtr> levelSSTs = *ssTables[level];
    SSTTimeStampPriorComparator sstComparator;
    sort(levelSSTs.begin(), levelSSTs.end(), sstComparator);

    vector<SSTPtr> compactSSTs;
    for (uint32_t i = 0; i < overflowNumber; ++i)
        compactSSTs.push_back(levelSSTs[i]);

    return compactSSTs;

}

/**
 * Find overlapping SSTables in the lower level for a compaction. The overlapping
 * interval is [minOverlapIndex, maxOverlapIndex).
 * @param minKey: Minimum key in the upper level.
 * @param maxKey: Maximum key in the lower level.
 * @param overlapSSTs: Overlapping SSTables in the lower level.
 */
vector<SSTPtr> KVStore::getOverlapSSTs(LsmKey minKey, LsmKey maxKey, size_t level,
                                       int64_t& minOverlapIndex, int64_t& maxOverlapIndex) {

    vector<SSTPtr> overlapSSTs;

    if (ssTables[level]->empty())
        return overlapSSTs;

    vector<SSTPtr> levelSSTs = *ssTables[level];
    uint32_t length = levelSSTs.size();

    uint32_t leftIndex = sstBinarySearch(levelSSTs, minKey, 0, length - 1);

    if (levelSSTs[leftIndex]->getMaxKey() < minKey)
        if ((leftIndex == length - 1)
            || (leftIndex < length - 1 && levelSSTs[leftIndex+1]->getMinKey() > maxKey))
            return overlapSSTs;

    minOverlapIndex = leftIndex;
    maxOverlapIndex = 1 + sstBinarySearch(levelSSTs, maxKey, 0, length - 1);

    for (uint32_t i = minOverlapIndex; i < maxOverlapIndex; ++i)
        overlapSSTs.push_back(levelSSTs[i]);

    return overlapSSTs;
}


/**
 * Merge sort the SSTs need compact in L0 and L1 using priority queue.
 * Write the new SSTs into the disk.
 * @param SSTs: SSTs need compact in L0 and L1.
 * @return New SSTs generated during compaction.
 */
vector<SSTPtr> KVStore::merge0AndWriteToDisk(const vector<SSTPtr> &SSTs, TimeStamp maxTimeStamp, const KVPair& data) {

    vector<SSTPtr> newSSTs;
    priority_queue<KeyRef, vector<KeyRef>, KeyRefGreaterThan> pq;
    vector<LsmKey> sortedKeys;
    KeyRef currentRef;
    size_t currentSize = HEADER_SIZE + BLOOM_FILTER_SIZE;

    for (const auto & SST : SSTs)
        pq.push(make_pair(SST, 0));

    while (!pq.empty()) {
        currentRef = pq.top();
        SSTPtr currentSST = currentRef.first;
        size_t currentIndex = currentRef.second;
        vector<DataIndex> currentDataIndexes = currentSST->getDataIndexes();
        LsmKey currentKey = currentDataIndexes[currentIndex].key;
        LsmValue currentValue = data.at(currentKey);

        if (sortedKeys.empty())
            sortedKeys.push_back(currentKey);

        if ((ssTables.size() != 2 || currentValue != DELETE_SIGN)
            && !sortedKeys.empty() && sortedKeys.back() != currentKey) {
            size_t sizeIncrement = DATA_INDEX_SIZE + currentValue.size();
            if (currentSize + sizeIncrement > MAX_SSTABLE_SIZE) {
                newSSTs.push_back(generateNewSST(sortedKeys, data, 1, maxTimeStamp));
                sortedKeys.clear();
                currentSize = HEADER_SIZE + BLOOM_FILTER_SIZE;
            }
            sortedKeys.push_back(currentKey);
            currentSize += sizeIncrement;
        }

        pq.pop();
        if (currentIndex != currentDataIndexes.size() - 1)
            pq.push(make_pair(currentSST, currentIndex + 1));
    }

    // Pack the remaining data into an SST.
    if (!sortedKeys.empty())
        newSSTs.push_back(generateNewSST(sortedKeys, data, 1, maxTimeStamp));

    return newSSTs;
}

/**
 * Merge sort the SSTs need compact in the upper level and the lower level.
 * Write the new SSTs into the disk.
 * @param upperLevelSST: The upper level SST that needs compact.
 * @param lowerLevelSSTs: An array of lower level SSTs that need compact, whose
 * size is at least 1.
 * @return New SSTs generated during compaction.
 */
vector<SSTPtr> KVStore::mergeAndWriteToDisk(const SSTPtr& upperLevelSST, const vector<SSTPtr>& lowerLevelSSTs,
                                            TimeStamp maxTimeStamp, const KVPair& data) {

    // Initialize.

    vector<SSTPtr> newSSTs;
    vector<LsmKey> sortedKeys;
    size_t currentSize = HEADER_SIZE + BLOOM_FILTER_SIZE;
    size_t upperKeyNumber = upperLevelSST->getKeyNumber();
    size_t upperIndex = 0;
    size_t lowerKeyNumber;
    size_t lowerIndex;
    size_t lowerLevel = upperLevelSST->getLevel() + 1;
    KeyRefLessThan keyRefLessThan;

    // Function for merging and writing data to the disk. Used in 2-way merge below.

    auto merge = [&](const SSTPtr& sst, size_t& index) {
        LsmKey key = sst->getDataIndexes()[index].key;
        const LsmValue& value = data.at(key);
        if ((lowerLevel != ssTables.size() || value != DELETE_SIGN)
            && !sortedKeys.empty() && sortedKeys.back() != key) {
            size_t sizeIncrement = DATA_INDEX_SIZE + value.size();
            if (currentSize + sizeIncrement > MAX_SSTABLE_SIZE) {
                newSSTs.push_back(generateNewSST(sortedKeys, data, lowerLevel, maxTimeStamp));
                sortedKeys.clear();
                currentSize = HEADER_SIZE + BLOOM_FILTER_SIZE;
            }
            sortedKeys.push_back(key);
            currentSize += sizeIncrement;
        }
        index++;
    };

    // 2-way merge sort and write data to the disk.

    for (const auto& lowerLevelSST : lowerLevelSSTs) {

        lowerIndex = 0;
        lowerKeyNumber = lowerLevelSST->getKeyNumber();

        if (sortedKeys.empty()) {
            LsmKey upperKey = upperLevelSST->getDataIndexes()[0].key;
            LsmKey lowerKey = lowerLevelSST->getDataIndexes()[0].key;
            if ((upperKey < lowerKey) ||
                (upperKey == lowerKey && upperLevelSST->getTimeStamp() < lowerLevelSST->getTimeStamp()))
                sortedKeys.push_back(upperKey);
            else
                sortedKeys.push_back(lowerKey);
        }

        while (upperIndex != upperKeyNumber && lowerIndex != lowerKeyNumber) {
            KeyRef upperKeyRef = make_pair(upperLevelSST, upperIndex);
            KeyRef lowerKeyRef = make_pair(lowerLevelSST, lowerIndex);

            if (keyRefLessThan(upperKeyRef, lowerKeyRef))
                merge(upperLevelSST, upperIndex);
            else
                merge(lowerLevelSST, lowerIndex);
        }

        while (lowerIndex != lowerKeyNumber)
            merge(lowerLevelSST, lowerIndex);
    }

    while (upperIndex != upperKeyNumber)
        merge(upperLevelSST, upperIndex);

    // Pack the remaining data into an SST.

    if (!sortedKeys.empty())
        newSSTs.push_back(generateNewSST(sortedKeys, data, lowerLevel, maxTimeStamp));

    return newSSTs;

}

/**
 * Clear L0 in memory.
 * Delete all the L0 files in the disk.
 */
void KVStore::reconstructL0() {
    vector<SSTPtr> L0SST = *ssTables[0];
    for (const auto& sst : L0SST)
        removeSSTFromDisk(sst);
    ssTables[0]->clear();
}

void KVStore::reconstructUpperLevel(size_t upperLevel, const vector<SSTPtr> &compactSSTs) {
    vector<SSTPtr> levelSSTs = *ssTables[upperLevel];
    for (const auto& compactSST : compactSSTs) {
        auto delIt = find(ssTables[upperLevel]->begin(), ssTables[upperLevel]->end(), compactSST);
        if (delIt != ssTables[upperLevel]->end()) {
            removeSSTFromDisk(*delIt);
            ssTables[upperLevel]->erase(delIt);
        }
    }
}

/**
 * Remove the overlapping SST files in the disk.
 */
void KVStore::reconstructLowerLevelDisk(int64_t minOverlapIndex, int64_t maxOverlapIndex, size_t lowerLevel) {

    if (minOverlapIndex == -1 && maxOverlapIndex == -1)
        return;

    vector<SSTPtr> previousSSTs = *ssTables[lowerLevel];
    for (uint32_t i = minOverlapIndex; i < maxOverlapIndex; ++i)
        removeSSTFromDisk(previousSSTs[i]);

}

void KVStore::reconstructLowerLevelMemory(int64_t minOverlapIndex, int64_t maxOverlapIndex,
                                          const vector<SSTPtr>& newSSTs, size_t lowerLevel) {

    vector<SSTPtr> previousSSTs = *ssTables[lowerLevel];
    uint32_t length = previousSSTs.size();
    vector<SSTPtr> updatedSSTs;

    if (length == 0)
        minOverlapIndex = maxOverlapIndex = 0;

    // If no overlapping, binary search for the insert position.
    if (minOverlapIndex == -1 && maxOverlapIndex == -1) {
        LsmKey insertMinKey = newSSTs.front()->getMinKey();
        if (insertMinKey < previousSSTs.front()->getMinKey())
            minOverlapIndex = maxOverlapIndex = 0;
        else
            minOverlapIndex = maxOverlapIndex =
                    sstBinarySearch(previousSSTs, insertMinKey, 0, length - 1) + 1;
    }

    // Old SSTs left to the overlapping SSTs.
    for (uint32_t i = 0; i < minOverlapIndex; ++i)
        updatedSSTs.push_back(previousSSTs[i]);

    // New SSTs that need insert into the level.
    for (const auto& newSST : newSSTs)
        updatedSSTs.push_back(newSST);

    // Old SSTs right to the overlapping SSTs.
    for (uint32_t i = maxOverlapIndex; i < length; ++i)
        updatedSSTs.push_back(previousSSTs[i]);

    // Save the new layer in the memory.
    ssTables[lowerLevel] = make_shared<vector<SSTPtr>>(updatedSSTs);

}


/**
 * Find the index of the SST where the key might exist according to the
 * minimum keys of the SSTs. Can also be used for searching for the
 * inserting position of one or some SSTs.
 * @param SSTs: An array of sorted SSTs according to their minimum keys.
 * @param key: Key to be found or minimum key of the SST to be inserted.
 * @param left: Left bound of search range.
 * @param right: Right bound of search range.
 * @return The index of the SST where the its minimum key is exactly smaller
 * than or equal to the searched key.
 */
uint32_t KVStore::sstBinarySearch(const vector<SSTPtr> &SSTs, LsmKey key,
                                  uint32_t left, uint32_t right) {
    if (left >= right) {
        if (right && SSTs[right]->getMinKey() > key)
            right--;
        return right;
    }

    uint32_t mid = left + (right - left) / 2;
    LsmKey midMinKey = SSTs[mid]->getMinKey();

    if (key < midMinKey)
        return sstBinarySearch(SSTs, key, left, mid ? mid - 1 : mid);
    if (key > midMinKey)
        return sstBinarySearch(SSTs, key, mid + 1, right);
    return mid;

}


TimeStamp KVStore::getMaxTimeStamp(const vector<SSTPtr> &SSTs) {
    TimeStamp maxTimeStamp = 0;
    for (const auto& SST : SSTs) {
        TimeStamp currentTimeStamp = SST->getTimeStamp();
        if (currentTimeStamp > maxTimeStamp)
            maxTimeStamp = currentTimeStamp;
    }
    return maxTimeStamp;
}

TimeStamp KVStore::getMaxTimeStamp(const SSTPtr& oneSST, const vector<SSTPtr> &SSTs) {
    TimeStamp maxTimeStamp = oneSST->getTimeStamp();
    for (const auto& SST : SSTs) {
        TimeStamp currentTimeStamp = SST->getTimeStamp();
        if (currentTimeStamp > maxTimeStamp)
            maxTimeStamp = currentTimeStamp;
    }
    return maxTimeStamp;
}

/**
 * @param SSTs: SSTables to retrieve key-value pairs.
 * @return Pairs of key and its latest value stored in an unordered map.
 */
KVPair KVStore::getCompactionData(const vector<SSTPtr>& SSTs) {

    // Sort the SSTs according to their time tokens so that the value of the same key
    // in a newer SST will always overwrite the previous one.

    auto sortByTimeStamp = [](const SSTPtr& sstA, const SSTPtr& sstB) {
        return sstA->getTimeStamp() < sstB->getTimeStamp();
    };

    vector<SSTPtr> tempSSTs(SSTs);
    sort(tempSSTs.begin(), tempSSTs.end(), sortByTimeStamp);

    KVPair sstData;
    for (const auto& sst : tempSSTs)
        sst->getValuesFromDisk(sstData);

    return sstData;
}


KVPair KVStore::getCompactionData(const SSTPtr& sst, const vector<SSTPtr>& SSTs) {

    vector<SSTPtr> allSSTs(SSTs);
    allSSTs.push_back(sst);
    return getCompactionData(allSSTs);
}

/**
 * Write the keys and their values into the disk in the form of SST.
 * @param keys: All the sorted keys to generate the new SST.
 * @param data: Key-value pairs.
 * @return The generated SST.
 */
SSTPtr KVStore::generateNewSST(const vector<LsmKey> &keys, const KVPair& data,
                               size_t level, TimeStamp maxTimeStamp) {

    // Create the directory.
    string pathname = "./data/level-" + to_string(level) + "/";
    utils::mkdir(pathname.c_str());

    // Open the output file.
    string filename = pathname + "table-" + to_string(maxTimeStamp)
                      + "-" + to_string(keys.front())
                      + "-" + to_string(keys.back())
                      + ".sst";
    ofstream out(filename, ios::out | ios::binary);
    if (!out.is_open()) {
        cerr << "Open file failed." << endl;
        exit(-1);
    }

    // Initialize.
    size_t keyNumber = keys.size();
    SSTHeader sstHeader = SSTHeader(maxTimeStamp, keyNumber, keys.front(), keys.back());
    BloomFilter bloomFilter;
    vector<DataIndex> dataIndexes = vector<DataIndex>();
    uint32_t dataIndexStart = HEADER_SIZE + BLOOM_FILTER_SIZE;
    uint32_t dataStart = HEADER_SIZE + BLOOM_FILTER_SIZE + DATA_INDEX_SIZE * keyNumber;

    // Write header into the file.
    out.write((char*)&sstHeader, HEADER_SIZE);

    // Set the file position to tha start of data index.
    out.seekp(dataIndexStart, ios::beg);

    // Write data indexes into the file.
    uint32_t offset = dataStart;
    for (const auto& key : keys) {
        out.write((char*)&key, sizeof(key));
        out.write((char*)&offset, sizeof(offset));

        bloomFilter.insert(key);
        DataIndex dataIndex = DataIndex(key, offset);
        dataIndexes.push_back(dataIndex);

        offset += data.at(key).size();
    }

    // Write data into the file.
    for (const auto& key: keys) {
        LsmValue value = data.at(key);
        out.write((char*)&value[0], value.size());
    }

    // Write bloom filter into the file.
    out.seekp(HEADER_SIZE, ios::beg);
    out.write((char*)(bloomFilter.byteArray), BLOOM_FILTER_SIZE);

    // Close the file.
    out.close();

    // Return an SST.
    SSTPtr sst = make_shared<SSTable>(level, sstHeader, bloomFilter, dataIndexes);
    return sst;

}

void KVStore::removeSSTFromDisk(const SSTPtr& delSST) {
    string filename = delSST->getFilename();
    if (utils::rmfile(filename.c_str()) < 0) {
        cerr << "Fail to remove file `" << filename << "`." << endl;
        exit(-1);
    }
}
