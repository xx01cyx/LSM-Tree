#include "kvstore.h"

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    memTable = make_shared<MemTable>();
    memTableSize = HEADER_SIZE + BLOOM_FILTER_SIZE;
    ssTables = unordered_map<size_t, shared_ptr<vector<SSTPtr>>>();
    ssTables[0] = make_shared<vector<SSTPtr>>();
    timeStamp = 1;
}

KVStore::~KVStore() {}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if (memTableOverflow(s)) {
        memToDisk();
        timeStamp++;

        // Update states
        memTable->reset();
        memTableSize = HEADER_SIZE + BLOOM_FILTER_SIZE;

        // Deal with level overflow
        if (levelOverflow(0))
            compact0();
//        size_t levelNumber = ssTables.size();
//        for (size_t level = 1; level < levelNumber; ++level) {
//            uint32_t overflowNumber = levelOverflow(level);
//            if (!overflowNumber)
//                break;
//            compact(level, overflowNumber);
//        }
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
    if (find)
        memTable->put(key, DELETE_SIGN);
    return find;
}

/**
 * Reset the LSM Tree. All key-value pairs should be removed, including
 * memtable and all SST files.
 */
void KVStore::reset()
{
    memTable->reset();
    char* pathname = "./data/";
    utils::rmdir(pathname);
    memTableSize = HEADER_SIZE + BLOOM_FILTER_SIZE;
    ssTables.clear();
    ssTables[0] = make_shared<vector<SSTPtr>>();
    timeStamp = 1;
}

bool KVStore::memTableOverflow(LsmValue v) {
    return memTableSize + DATA_INDEX_SIZE + v.size() > MAX_SSTABLE_SIZE;
}

/**
 * Write the data in memTable to the disk on overflowing.
 * Truncate memTable.
 */
void KVStore::memToDisk() {
    SSTPtr sst = memTable->writeToDisk(timeStamp);   // Write the data into disk (level 0)
    ssTables[0]->push_back(sst);    // Append to level 0 cache
}


LsmValue KVStore::getValueFromDisk(LsmKey key) {

    size_t levelNumber = ssTables.size();
    if (levelNumber == 0)   // All data are stored in memTable.
        return "";

    TimeStamp maxTimeStamp = 0;
    LsmValue newestValue = "";

    // Read from L0.
    vector<SSTPtr> L0SSTs = *ssTables[0];
    uint32_t L0SSTNumber = L0SSTs.size();
    for (size_t i = 0; i < L0SSTNumber; ++i) {
        SSTPtr sst = L0SSTs[i];
        getNewestValue(sst, key, maxTimeStamp, newestValue);
    }

    // Read from the rest levels.
    for (size_t n = 1; n < levelNumber; n++) {
        vector<SSTPtr> levelSSTs = *ssTables[n];
        if (key < levelSSTs.front()->getMinKey() || key > levelSSTs.back()->getMaxKey())
            continue;
        uint32_t sstIndex = sstBinarySearch(levelSSTs, key, 0, levelSSTs.size() - 1);
        SSTPtr targetSST = levelSSTs[sstIndex];
        getNewestValue(targetSST, key, maxTimeStamp, newestValue);
    }

    return newestValue;

}

void KVStore::getNewestValue(const SSTPtr sst, LsmKey key,
                             TimeStamp& maxTimeStamp, LsmValue& newestValue) {
    LsmValue newValue = sst->get(key);
    TimeStamp newTimeStamp = sst->getTimeStamp();
    if (newValue.length() != 0 && newTimeStamp > maxTimeStamp) {
        newestValue = newValue;
        maxTimeStamp = newTimeStamp;
    }
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

    // Get the overall interval of SSTs in L0.
    LsmKey minKey, maxKey;
    getCompact0Range(minKey, maxKey);

    // Get all the SSTs in L0 and L1 that need merge.
    int64_t minOverlapIndex = -1;
    int64_t maxOverlapIndex = -1;
    vector<SSTPtr> overlapSSTs = getOverlapSSTs(minKey, maxKey, 1,
                                                minOverlapIndex, maxOverlapIndex);
    SSTs.insert(SSTs.end(), overlapSSTs.begin(), overlapSSTs.end());

    // Sort the keys and write the data into the disk.
    vector<SSTPtr> mergedSSTs = merge0AndWriteToDisk(SSTs);

    // Reconstruct L1 in memory and delete corresponding files in the disk.
    reconstructLowerLevel(minOverlapIndex, maxOverlapIndex, mergedSSTs, 1);

    // Clear L0 in memory and delete corresponding files in the disk.
    clearL0();

}

void KVStore::getCompact0Range(LsmKey& minKey, LsmKey& maxKey) {
    vector<SSTPtr> L0SSTs = *ssTables[0];
    minKey = min(min(L0SSTs[0]->getMinKey(), L0SSTs[1]->getMinKey()),
                 L0SSTs[2]->getMinKey());
    maxKey = max(max(L0SSTs[0]->getMaxKey(), L0SSTs[1]->getMaxKey()),
                 L0SSTs[2]->getMaxKey());
}

/**
 * Compact SSTs from an upper level to a lower level one by one.
 * @param level: The upper level that overflows.
 * @param compactNumber: Number of SSTs in the upper level to be compacted.
 */
void KVStore::compact(size_t upperLevel, uint32_t compactNumber) {

    // Find the SSTs possessing the smallest time tokens.
    // return vector<SSTPtr>
    // sort

    // Compact the SSTs one by one.
    // for ...
    // compactOneSST(SSTPtr, level, ...)
    // [Including reconstructing lower level]

    // Reconstruct the upper level
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

    if (ssTables.count(level) == 0)
        return overlapSSTs;

    vector<SSTPtr> SSTs = *ssTables[level];
    uint32_t length = SSTs.size();

    bool overlap = false;

    // Get overlaps SSTs.
    for (uint32_t index = 0; index < length; ++index) {
        SSTPtr sst = SSTs[index];
        if (!overlap && sst->getMaxKey() >= minKey && sst->getMinKey() <= minKey) {
            minOverlapIndex = index;
            overlap = true;
        }
        if (sst->getMinKey() > maxKey) {
            maxOverlapIndex = index;
            break;
        }
        if (overlap)
            overlapSSTs.push_back(sst);
    }

    return overlapSSTs;
}

void KVStore::compactWithoutMerging(const SSTPtr upperLevelSST, size_t lowerLevel) {
    vector<SSTPtr> lowerLevelSSTs = *ssTables[lowerLevel];
    uint32_t insertIndex = sstBinarySearch(lowerLevelSSTs, upperLevelSST->getMinKey(), 0, lowerLevelSSTs.size());

}


void KVStore::reconstructLowerLevel(uint32_t minOverlapIndex, uint32_t maxOverlapIndex,
                                    const vector<SSTPtr>& newSSTs, size_t lowerLevel) {

    if (!ssTables.count(lowerLevel)) {      // new level
        ssTables[lowerLevel] = make_shared<vector<SSTPtr>>();
        minOverlapIndex = maxOverlapIndex = 0;
    }

    vector<SSTPtr> previousSSTs = *ssTables[lowerLevel];
    uint32_t length = previousSSTs.size();
    vector<SSTPtr> updatedSSTs;

    // If no overlapping, binary search for the insert position.
    if (minOverlapIndex == -1 && maxOverlapIndex == -1) {
        LsmKey insertMinKey = newSSTs.front()->getMinKey();
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

    ssTables[lowerLevel] = make_shared<vector<SSTPtr>>(updatedSSTs);

    // Delete the SST files in disk.
    for (uint32_t i = minOverlapIndex; i < maxOverlapIndex; ++i) {
        SSTPtr delSST = previousSSTs[i];
        string filename = delSST->getFilename();
        if (utils::rmfile((char*)&filename[0]) < 0) {
            cerr << "Fail to remove file `" << filename << "`." << endl;
            exit(-1);
        }
    }
}


/**
 * @param SSTs: SSTables to retrieve key-value pairs.
 * @return Pairs of key and its latest value stored in an unordered map.
 */
KVPair KVStore::readDataFromDisk(const vector<SSTPtr>& SSTs) {

    // Sort the SSTs according to their time tokens so that the value of the same key
    // in a newer SST will always overwrite the previous one.

    auto sortByTimeStamp = [](const SSTPtr sstA, const SSTPtr sstB) {
        return sstA->getTimeStamp() < sstB->getTimeStamp();
    };

    vector<SSTPtr> tempSSTs(SSTs);
    sort(tempSSTs.begin(), tempSSTs.end(), sortByTimeStamp);

    KVPair sstData;
    for (auto sst : tempSSTs)
        sst->getValuesFromDisk(sstData);

    return sstData;
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

/**
 * Merge sort the SSTs need compact in L0 and L1 using priority queue.
 * Write the new SSTs into the disk.
 * @param SSTs: SSTs need compact in L0 and L1.
 * @return New SSTs generated during compaction.
 */
vector<SSTPtr> KVStore::merge0AndWriteToDisk(const vector<SSTPtr> &SSTs) {

    KVPair data = readDataFromDisk(SSTs);
    TimeStamp maxTimeStamp = getMaxTimeStamp(SSTs);

    vector<SSTPtr> newSSTs;
    priority_queue<KeyRef, vector<KeyRef>, KeyRefComparator> pq;
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

        if (sortedKeys.empty())
            sortedKeys.push_back(currentKey);

        conditionalPushAndWrite(currentKey, sortedKeys, currentSize, data, newSSTs, 1);

        pq.pop();
        if (currentIndex != currentDataIndexes.size() - 1)
            pq.push(make_pair(currentSST, currentIndex + 1));
    }

    // Pack the remaining data into an SST.
    if (!sortedKeys.empty()) {
        newSSTs.push_back(generateNewSST(sortedKeys, data, 1));
        timeStamp++;
    }

    return newSSTs;
}

inline bool operator< (const KeyRef& ref1, const KeyRef& ref2) {
    LsmKey key1 = (ref1.first)->getDataIndexes()[ref1.second].key;
    LsmKey key2 = (ref2.first)->getDataIndexes()[ref2.second].key;
    if (key1 == key2)
        return (ref1.first)->getTimeStamp() < (ref2.first)->getTimeStamp();
    return key1 < key2;
}



void KVStore::conditionalPushAndWrite(LsmKey key, vector<LsmKey> &sortedKeys, size_t &currentSize,const KVPair& data,
                                      vector<SSTPtr> &newSSTs, size_t lowerLevel) {

    if (!sortedKeys.empty() && sortedKeys.back() != key) {      // conditionally push key
        size_t sizeIncrement = DATA_INDEX_SIZE + data.at(key).size();

        if (currentSize + sizeIncrement > MAX_SSTABLE_SIZE) {   // conditionally write data
            // Generate a new SST.
            newSSTs.push_back(generateNewSST(sortedKeys, data, lowerLevel));

            // Update states.
            sortedKeys.clear();
            currentSize = HEADER_SIZE + BLOOM_FILTER_SIZE;
            timeStamp++;
        }

        sortedKeys.push_back(key);
        currentSize += sizeIncrement;
    }
}

/**
 * Write the keys and their values into the disk in the form of SST.
 * @param keys: All the sorted keys to generate the new SST.
 * @param data: Key-value pairs.
 * @return The generated SST.
 */
SSTPtr KVStore::generateNewSST(const vector<LsmKey> &keys, const KVPair& data, size_t level) {

    // Create the directory.
    string pathname = "./data/level-" + to_string(level) + "/";
    utils::mkdir(pathname.c_str());

    // Open the output file.
    string filename = pathname + "table-" + to_string(timeStamp)
                      + "-" + to_string(keys.front()) + ".sst";
    ofstream out(filename, ios::out | ios::binary);
    if (!out.is_open()) {
        cerr << "Open file failed." << endl;
        exit(-1);
    }

    // Initialize.
    size_t keyNumber = keys.size();
    SSTHeader sstHeader = SSTHeader(timeStamp, keyNumber, keys.front(), keys.back());
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
    out.write((char*)(bloomFilter.bitArray), BLOOM_FILTER_SIZE);

    // Close the file.
    out.close();

    // Return an SST.
    SSTPtr sst = make_shared<SSTable>(level, sstHeader, bloomFilter, dataIndexes);
    return sst;

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
        if (SSTs[right]->getMinKey() > key)
            right--;
        return right;
    }

    uint32_t mid = left + (right - left) / 2;
    LsmKey midMinKey = SSTs[mid]->getMinKey();

    if (key < midMinKey)
        return sstBinarySearch(SSTs, key, left, mid - 1);
    if (key > midMinKey)
        return sstBinarySearch(SSTs, key, mid + 1, right);
    return mid;

}

/**
 * Clear L0 in memory.
 * Delete all the L0 files in the disk.
 */
void KVStore::clearL0() {
    vector<SSTPtr> L0SST = *ssTables[0];
    for (const auto& sst : L0SST) {
        string filename = sst->getFilename();
        if (utils::rmfile((char*)&filename[0]) < 0) {
            cerr << "Fail to remove file `" << filename << "`." << endl;
            exit(-1);
        }
        ssTables[0]->clear();
    }
}