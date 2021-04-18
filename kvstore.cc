#include "kvstore.h"

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    memTable = make_shared<MemTable>();
    memTableSize = HEADER_SIZE + BLOOM_FILTER_SIZE;
    ssTables = vector<shared_ptr<vector<SSTPtr>>>();
    ssTables.push_back(make_shared<vector<SSTPtr>>());  // level 0
    sstNumber = 0;
}

KVStore::~KVStore() {}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if (memTableOverflow(s)) {
        sstNumber++;
        memToDisk();
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
    return readFromDisk(key);
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
    SSTPtr sst = memTable->writeToDisk(sstNumber);   // Write the data into disk (level 0)
    ssTables[0]->push_back(sst);    // Append to level 0 cache
    memTable->reset();
    memTableSize = HEADER_SIZE + BLOOM_FILTER_SIZE;
}

LsmValue KVStore::readFromDisk(LsmKey key) {
//    uint64_t level = 0;
//    std::string directory = "/data/level" + to_string(level);
//    while (utils::dirExists(directory)) {
//        level++;
//    }

    uint64_t levels = ssTables.size();
    if (levels == 0)   // All the data are stored in memTable till now,
        return "";

    uint64_t maxTimeToken = 0;
    LsmValue newestValue = "";

    // Read from level 0.
    shared_ptr<vector<SSTPtr>> level0Tables = ssTables[0];
    int level0Number = level0Tables->size();

    for (uint64_t i = 0; i < level0Number; ++i) {
        SSTPtr ssTable = level0Tables->at(i);
        std::string filename = "data/level-0/table" + to_string(i) + ".sst";
        LsmValue value = ssTable->get(key, filename);

        if (value != DELETE_SIGN && value.length() != 0) {
            uint64_t timeToken = ssTable->getTimeToken();
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