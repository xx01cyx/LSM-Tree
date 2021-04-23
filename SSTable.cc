#include "SSTable.h"


SSTable::SSTable(size_t level, SSTHeader header, BloomFilter bloomFilter, vector<DataIndex> dataIndexes)
        : level(level), header(header), bloomFilter(bloomFilter), dataIndexes(dataIndexes) {}

LsmValue SSTable::get(LsmKey k) const {
    if (!bloomFilter.hasKey(k))
        return "";
    int64_t index = find(k, dataIndexes, 0, header.keyNumber - 1);
    if (index < 0)
        return "";
    return getValueFromDisk(index);
}

/**
 * Do not exactly return the target data index.
 * If the key cannot be found, `find` returns whatever is at the end of the recursion.
 */
int64_t SSTable::find(LsmKey k, vector<DataIndex> arr, int64_t start, int64_t end) const {

    if (start >= end && arr[start].key != k)
        return -1;

    uint64_t mid = start + (end - start) / 2;
    if (k < arr[mid].key)
        return find(k, arr, start, mid-1);
    if (k > arr[mid].key)
        return find(k, arr, mid+1, end);

    return mid;  // found

}

LsmValue SSTable::getValueFromDisk(size_t index) const {

    string filename = getFilename();

    // Open the SST file.
    ifstream table(filename, ios::in | ios::binary);
    if (!table) {
        cerr << "Cannot open file `" << filename << "`." << endl;
        exit(-1);
    }

    // Find the start and end of the value.
    uint32_t start = dataIndexes[index].offset;
    uint32_t end;
    if (index != dataIndexes.size() - 1)
        end = dataIndexes[index + 1].offset;
    else {      // If `key` is the last key, set `end` as the length of the file.
        table.seekg(0, ios::end);
        end = table.tellg();
    }

    // Read value from the file.
    LsmValue value = readValueFromFile(table, start, end, false);

    // Close the file.
    table.close();

    return value;
}

/**
 * Read all the key-value pairs of the SST from the disk without frequently altering
 * file position. The result is stored in sstData.
 */
void SSTable::getValuesFromDisk(KVPair& sstData) const {

    string filename = getFilename();
    ifstream table(filename, ios::in | ios::binary);
    if (!table) {
        cerr << "Cannot open file `" << filename << "`." << endl;
        exit(-1);
    }

    table.seekg(0, ios::end);
    uint32_t fileLength = table.tellg();

    uint32_t dataStart = HEADER_SIZE + BLOOM_FILTER_SIZE + DATA_INDEX_SIZE * header.keyNumber;
    table.seekg(dataStart, ios::beg);

    uint32_t start = dataStart;
    uint32_t end;

    auto it = dataIndexes.cbegin();
    LsmKey key = (*it).key;
    it++;
    while (it != dataIndexes.cend()) {
        end = (*it).offset;
        LsmValue value = readValueFromFile(table, start, end, true);
        sstData[key] = value;
        start = end;
        key = (*it).key;    // Get key for the next loop.
        it++;
    }

    // Set the last k-v pair.
    end = fileLength;
    LsmValue value = readValueFromFile(table, start, end, true);
    sstData[key] = value;
}

/**
 * Read a singleton value from an open SST file based on its offset.
 * @param file: An open SST file.
 * @param startOffset: The start offset of the value in the file.
 * @param endOffset: The end offset of the value in the file.
 * @param multiValue: Set true if reading a contiguous sequence of values. Set false otherwise.
 * Note that the start position must have been specified before if the param is set true.
 * @return The value read from the file.
 */
LsmValue SSTable::readValueFromFile(ifstream& table, uint32_t startOffset, uint32_t endOffset, bool multiValue) {
    LsmValue value;
    uint32_t length = endOffset - startOffset;
    value.resize(length);
    if (!multiValue)
        table.seekg(startOffset, ios::beg);
    table.read((char*)&value[0], length);
    return value;
}


string SSTable::getFilename() const {
    return "./data/level-" + to_string(level)
           + "/table-" + to_string(header.timeStamp)
           + "-" + to_string(header.minKey)
           + "-" + to_string(header.maxKey)
           + ".sst";
}

size_t SSTable::getLevel() const {
    return level;
}

TimeStamp SSTable::getTimeStamp() const {
    return header.timeStamp;
}

LsmKey SSTable::getMinKey() const {
    return header.minKey;
}

LsmKey SSTable::getMaxKey() const {
    return header.maxKey;
}

size_t SSTable::getKeyNumber() const {
    return header.keyNumber;
}

vector<DataIndex> SSTable::getDataIndexes() const {
    return dataIndexes;
}

vector<LsmKey> SSTable::getKeys() const {
    vector<LsmKey> keys;
    for (const auto& dataIndex : dataIndexes) {
        keys.push_back(dataIndex.key);
    }
    return keys;
}
