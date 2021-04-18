#include "SSTable.h"

SSTable::SSTable() {
    dataIndexes = vector<DataIndexPtr>();
}

SSTable::SSTable(SSTHeader header,
                 BloomFilter bloomFilter,
                 vector<DataIndexPtr> dataIndexes) {
    this->header = header;
    this->bloomFilter = bloomFilter;
    this->dataIndexes = dataIndexes;
}

LsmValue SSTable::get(LsmKey k, string filename) const {
    if (!bloomFilter.hasKey(k))
        return "";
    int64_t index = find(k, dataIndexes, 0, header.keyNumber - 1);
    if (index < 0)
        return "";
    return getFromDisk(index, filename);
}

/**
 * Do not exactly return the target data index.
 * If the key cannot be found, `find` returns whatever is at the end of the recursion.
 */
int64_t SSTable::find(LsmKey k, vector<DataIndexPtr> arr, int64_t start, int64_t end) const {

    if (start >= end && arr[start]->key != k)
        return -1;

    uint64_t mid = (start + end) / 2;
    if (k < arr[mid]->key)
        return find(k, arr, start, mid-1);
    if (k > arr[mid]->key)
        return find(k, arr, mid+1, end);

    return mid;  // found

}

LsmValue SSTable::getFromDisk(int64_t index, string filename) const {

    // Open the SST file.
    ifstream table(filename, ios::in | ios::binary);
    if (!table) {
        cerr << "Cannot open file `" << filename << '`.' << endl;
        exit(-1);
    }

    // Find the start and end of the value.
    uint32_t start = dataIndexes[index]->offset;
    uint32_t end;
    if (index != dataIndexes.size() - 1)
        end = dataIndexes[index + 1]->offset;
    else {      // If `key` is the last key, set `end` as the length of the file.
        table.seekg(0, ios::end);
        end = table.tellg();
    }

    // Read value from the file.
    string value;
    value.resize(end - start);
    table.seekg(start, ios::beg);
    table.read((char*)&value[0], end - start);

    // Close the file.
    table.close();

    return value;
}

uint64_t SSTable::getTimeToken() const {
    return header.timeToken;
}
