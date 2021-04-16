#include "BloomFilter.h"
#include <cstring>

BloomFilter::BloomFilter() {
    bitArray = new bool[BLOOM_FILTER_BYTE_LENGTH];
    bitArray = (bool*)memset(bitArray, 0, BLOOM_FILTER_BYTE_LENGTH);
}

BloomFilter::~BloomFilter() {
    delete bitArray;
}

void BloomFilter::insert(LsmKey k) {
    uint32_t* hashValues = new uint32_t[4];
    hashValues = (uint32_t*)memset(hashValues, 0, 4 * sizeof(uint32_t));
    MurmurHash3_x64_128(&k, sizeof(k), 1, hashValues);
    for (int i = 0; i < 4; ++i) {
        int index = *(hashValues + i) % BLOOM_FILTER_BYTE_LENGTH;
        bitArray[index] = 1;
    }
}

bool BloomFilter::hasKey(LsmKey k) {
    uint32_t* hashValues = new uint32_t[4];
    hashValues = (uint32_t*)memset(hashValues, 0, 4 * sizeof(uint32_t));
    MurmurHash3_x64_128(&k, sizeof(k), 1, hashValues);
    return bitArray[*hashValues % BLOOM_FILTER_BYTE_LENGTH]
           & bitArray[*(hashValues + 1) % BLOOM_FILTER_BYTE_LENGTH]
           & bitArray[*(hashValues + 2) % BLOOM_FILTER_BYTE_LENGTH]
           & bitArray[*(hashValues + 3) % BLOOM_FILTER_BYTE_LENGTH];
}