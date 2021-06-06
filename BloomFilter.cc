#include "BloomFilter.h"

BloomFilter::BloomFilter() {
    byteArray = new bool[BLOOM_FILTER_SIZE];
    byteArray = (bool*)memset(byteArray, 0, BLOOM_FILTER_SIZE);
}

BloomFilter::BloomFilter(bool* bits) {
    byteArray = new bool[BLOOM_FILTER_SIZE];
    memcpy(byteArray, bits, BLOOM_FILTER_SIZE);
}

BloomFilter::~BloomFilter() = default;

void BloomFilter::insert(LsmKey k) {
    uint32_t* hashValues = new uint32_t[4];
    hashValues = (uint32_t*)memset(hashValues, 0, 4 * sizeof(uint32_t));
    MurmurHash3_x64_128(&k, sizeof(k), 1, hashValues);
    for (int i = 0; i < 4; ++i) {
        int index = *(hashValues + i) % BLOOM_FILTER_SIZE;
        byteArray[index] = 1;
    }
}

bool BloomFilter::hasKey(LsmKey k) const {
    uint32_t* hashValues = new uint32_t[4];
    hashValues = (uint32_t*)memset(hashValues, 0, 4 * sizeof(uint32_t));
    MurmurHash3_x64_128(&k, sizeof(k), 1, hashValues);
    return byteArray[*hashValues % BLOOM_FILTER_SIZE]
           & byteArray[*(hashValues + 1) % BLOOM_FILTER_SIZE]
           & byteArray[*(hashValues + 2) % BLOOM_FILTER_SIZE]
           & byteArray[*(hashValues + 3) % BLOOM_FILTER_SIZE];
}