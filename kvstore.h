#pragma once

#include <memory>
#include <string>
#include "kvstore_api.h"
#include "MemTable.h"
#include "SSTable.h"
#include "constants.h"

class KVStore : public KVStoreAPI {

private:
    shared_ptr<MemTable> memTable;
    uint32_t memTableSize;
    vector<shared_ptr<vector<SSTPtr>>> ssTables;
    uint64_t sstNumber;

    bool memTableOverflow(LsmValue v);
    void memToDisk();
    LsmValue readFromDisk(LsmKey key);

public:
    KVStore(const std::string &dir);
    ~KVStore();

    void put(uint64_t key, const std::string &s) override;
    std::string get(uint64_t key) override;
    bool del(uint64_t key) override;
    void reset() override;

};
