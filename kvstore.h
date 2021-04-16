#pragma once

#include "kvstore_api.h"
#include "MemTable.h"
#include "constants.h"

class KVStore : public KVStoreAPI {

private:
    MemTable* memTable;

public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

};
