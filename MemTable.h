#ifndef LSM_TREE_MEMTABLE_H
#define LSM_TREE_MEMTABLE_H

#include <iostream>
#include <string>
#include <stack>
#include <cstdlib>
#include <memory>
#include <utility>
#include "constants.h"
#include "SSTable.h"

using namespace std;

class MemTable {

    struct Node {

        LsmKey key{};
        LsmValue value;
        Node* next;
        Node* down;

        Node() : next(nullptr), down(nullptr) {}
        Node(LsmKey key, LsmValue value) : key(key), value(std::move(value)), next(nullptr), down(nullptr) {}
        Node(Node* down) : next(nullptr), down(down) {}
        Node(Node* next, Node* down) : next(next), down(down) {}
        Node(LsmKey key, LsmValue value, Node* next) : key(key), value(std::move(value)), next(next), down(nullptr) {}
        Node(LsmKey key, LsmValue value, Node* next, Node* down) : key(key), value(std::move(value)), next(next), down(down) {}

    };

private:
    Node* head;
    uint64_t keyNumber;

    Node* getLowestHead() const;

public:
    MemTable();
    ~MemTable();

    void put(LsmKey k, const LsmValue& v);
    LsmValue get(LsmKey k);
    bool del(LsmKey k);
    void reset();
    bool empty();
    SSTPtr writeToDisk(TimeStamp timeStamp);

};


#endif //LSM_TREE_MEMTABLE_H
