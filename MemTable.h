#ifndef LSM_TREE_MEMTABLE_H
#define LSM_TREE_MEMTABLE_H

#include <iostream>
#include <string>
#include <stack>
#include <cstdlib>
#include <memory>
#include "constants.h"
#include "SSTable.h"

using namespace std;

class MemTable {

    struct Node {

        LsmKey key;
        LsmValue value;
        Node* next;
        Node* down;

        Node() : next(nullptr), down(nullptr) {}
        Node(LsmKey key, LsmValue value) : key(key), value(value), next(nullptr), down(nullptr) {}
        Node(Node* down) : next(nullptr), down(down) {}
        Node(Node* next, Node* down) : next(next), down(down) {}
        Node(LsmKey key, LsmValue value, Node* next) : key(key), value(value), next(next), down(nullptr) {}
        Node(LsmKey key, LsmValue value, Node* next, Node* down) : key(key), value(value), next(next), down(down) {}

    };

private:
    Node* head;
    uint64_t keyNumber;
    int level0Number;   // for infinite level 0

    Node* getLowestHead() const;

public:
    MemTable();
    ~MemTable();
    void put(LsmKey k, LsmValue v);
    LsmValue get(LsmKey k);
    bool del(LsmKey k);
    void reset();
    SSTPtr writeToDisk(uint64_t timeToken);
};


#endif //LSM_TREE_MEMTABLE_H
