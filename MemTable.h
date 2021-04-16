#ifndef LSM_TREE_MEMTABLE_H
#define LSM_TREE_MEMTABLE_H

#include "constants.h"

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

public:
    MemTable();
    ~MemTable();
    void put(LsmKey k, LsmValue v);
    LsmValue get(LsmKey k);
    bool del(LsmKey k);
    void reset();
};


#endif //LSM_TREE_MEMTABLE_H
