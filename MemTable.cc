#include "MemTable.h"
#include <string>
#include <stack>
#include <cstdlib>

MemTable::MemTable() {
    head = new Node();
}

MemTable::~MemTable() {
    reset();
    delete head;
}


void MemTable::put(LsmKey k, LsmValue v) {

    stack<Node*> path = stack<Node*>();
    Node* p = head;


    while (p) {
        while (p->next && p->next->key < k)
            p = p->next;
        if (p->next && k == p->next->key) {    // substitute
            p = p->next;        // p points to the topmost existing node
            while (p) {
                p->value = v;
                p = p->down;
            }
            return;
        }
        path.push(p);
        p = p->down;
    }

    Node* prevAddedNode = nullptr;
    do {                                // 50% add a new layer
        Node* prev;
        if (!path.empty()) {
            prev = path.top();
            path.pop();
        } else {
            prev = head = new Node(head);
        }
        Node* newNode = new Node(k, v, prev->next, prevAddedNode);
        prev->next = newNode;
        prevAddedNode = newNode;
    } while (rand() & 1);

}

LsmValue MemTable::get(LsmKey k) {

    Node* p = head;

    while (p) {
        while (p->next && p->next->key < k)
            p = p->next;
        if (p->next && k == p->next->key) {
            LsmValue value = p->next->value;
            if (value == DELETE_SIGN)
                return "";
            return p->next->value;
        }
        p = p->down;
    }

    return "";
}

bool MemTable::del(LsmKey k) {

    Node* p = head;
    bool find = false;

    while (p) {
        while (p->next && p->next->key < k)
            p = p->next;
        if (p->next && k == p->next->key) {
            if (p->next->value == DELETE_SIGN)
                return false;
            find = true;
            Node *delNode = p->next;
            p->next = delNode->next;
            delete delNode;
        }
        p = p->down;
    }

    while (head && !head->next) {   // delete empty layer
        Node* delHead = head;
        head = head->down;
        delete delHead;
    }

    return find;

}

void MemTable::reset() {
    while (head) {
        while (head->next) {
            Node* delNode = head->next;
            head->next = delNode->next;
            delete delNode;
        }
        Node* delHead = head;
        head = head->down;
        delete delHead;
    }
    head = new Node();
}