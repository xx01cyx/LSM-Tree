#include "MemTable.h"
#include "utils.h"

MemTable::MemTable() {
    head = new Node();
    keyNumber = 0;
}

MemTable::~MemTable() {
    reset();
    delete head;
}


void MemTable::put(LsmKey k, const LsmValue& v) {

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

    keyNumber++;

}

LsmValue MemTable::get(LsmKey k) {

    Node* p = head;

    while (p) {
        while (p->next && p->next->key < k)
            p = p->next;
        if (p->next && k == p->next->key)
            return p->next->value;
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
    keyNumber = 0;
}

bool MemTable::empty() {
    return head->next == nullptr;
}

/**
 * If overflow, write the data in memTable into level 0 in disk
 * in the order of data index, header, bloom filter and data.
 * @return an SSTable that stores the cached information.
 */
SSTPtr MemTable::writeToDisk(TimeStamp timeStamp) {

    // Create the directory.
    string pathname = "./data/level-0/";
    utils::mkdir(pathname.c_str());

    // Open the output file.
    string filename = pathname + "table-" + to_string(timeStamp) + ".sst";
    ofstream out(filename, ios::out | ios::binary);
    if (!out.is_open()) {
        cerr << "Open file failed." << endl;
        exit(-1);
    }

    // Initialize.
    SSTHeader sstHeader;
    BloomFilter bloomFilter;
    vector<DataIndex> dataIndexes = vector<DataIndex>();
    uint32_t dataIndexStart = HEADER_SIZE + BLOOM_FILTER_SIZE;
    uint32_t dataStart = HEADER_SIZE + BLOOM_FILTER_SIZE + DATA_INDEX_SIZE * keyNumber;

    // Set the file position to tha start of data index.
    out.seekp(dataIndexStart, ios::beg);

    // Write data indexes into the file.
    Node* q = getLowestHead();
    Node* p = q;
    uint32_t offset = dataStart;
    while (p->next) {
        LsmKey k = p->next->key;
        LsmValue v = p->next->value;

        out.write((char*)&k, sizeof(k));
        out.write((char*)&offset, sizeof(offset));

        bloomFilter.insert(k);
        DataIndex dataIndex = DataIndex(k, offset);
        dataIndexes.push_back(dataIndex);

        offset += v.size();
        p = p->next;
    }

    // After the loop, p now points to the max key.
    sstHeader = SSTHeader(timeStamp, keyNumber, q->next->key, p->key);

    // Set the file position to the beginning.
    out.seekp(0, ios::beg);

    // Write header and bloom filter into the file.
    out.write((char*)&sstHeader, HEADER_SIZE);
    out.write((char*)(bloomFilter.byteArray), BLOOM_FILTER_SIZE);

    // Set the file position to the start of data.
    out.seekp(dataStart, ios::beg);

    // Write data into the file.
    while (q->next) {
        LsmValue v = q->next->value;
        out.write((char*)&v[0], v.size());
        q = q->next;
    }

    // Close the file.
    out.close();

    // Create an SST in the memory.
    SSTPtr sst = make_shared<SSTable>(0, sstHeader, bloomFilter, dataIndexes);

    // Rename the file.
    string newFilename = sst->getFilename();
    rename(filename.c_str(), newFilename.c_str());

    return sst;

}

MemTable::Node* MemTable::getLowestHead() const {
    Node* p = head;
    while (p->down)
        p = p->down;
    return p;
}