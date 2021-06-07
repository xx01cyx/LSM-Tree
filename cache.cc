#include <iostream>
#include <vector>
#include <utility>
#include <string>
#include <ctime>
#include <random>
#include "kvstore.h"

using namespace std;

const int OP_TIMES = 10000;
const int TEST_ITERS = 5;
const int VALUE_SIZES_LEN = 4;
const size_t valueSizes[VALUE_SIZES_LEN] = {50, 500, 5000, 50000};

float computeTimeDiff(clock_t startTime, clock_t endTime);

int main() {

    KVStore kvStore("./data");
    vector<float> getRecord = vector<float>(5);

    // Test for multiple times to get a more accurate result.
    for (int i = 0; i < TEST_ITERS; ++i) {

        cout << "Iteration " << i + 1 << ":" << endl;

        for (int j = 0; j < VALUE_SIZES_LEN; ++j) {

            // Construct test data based on different sizes of values.
            int valueSize = valueSizes[j];
            string value = string(valueSize, 's');

            vector<LsmKey> testKeys;
            for (LsmKey key = 0; key < OP_TIMES; ++key)
                testKeys.push_back(key);

            // Clear previous data.
            kvStore.reset();

            // Do the test.
            shuffle(testKeys.begin(), testKeys.end(), std::mt19937(std::random_device()()));

            for (const auto& key : testKeys)
                kvStore.put(key, value);

            shuffle(testKeys.begin(), testKeys.end(), std::mt19937(std::random_device()()));

            clock_t getStart = clock();
            for (const auto& key : testKeys)
                kvStore.get(key);
            clock_t getEnd = clock();

            // Calculate the delay.
            float getDelay = computeTimeDiff(getStart, getEnd);

            getRecord[j] += getDelay;

            cout << "GET delay for size " << valueSize << " is " << getDelay << endl;

        }

    }

    auto calcAvgDelay = [] (float& x) {
        x /= (OP_TIMES * TEST_ITERS);
    };

    for_each(getRecord.begin(), getRecord.end(), calcAvgDelay);

    for (int j = 0; j < VALUE_SIZES_LEN; ++j) {
        cout << "Value size: " << valueSizes[j] << endl;
        cout << "Average delay for GET: " << getRecord[j] << endl;
    }

    return 0;
}


float computeTimeDiff(clock_t startTime, clock_t endTime) {
    return float(endTime - startTime) / CLOCKS_PER_SEC;
}
