#include <iostream>
#include <cstdint>
#include <random>
#include <string>
#include "test.h"

class CorrectnessTest : public Test {
private:
	const uint64_t SIMPLE_TEST_MAX = 512;
	const uint64_t LARGE_TEST_MAX = 1024 * 64;

	void regular_test(uint64_t max)
	{
		uint64_t i;

		// Test a single key
		EXPECT(not_found, store.get(1));
		store.put(1, "SE");
		EXPECT("SE", store.get(1));
		EXPECT(true, store.del(1));
		EXPECT(not_found, store.get(1));
		EXPECT(false, store.del(1));

		phase();

		vector<uint64_t> testKeys;
		for (i = 0; i < max; ++i)
            testKeys.push_back(i);

		// Test multiple key-value pairs

        std::shuffle(testKeys.begin(), testKeys.end(), std::mt19937(std::random_device()()));
        for (i = 0; i < max; ++i) {
		    uint64_t key = testKeys[i];
			store.put(key, std::string(key+1, 's'));
			EXPECT(std::string(key + 1, 's'), store.get(key));
		}
		phase();

		// Test after all insertions

        std::shuffle(testKeys.begin(), testKeys.end(), std::mt19937(std::random_device()()));
        for (i = 0; i < max; ++i) {
		    uint64_t key = testKeys[i];
            EXPECT(std::string(key + 1, 's'), store.get(key));
        }
		phase();

		// Test deletions

        vector<uint64_t> evenKeys;
        for (i = 0; i < max; i += 2)
            evenKeys.push_back(i);

        vector<uint64_t> oddKeys;
        for (i = 1; i < max; i += 2)
            oddKeys.push_back(i);

        std::shuffle(testKeys.begin(), testKeys.end(), std::mt19937(std::random_device()()));
        std::shuffle(evenKeys.begin(), evenKeys.end(), std::mt19937(std::random_device()()));
        std::shuffle(oddKeys.begin(), oddKeys.end(), std::mt19937(std::random_device()()));

        uint64_t evenNumber = evenKeys.size();
        uint64_t oddNumber = oddKeys.size();

        for (i = 0; i < evenNumber; ++i) {
            uint64_t key = evenKeys[i];
            EXPECT(true, store.del(key));
        }

		for (i = 0; i < max; ++i) {
            uint64_t key = testKeys[i];
            EXPECT((key & 1) ? std::string(key + 1, 's') : not_found,
                   store.get(key));
        }

		for (i = 1; i < oddNumber; ++i) {
            uint64_t key = oddKeys[i];
            EXPECT(key & 1, store.del(key));
        }

		phase();

		report();
	}

public:
	CorrectnessTest(const std::string &dir, bool v=true) : Test(dir, v)
	{
	}

	void start_test(void *args = NULL) override
	{
		std::cout << "KVStore Correctness Test" << std::endl;

		std::cout << "[Simple Test]" << std::endl;
		regular_test(SIMPLE_TEST_MAX);

		std::cout << "[Large Test]" << std::endl;
		regular_test(LARGE_TEST_MAX);
	}
};

int main(int argc, char *argv[])
{
	bool verbose = (argc == 2 && std::string(argv[1]) == "-v");

	std::cout << "Usage: " << argv[0] << " [-v]" << std::endl;
	std::cout << "  -v: print extra info for failed tests [currently ";
	std::cout << (verbose ? "ON" : "OFF")<< "]" << std::endl;
	std::cout << std::endl;
	std::cout.flush();

	CorrectnessTest test("./data", verbose);

	test.start_test();

	return 0;
}
