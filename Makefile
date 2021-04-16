
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence

correctness: kvstore.o correctness.o MemTable.o BloomFilter.o

persistence: kvstore.o persistence.o MemTable.o BloomFilter.o

clean:
	-rm -f correctness persistence *.o
