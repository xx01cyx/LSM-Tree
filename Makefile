
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence cache

correctness: BloomFilter.o SSTable.o MemTable.o kvstore.o correctness.o
persistence: BloomFilter.o SSTable.o MemTable.o kvstore.o persistence.o
cache: BloomFilter.o SSTable.o MemTable.o kvstore.o cache.o

clean:
	-rm -f correctness persistence *.o
