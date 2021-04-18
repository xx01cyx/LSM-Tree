
LINK.o = $(LINK.cc)
CXXFLAGS = -std=c++14 -Wall

all: correctness persistence

correctness: BloomFilter.o SSTable.o MemTable.o kvstore.o correctness.o
persistence: BloomFilter.o SSTable.o MemTable.o kvstore.o persistence.o

clean:
	-rm -f correctness persistence *.o
