#include "include/Memory.h"
#include <vector>
#include <iostream>

using std::vector;

int cacheLineCount(int start, int size, int count, int cacheLineSize) {
    return (start + size * count - 1) / cacheLineSize - start / cacheLineSize + 1;
}

void test(int cacheLineSize) {
    Memory mem(cacheLineSize);

    int touched = 0;
    assert(mem.touchedCacheLines() == touched);

    // Read 1 int
    int x;
    mem.read(400, &x, 4);
    touched = cacheLineCount(400, 4, 1, cacheLineSize);
    assert(mem.touchedCacheLines() == touched);

    // Read 1 char overlapping with the previous int
    char c;
    mem.read(401, &c, 1);
    assert(mem.touchedCacheLines() == touched);

    // Read 1000 ints
    int i = 1003;
    for (int j = 0; j < 1000; ++j) {
        mem.read(i, &x, 4);
        i += 4;
    }
    touched += cacheLineCount(1003, 4, 1000, cacheLineSize);
    assert(mem.touchedCacheLines() == touched);

    // Read 1024 ints
    i = 6010;
    for (int j = 0; j < 1024; ++j) {
        mem.read(i, &x, 4);
        i += 4;
    }
    touched += cacheLineCount(6010, 4, 1024, cacheLineSize);
    assert(mem.touchedCacheLines() == touched);

    // Read a big struct
    struct {
        double foo[123];
        int bar;
    } big;

    mem.read(12000, &big, sizeof(big));
    touched += cacheLineCount(12000, sizeof(big), 1, cacheLineSize);
    assert(mem.touchedCacheLines() == touched);
}

int main() {
    for (int cacheLineSize : vector<int>({1, 16, 32, 64, 128})) {
        test(cacheLineSize);
    }
    return 0;
}