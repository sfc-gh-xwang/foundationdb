#pragma once

#include <iostream>
#include <unordered_set>
#include <vector>
#include <stdint.h>

using std::unordered_set;
using std::vector;

// Counts the number of cache lines ever touched
class Memory {
public:
	Memory(int cacheLineSize) : cacheLineSize(cacheLineSize) {}

	// Writes the i-th byte in the memory
	void write(int offset, char c) {
		expand(offset);
		touched.insert(offset / cacheLineSize);
		memory[offset] = c;
	}

	// Reads the i-th byte in the memory
	char read(int offset) {
		expand(offset);
		touched.insert(offset / cacheLineSize);
		return memory[offset];
	}

	void write(int offset, void* item, size_t size) {
		for (int i = 0; i < size; ++i) {
			write(offset + i, *(static_cast<char*>(item) + i));
		}
	}

	void read(int offset, void* output, size_t size) {
		char* ptr = static_cast<char*>(output);
		for (int i = 0; i < size; ++i) {
			ptr[i] = read(offset + i);
		}
	}

	void probe(int offset, size_t size) {
		for (int i = 0; i < size; ++i) {
			read(offset + i);
		}
	}

	// Returns the number of cache lines touched
	int touchedCacheLines() { return touched.size(); }

	void reset() { touched.clear(); }

private:
	void expand(int offset) {
		if (memory.size() <= offset) {
			memory.resize(offset + 1);
		}
	}

	int cacheLineSize;
	vector<char> memory;
	unordered_set<int> touched;
};