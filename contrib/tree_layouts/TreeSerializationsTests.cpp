#include "include/TreeSerializations.h"
#include "include/Tree.h"

using std::unordered_map;

void vebOrderTest() {
	int nodeCount = (1 << 4) - 1; // Complete binary tree of height 4
	int cacheLineSize = 64;
	int nodeSize = 16; // 4 nodes per cache line

	TreeNode* root = buildLeftBalancedTree(nodeCount);
	Memory mem(cacheLineSize);
	VEBSerialization veb(mem, root, nodeSize);

	vector<int> expectedOrder{ 7, 3, 11, 1, 0, 2, 5, 4, 6, 9, 8, 10, 13, 12, 14 };
	unordered_map<int, int> expectedTouched{ { 7, 1 }, { 3, 1 },  { 11, 1 }, { 1, 1 },  { 0, 2 },
		                                     { 2, 2 }, { 5, 2 },  { 4, 2 },  { 6, 3 },  { 9, 2 },
		                                     { 8, 2 }, { 10, 2 }, { 13, 2 }, { 12, 2 }, { 14, 2 } };

	int offset = 0;
	for (auto key : expectedOrder) {
		mem.reset();
		SerializedTreeNode serialized;
		serialized.read(mem, offset);
		assert(serialized.key == key);
		offset += nodeSize;

		mem.reset();
		veb.get(key);
		assert(mem.touchedCacheLines() == expectedTouched[key]);
	}
}

int main() {
	vebOrderTest();
	return 0;
}