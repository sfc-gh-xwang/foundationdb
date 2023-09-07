#pragma once
#include "Memory.h"
#include "Tree.h"
#include <cstddef>
#include <cstdlib>
#include <vector>
#include <queue>
#include <unordered_map>

#define NULL_SERIALIZED -1

using std::make_pair;
using std::max;
using std::pair;
using std::queue;
using std::unordered_map;

#pragma pack(push, 1)
struct SerializedTreeNode {
	SerializedTreeNode() : left(NULL_SERIALIZED), right(NULL_SERIALIZED) {}

	SerializedTreeNode(int key, int size) : key(key), size(size), left(NULL_SERIALIZED), right(NULL_SERIALIZED) {
		assert(size >= sizeof(SerializedTreeNode));
	}

	void write(Memory& mem, int offset) {
		mem.write(offset, this, sizeof(SerializedTreeNode));
		mem.probe(offset + sizeof(SerializedTreeNode), size - sizeof(SerializedTreeNode));
	}

	void read(Memory& mem, int offset) {
		mem.read(offset, this, sizeof(SerializedTreeNode));
		mem.probe(offset + sizeof(SerializedTreeNode), size - sizeof(SerializedTreeNode));
	}

	int key;
	int left;
	int right;
	int size;
};
#pragma pack(pop)

class TreeSerialization {
public:
	TreeSerialization(Memory& mem, TreeNode* root, int nodeSize) : mem(mem), root(root), nodeSize(nodeSize) {}

	void get(int key) { get(key, 0); }

	Memory& getMemory() const { return mem; }

protected:
	Memory& mem;
	TreeNode* root;
	int nodeSize;

private:
	void get(int key, int offset) {
		assert(offset >= 0);
		SerializedTreeNode serialized;
		serialized.read(mem, offset);
		if (key == serialized.key) {
			return;
		} else if (key < serialized.key and serialized.left != NULL_SERIALIZED) {
			get(key, serialized.left);
		} else if (key > serialized.key and serialized.right != NULL_SERIALIZED) {
			get(key, serialized.right);
		}
	}
};

class PreorderSerialization : public TreeSerialization {
public:
	PreorderSerialization(Memory& mem, TreeNode* root, int nodeSize) : TreeSerialization(mem, root, nodeSize) {
		serialize(root, 0);
	}

private:
	int serialize(TreeNode* root, int offset) {
		if (root == nullptr) {
			return 0;
		}
		SerializedTreeNode serialized(root->key, nodeSize);
		int leftOffset = offset + serialized.size;
		int leftSize = serialize(root->left, leftOffset);
		int rightOffset = leftOffset + leftSize;
		int rightSize = serialize(root->right, rightOffset);
		if (leftSize > 0) {
			serialized.left = leftOffset;
		}
		if (rightSize > 0) {
			serialized.right = rightOffset;
		}
		serialized.write(mem, offset);
		return serialized.size + leftSize + rightSize;
	}
};

class VEBSerialization : public TreeSerialization {
public:
	VEBSerialization(Memory& mem, TreeNode* root, int nodeSize) : TreeSerialization(mem, root, nodeSize) {
		vector<pair<TreeNode*, int>> bottom;
		serialize(root, 0, height(root) - 1, bottom);
	}

private:
	// Serializes the tree up to the specified (0-based) depth.
	// The output argument bottom maps the nodes at the target depth to their offsets.
	int serialize(TreeNode* root, int offset, int depth, vector<pair<TreeNode*, int>>& bottom) {
		if (root == nullptr or depth < 0) {
			return 0;
		}
		if (depth <= 2) {
			SerializedTreeNode serialized(root->key, nodeSize);
			int leftOffset = offset + serialized.size;
			int leftSize = serialize(root->left, leftOffset, depth - 1, bottom);
			int rightOffset = leftOffset + leftSize;
			int rightSize = serialize(root->right, rightOffset, depth - 1, bottom);
			if (leftSize > 0) {
				serialized.left = leftOffset;
			}
			if (rightSize > 0) {
				serialized.right = rightOffset;
			}
			serialized.write(mem, offset);
			if (depth == 0) {
				bottom.push_back(make_pair(root, offset));
			}
			return serialized.size + leftSize + rightSize;
		}

		vector<pair<TreeNode*, int>> middle;
		int initialOffset = offset;
		offset += serialize(root, offset, (depth >> 1) - 1, middle);

		for (auto x : middle) {
			TreeNode* node = x.first;
			int nodeOffset = x.second;
			int leftOffset = offset;
			int leftSize = serialize(node->left, leftOffset, depth - (depth >> 1), bottom);
			int rightOffset = leftOffset + leftSize;
			int rightSize = serialize(node->right, rightOffset, depth - (depth >> 1), bottom);
			offset = rightOffset + rightSize;

			// Modify the bottom nodes of the top tree to point to their serialized children
			SerializedTreeNode serialized;
			serialized.read(mem, nodeOffset);
			if (leftSize > 0) {
				serialized.left = leftOffset;
			}
			if (rightSize > 0) {
				serialized.right = rightOffset;
			}
			serialized.write(mem, nodeOffset);
		}

		return offset - initialOffset;
	}
};

/*
 * Cache-aware serialization of the tree. Roughly speaking, if each cache line can hold
 * B nodes, the algorithm groups the first B nodes in BFS order, and then recurses on
 * the unvisited children of these B nodes.
 */
class CacheAwareSerialization : public TreeSerialization {
public:
	CacheAwareSerialization(Memory& mem, TreeNode* root, int nodeSize, int cacheLineSize, bool straddling)
	  : TreeSerialization(mem, root, nodeSize),
	    cacheLineSize(cacheLineSize),
	    straddling(straddling) {
		// When straddling is set to false, nodes never straddle cache lines
		assert(straddling ||
		       nodeSize <= cacheLineSize); // Cache lines must be able to hold at least 1 full serialized node
		serialize(root, 0);
	}

private:
	struct ParentPointer {
		int offset;
		enum { LEFT_CHILD, RIGHT_CHILD } which;
	};

	int aligned(int offset) {
		if (offset % cacheLineSize == 0) {
			return offset;
		}
		return ((offset / cacheLineSize) + 1) * cacheLineSize;
	}

	void attach(int offset, ParentPointer& parent) {
		SerializedTreeNode serialized;
		serialized.read(mem, parent.offset);
		if (parent.which == ParentPointer::LEFT_CHILD) {
			serialized.left = offset;
		} else {
			serialized.right = offset;
		}
		serialized.write(mem, parent.offset);
	}

	int serialize(TreeNode* root, int offset) {
		int initialOffset = offset;
		assert(straddling || offset % cacheLineSize == 0);
		queue<TreeNode*> q;
		unordered_map<TreeNode*, ParentPointer> parentOffset; // Maps each node to the offset of the serialized parent
		int run = max(1, cacheLineSize / nodeSize);
		q.push(root);
		while (not q.empty() and run-- > 0) {
			TreeNode* node = q.front();
			q.pop();
			SerializedTreeNode serialized(node->key, nodeSize);
			serialized.write(mem, offset);
			if (node != root) {
				attach(offset, parentOffset[node]);
			}
			if (node->left != nullptr) {
				parentOffset[node->left] = ParentPointer{ offset, ParentPointer::LEFT_CHILD };
				q.push(node->left);
			}
			if (node->right != nullptr) {
				parentOffset[node->right] = ParentPointer{ offset, ParentPointer::RIGHT_CHILD };
				q.push(node->right);
			}
			offset += nodeSize;
		}

		while (not q.empty()) {
			if (not straddling) {
				offset = aligned(offset);
			}
			TreeNode* node = q.front();
			q.pop();
			assert(node != root);
			attach(offset, parentOffset[node]);
			offset += serialize(node, offset);
		}

		return offset - initialOffset;
	}

	int cacheLineSize;
	bool straddling;
};
