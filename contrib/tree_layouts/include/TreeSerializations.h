#pragma once
#include "Memory.h"
#include "Tree.h"
#include <cstddef>
#include <cstdlib>
#include <vector>

#define NULL_SERIALIZED -1

using std::pair;
using std::make_pair;

#pragma pack(push, 1)
struct SerializedTreeNode {
    SerializedTreeNode() {}

    SerializedTreeNode(int key, int size) : key(key), left(NULL_SERIALIZED), right(NULL_SERIALIZED), size(size) {
        assert(size >= sizeof(SerializedTreeNode));
    }

    void write(Memory &mem, int offset) {
        mem.write(offset, this, sizeof(SerializedTreeNode));
        mem.probe(offset + sizeof(SerializedTreeNode), size - sizeof(SerializedTreeNode));
    }

    void read(Memory &mem, int offset) {
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
    TreeSerialization(Memory &mem, TreeNode *root, int nodeSize) : mem(mem), root(root), nodeSize(nodeSize) {}

    void get(int key) {
        get(key, 0);
    }

    Memory &getMemory() const {
        return mem;
    }

protected:
    Memory &mem;
    TreeNode *root;
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
    PreorderSerialization(Memory &mem, TreeNode *root, int nodeSize) : TreeSerialization(mem, root, nodeSize) {
        serialize(root, 0);
    }

private:
    // Returns the number of bytes written
    int serialize(TreeNode *root, int offset) {
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
    VEBSerialization(Memory &mem, TreeNode *root, int nodeSize) : TreeSerialization(mem, root, nodeSize) {
        vector<pair<TreeNode *, int>> bottom;
        serialize(root, 0, height(root) - 1, bottom);
    }

private:
    // Serializes the tree up to the specified (0-based) depth. Returns the number of bytes written.
    // The argument bottom outputs a map from the nodes at the target depth into their offsets.
    int serialize(TreeNode *root, int offset, int depth, vector<pair<TreeNode *, int>> &bottom) {
        if (root == nullptr or depth < 0) {
            return 0;
        }
        if (depth <= 1) {
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
        
        vector<pair<TreeNode *, int>> middle;
        int written = serialize(root, offset, depth >> 1, middle);
        offset += written;

        for (auto x : middle) { 
            TreeNode* node = x.first;
            int nodeOffset = x.second;
            int leftOffset = offset;
            int leftSize = serialize(node->left, leftOffset, depth - (depth >> 1) - 1, bottom);
            int rightOffset = leftOffset + leftSize;
            int rightSize = serialize(node->right, rightOffset, depth - (depth >> 1) - 1, bottom);
            offset = rightOffset + rightSize;
            written += leftSize + rightSize;

            // Modify the bottom nodes of the top tree to point to the serializations
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

        return written;
    }
};