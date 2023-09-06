#pragma once
#include <vector>

using std::vector;

struct TreeNode {
	TreeNode(int key) : key(key), left(nullptr), right(nullptr) {}

	int key;
	TreeNode* left;
	TreeNode* right;
};

// Returns a leftist perfectly balanced binary tree with n nodes
TreeNode* buildTree(int n);

int height(TreeNode* node);

void level(TreeNode* root, int depth, vector<TreeNode*>& output);