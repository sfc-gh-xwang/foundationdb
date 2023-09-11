#pragma once
#include <vector>

using std::vector;

struct TreeNode {
	TreeNode(int key) : key(key), left(nullptr), right(nullptr) {}

	int key;
	TreeNode* left;
	TreeNode* right;
};

// Returns a left balanced binary tree with n nodes
TreeNode* buildLeftBalancedTree(int n);

// Returns a balanced binary tree with n nodes
TreeNode* buildWeightBalancedTree(int n);

int height(TreeNode* node);

void level(TreeNode* root, int depth, vector<TreeNode*>& output);