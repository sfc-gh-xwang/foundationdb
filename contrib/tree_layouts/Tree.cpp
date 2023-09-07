#include "include/Tree.h"
#include <vector>

using std::max;
using std::vector;

TreeNode* buildLeftBalancedTree(int n, int start) {
	if (n == 0) {
		return nullptr;
	}
	if (n == 1) {
		return new TreeNode(start);
	}
	int r = (1 << static_cast<int>(log2(n))) - 1;
	TreeNode* root = new TreeNode(r + start);
	root->left = buildLeftBalancedTree(r, start);
	root->right = buildLeftBalancedTree(n - r - 1, start + r + 1);
	return root;
}

TreeNode* buildBalancedTree(int n, int start) {
	if (n == 0) {
		return nullptr;
	}
	if (n == 1) {
		return new TreeNode(start);
	}
	int r = n >> 1;
	TreeNode* root = new TreeNode(r + start);
	root->left = buildBalancedTree(r, start);
	root->right = buildBalancedTree(n - r - 1, start + r + 1);
	return root;
}

TreeNode* buildLeftBalancedTree(int n) {
	return buildLeftBalancedTree(n, 0);
}

TreeNode* buildBalancedTree(int n) {
	return buildBalancedTree(n, 0);
}

int height(TreeNode* node) {
	if (node == nullptr) {
		return 0;
	}
	return 1 + max(height(node->left), height(node->right));
}

void level(TreeNode* root, int depth, vector<TreeNode*>& output) {
	if (root == nullptr) {
		return;
	}
	if (depth == 0) {
		output.push_back(root);
		return;
	}
	level(root->left, depth - 1, output);
	level(root->right, depth - 1, output);
}