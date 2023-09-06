#include "include/Tree.h"
#include <vector>

using std::max;
using std::vector;

TreeNode* buildTree(int n, int start) {
	if (n == 0) {
		return nullptr;
	}
	if (n == 1) {
		return new TreeNode(start);
	}
	int r = (1 << static_cast<int>(log2(n))) - 1;
	TreeNode* root = new TreeNode(r + start);
	root->left = buildTree(r, start);
	root->right = buildTree(n - r - 1, start + r + 1);
	return root;
}

TreeNode* buildTree(int n) {
	return buildTree(n, 0);
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