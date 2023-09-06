#include "include/Memory.h"
#include "include/TreeSerializations.h"
#include "include/Tree.h"
#include <cstddef>
#include <iostream>
#include <vector>
#include <map>
#include <unistd.h>
#include <getopt.h>


using std::endl;
using std::cout;
using std::map;

void computeStats(TreeSerialization &tree, int nodeCount) {
    Memory &mem = tree.getMemory();
    map<int, int> histogram;
    for (int key = 0; key < nodeCount; ++key) {
        mem.reset();
        tree.get(key);
        int touched = mem.touchedCacheLines();
        histogram[touched]++;
    }

    int max = prev(histogram.end())->first;
    double avg = 0;
    for (int i = 1; i <= max; ++i) {
        cout << i << " cache lines:\t " << histogram[i] << " keys" << endl;
        avg += i * histogram[i];
    }
    cout << "Average:\t " << avg / nodeCount << " cache lines per read" << endl;
}

void stats(int cacheLineSize, int nodeSize, int nodeCount) {
    TreeNode* root = buildTree(nodeCount);
    Memory mem1(cacheLineSize);
    Memory mem2(cacheLineSize);
    PreorderSerialization preorder(mem1, root, nodeSize);
    VEBSerialization veb(mem2, root, nodeSize);

    cout << "Cache line:\t " << cacheLineSize << " bytes" << endl;
    cout << "Node size:\t " << nodeSize << " bytes (" << cacheLineSize / static_cast<double>(nodeSize) << " nodes per cache line)" << endl;
    cout << "Node count:\t " << nodeCount << " (" << nodeSize * nodeCount << " bytes in total)" << endl << endl;

    cout << "Preorder:" << endl;
    computeStats(preorder, nodeCount);
    cout << endl << "VEB:" << endl;
    computeStats(veb, nodeCount);
}

void help(char *cmd) {
    cout << cmd << " --cache-line L --node-size S --node-count N" << endl << endl;
    cout << "Computes the average number of cache lines touched on a random read with ";
    cout << "preorder serialization and van Emde Boas serialization of a leftist ";
    cout << "perfectly balanced binary search tree." << endl << endl;
    cout << "Options:" << endl;
    cout << "-l L, --cache-line L" << endl;
    cout << "\tThe size in bytes of the cache lines (the default is L = 64)." << endl;
    cout << "-s S, --node-size S" << endl;
    cout << "\tThe size in bytes of each tree node in serialized form (the default is S = 20)." << endl;
    cout << "-n N, --node-count N" << endl;
    cout << "\tThe number of nodes in the tree (the default is N = 200)." << endl;
}

int main(int argc, char *argv[]) {
    struct option long_options[] =
    {
        {"cache-line", required_argument, NULL, 'l'},
        {"node-size", required_argument, NULL, 's'},
        {"node-count", required_argument, NULL, 'n'},
        {"help", no_argument, NULL, 'h'},
        {NULL, 0, NULL, 0}
    };

    int cacheLineSize = 64;
    int nodeSize = 20;
    int nodeCount = 200;
    char ch;
    while ((ch = getopt_long(argc, argv, "l:s:n:h", long_options, NULL)) != -1) {
        switch (ch) {
            case 'l':
                cacheLineSize = atoi(optarg);
                break;
            case 'n':
                nodeCount = atoi(optarg);
                break;
            case 's':
                nodeSize = atoi(optarg);
                break;
            case 'h':
                help(argv[0]);
                return 0;
            default:
                help(argv[0]);
                exit(1);
        }
    }

    cout << "Run \"" << argv[0] << " --help\" for usage information" << endl << endl;

    stats(cacheLineSize, nodeSize, nodeCount);

    return 0;
}