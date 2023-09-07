Memory simulator for estimating the cache efficiency of some tree serializations. We implemented a preorder serialization and van Emde Boas serialization of a binary search tree, which run on top of the memory simulator. The goal is to evaluate whether using VEB serialization in the DeltaTree can save cache misses on average, compared to the current preorder serialization.

We also implemented a cache-aware serialization. Roughly speaking, if B is the number of nodes that fit in a cache line, this algorithm groups together the top B nodes in BFS order, and recurses on the unvisited children of these nodes. This algorithm has a tunable option that prevents nodes from straddling cache lines (which cause unnecessary cache misses) at the cost of some space amplification.

The simulation outputs for each layout the histogram of the distribution of the number of cache lines probed, as well as the average.

# Build/run instructions:
`cd contrib/tree_layouts`
`mkdir build && cd build && cmake .. && make`
`./stats`

The simulation has three parameters: the cache line size, the tree node size, and the number of nodes. See usage information with `./stats --help`.

# Results

We tested the serializations on cache line sizes 64, 128 and 256 bytes, on three data sets about 8kB big, namely (i) 400 nodes with size 20 bytes, (ii) 320 nodes with size 25 bytes, and (iii) 200 nodes with size 40 bytes. We also varied the tree topology: we used a left balanced binary search tree (which places more nodes on the left child (and, thus, should benefit preorder)), and a weight balanced binary search tree (which places about the same nodes on left and right children). In each case, we compute the average number of cache lines touched on a positive query.

1. **Node count:** 400
**Node size:** 20
**Tree topology:** Left balanced binary tree

Cache line size | Preorder | VEB | Cache aware, with straddling | Cache aware, no straddling
-- | -- | -- | -- | --
64 | 5.43 | 6.1075 | 5.7575 | 4.285
128 | 4.2775 | 4.2325 | 4.08 | 3.09
256 | 3.7475 | 3.0275 | 3.0875 | 2.58

2. **Node count:** 320
**Node size:** 25
**Tree topology:** Left balanced binary tree

Cache line size | Preorder | VEB | Cache aware, with straddling | Cache aware, no straddling
-- | -- | -- | -- | --
64 | 6.1875 | 6.48438 | 6.36875 | 5.1625
128 | 4.72187 | 4.5875 | 4.275 | 3.32812
256 | 3.88437 | 3.15313 | 2.84063 | 2.625

3. **Node count:** 200
**Node size:** 40
**Tree topology:** Left balanced binary tree

Cache line size | Preorder | VEB | Cache aware, with straddling | Cache aware, no straddling
-- | -- | -- | -- | --
64 | 7.12 | 7.54 | 7.12 | 6.935
128 | 4.585 | 5.165 | 4.94 | 3.63
256 | 3.74 | 3.555 | 3.46 | 2.76

4. **Node count:** 400
**Node size:** 20
**Tree topology:** Weight balanced binary tree

Cache line size | Preorder | VEB | Cache aware, with straddling | Cache aware, no straddling
-- | -- | -- | -- | --
64 | 6.075 | 5.975 | 5.2 | 4.16
128 | 4.7 | 4.24 | 3.86 | 3.0675
256 | 4.15 | 3.03 | 3.01 | 2.55

5. **Node count:** 320
**Node size:** 25
**Tree topology:** Weight balanced binary tree

Cache line size | Preorder | VEB | Cache aware, with straddling | Cache aware, no straddling
-- | -- | -- | -- | --
64 | 6.53125 | 6.42188 | 5.925 | 5.10625
128 | 4.72187 | 4.72187 | 4.175 | 3.25
256 | 4.06562 | 3.34687 | 3.18125 | 2.59375

6. **Node count:** 200
**Node size:** 40
**Tree topology:** Weight balanced binary tree

Cache line size | Preorder | VEB | Cache aware, with straddling | Cache aware, no straddling
-- | -- | -- | -- | --
64 | 6.59 | 7.28 | 6.59 | 6.765
128 | 4.83 | 4.895 | 4.81 | 3.595
256 | 4.065 | 3.495 | 3.36 | 2.755

# Analysis

Our FDB clusters currently use 64-byte CPU cache lines, so for VEB to be a viable replacement of preorder it should at least win in that case. Unfortunately, most of the time it does not. This is because when the cache line is small, instance-specific details can cause VEB to generate a partition of the nodes with a slightly worse access cost distribution. For instance, because VEB works by dividing the tree into layers, it can produce a serialization in which some nodes do not share its cache line with neither its parent nor a child; this can never happen for internal nodes in preorder.

One problem that affects both preorder and VEB are straddling nodes. When a node straddles a cache line, the second read is wasteful when the query path does not reuse the second cache line. When a leaf straddles a cache line, the second read is always unnecessary. Notice that the cache aware serialization without straddling incurs 1-2 less cache misses than preorder, in most cases. To avoid straddling, the algorithm skips to a new cache when writing a new block of nodes, causing some internal fragmentation. The space amplification we observed is very instance-dependent (of course, space amp larger than 2x can always be fixed with defragmentation). It may be interesting to think how to trade space for I/Os when designing a memory layout.