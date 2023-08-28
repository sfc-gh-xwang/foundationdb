#!/usr/bin/env python3

# Example usage:
#
# $ devGetTestDurations <ensemble_id_1> > durations1
# $ devGetTestDurations <ensemble_id_2> > durations2
# $ test_duration_stats.py [-f TestName] durations1 durations2

import argparse
import math
import sys

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--filter", type=str, help="Filter string for test file name")
parser.add_argument(
    "filenames",
    nargs="+",
    help="One or more files with ascending ordered set of test runtimes. These can be produced using devGetTestDurations.",
)

args = parser.parse_args()

data = []
for f in args.filenames:
    file = open(f, "r+")
    lines = file.readlines()
    data.append([])
    for line in lines:
        fields = line.split(" ")
        if len(fields) == 2 and args.filter is None or fields[1].find(args.filter) >= 0:
            data[-1].append(float(fields[0]))


def percentile(label, p):
    print("%s:" % label, end="")
    base = None
    for d in data:
        if len(d) > 0:
            val = d[min(len(d) - 1, int(len(d) * p))]
            if base is None:
                base = val
                print(" {0: >8}".format("%.2f" % val), end="")
            else:
                print(" {0: >20}".format("%.2f (%.2fx)" % (val, val / base)), end="")
        else:
            print(" N/A", end="")

    print()


print("Num tests:", end="")
for i, d in enumerate(data):
    if i == 0:
        print(" {0: >8}".format(len(d)), end="")
    else:
        print(" {0: >20}".format(len(d)), end="")

print()

percentile("Median   ", 0.5)
percentile("p90      ", 0.9)
percentile("p99      ", 0.99)
percentile("p99.9    ", 0.999)
percentile("Max      ", 1)
