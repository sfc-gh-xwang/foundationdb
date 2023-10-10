#!/bin/sh
python3 -m test_harness.app -s ${JOSHUA_SEED} --use-valgrind --clean-up simfdb
