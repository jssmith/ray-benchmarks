#!/bin/bash

PROGRESSION=arithmetic
START=32
END=32
STEP=1

(BENCHMARK=wc
PARTITION_SIZE=100000
INPUT_PREFIX=wc_test
FILE_FORMAT=text
python sweep.py $PROGRESSION $START $END $STEP $BENCHMARK $PARTITION_SIZE $INPUT_PREFIX $FILE_FORMAT)
mv events.json.gz output/events_wc.json.gz
python plot_worker_activity.py output/events_wc.json.gz output/events_wc.pdf

# This benchmark is running really slowly
#(BENCHMARK=kvs
#PARTITION_SIZE=10000
#INPUT_PREFIX=sort_test_10k
#FILE_FORMAT=numpy
#python sweep.py $PROGRESSION $START $END $STEP $BENCHMARK $PARTITION_SIZE $INPUT_PREFIX $FILE_FORMAT)
#mv events.json.gz output/events_kvs.json.gz

(BENCHMARK=sort
PARTITION_SIZE=10000
INPUT_PREFIX=sort_test_10k
FILE_FORMAT=numpy
python sweep.py $PROGRESSION $START $END $STEP $BENCHMARK $PARTITION_SIZE $INPUT_PREFIX $FILE_FORMAT)
mv events.json.gz output/sort_10k.json.gz
python plot_worker_activity.py output/sort_10k.json.gz output/sort_10k.pdf

(BENCHMARK=sort
PARTITION_SIZE=100000
INPUT_PREFIX=sort_test_100k
FILE_FORMAT=numpy
python sweep.py $PROGRESSION $START $END $STEP $BENCHMARK $PARTITION_SIZE $INPUT_PREFIX $FILE_FORMAT)
mv events.json.gz output/sort_100k.json.gz
python plot_worker_activity.py output/sort_100k.json.gz output/sort_100k.pdf

(BENCHMARK=sort
PARTITION_SIZE=1000000
INPUT_PREFIX=sort_test_1m
FILE_FORMAT=numpy
python sweep.py $PROGRESSION $START $END $STEP $BENCHMARK $PARTITION_SIZE $INPUT_PREFIX $FILE_FORMAT)
mv events.json.gz output/sort_1m.json.gz
python plot_worker_activity.py output/sort_1m.json.gz output/sort_1m.pdf

(BENCHMARK=matmul
PROGRESSION=arithmetic
START=4
END=4
STEP=1
PARTITION_SIZE=2000
INPUT_PREFIX=mat_8_16k
FILE_FORMAT=numpy
python sweep.py $PROGRESSION $START $END $STEP $BENCHMARK $PARTITION_SIZE $INPUT_PREFIX $FILE_FORMAT)
mv events.json.gz output/mat_mult_16.json.gz
python plot_worker_activity.py output/mat_mult_16.json.gz output/mat_mult_16.pdf

(BENCHMARK=matmul
PROGRESSION=arithmetic
START=5
END=5
STEP=1
PARTITION_SIZE=2000
INPUT_PREFIX=mat_8_16k
FILE_FORMAT=numpy
python sweep.py $PROGRESSION $START $END $STEP $BENCHMARK $PARTITION_SIZE $INPUT_PREFIX $FILE_FORMAT)
mv events.json.gz output/mat_mult_25.json.gz
python plot_worker_activity.py output/mat_mult_25.json.gz output/mat_mult_25.pdf
