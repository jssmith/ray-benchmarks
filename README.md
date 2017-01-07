# Ray Benchmarks

## Word Count

### Generate data files

We generate synthetic data for the word count benchmark by sampling from a reference text (Shakespeare's King Lear).

Parameters are as follows:

- `num workers` - how many worker processes Ray is to launch.
- `num splits` - how many tasks will be used to generate random data.
- `num words` - how many words to generate across all splits.
- `file prefix` - prefix for generated file names.


```
(NUM_WORKERS=4
NUM_SPLITS=64
NUM_WORDS=6400000
FILE_PREFIX=wc_test
python wcgen.py $NUM_WORKERS $NUM_SPLITS $NUM_WORDS $FILE_PREFIX)
```

### Counting words

Parameters are as follows:
- `num workers` - how many worker processes Ray is to launch.
- `num splits` - how much parallelism there will be in the task graph. This will set the number of tasks used to load inputs.
- `input files` - list of files to sort.

```
(NUM_WORKERS=5
NUM_SPLITS=5
python wc_ray.py $NUM_WORKERS $NUM_SPLITS wc_test_*)
```

## Sorting

This sort benchmark is based on the [TeraSort benchmark](http://hadoop.apache.org/docs/r2.7.3/api/org/apache/hadoop/examples/terasort/package-summary.html).

### Generate data files

Parameters are as follows:

- `num workers` - how many worker processes Ray is to launch.
- `num splits` - how many tasks will be used to generate random data.
- `num records` - how many records to produce. This is the aggregate number across all files.
- `file prefix` - prefix for generated file names.
- `file format` - file format, either `text` or `numpy`

```
(NUM_WORKERS=4
NUM_RECORDS=1000000
NUM_SPLITS=25
FILE_PREFIX=sort_test
FILE_FORMAT=numpy
python teragen.py $NUM_WORKERS $NUM_SPLITS $NUM_RECORDS $FILE_PREFIX $FILE_FORMAT)
```

### Run the benchmark

Parameters are as follows:

- `num workers` - how many worker processes Ray is to launch.
- `num splits` - how much parallelism there will be in the task graph. This will set the number of tasks used to load inputs and the number of partitions in the sorted result.
- `input files` - list of files to sort.

```
(NUM_WORKERS=6
NUM_SPLITS=6
python sort_ray.py $NUM_WORKERS $NUM_SPLITS sort_test*)
```

## Key-Value Store

The Key-Value Store benchmark runs on the same input data as the Sorting benchmark.

Parameters are as follows:

- `num workers` - how many worker processes Ray is to launch. This should be at least two times `num splits`, otherwise the benchmark will hang.
- `num splits` - how much parallelism there will be in the task graph. This will set the number of tasks used to load inputs and the number tasks used to execute parallel queries against the key-value store.
- `input files` - list of files containing data to index.

```
(NUM_WORKERS=12
NUM_SPLITS=6
python kvs_ray.py $NUM_WORKERS $NUM_SPLITS sort_test*)
```

# Ray Benchmark Parameter Sweeps

Parameters are as follows:

- `progression` - either `arithmetic` or `geometric`
- `start` - starting value, may use `0` with arithmetic progression to include `1`
- `end` - ending value
- `step` - increment, additive for arithmetic progression and multiplicative for geometric progression
- `benchmark` - what program to test - either `wc` or `sort`
- `partition size` - how many records per partition
- `input prefix` - input prefix for data files
- `file format` - either `text` or `numpy`

```
(PROGRESSION=geometric
START=1
END=64
STEP=4
BENCHMARK=wc
PARTITION_SIZE=100000
INPUT_PREFIX=wc_test
FILE_FORMAT=text
python sweep.py $PROGRESSION $START $END $STEP $BENCHMARK $PARTITION_SIZE $INPUT_PREFIX $FILE_FORMAT)
```