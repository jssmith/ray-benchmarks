# Ray Benchmarks

## Word Count

Parameters are as follows:
- `num workers` - how many worker processes Ray is to launch.
- `num splits` - how much parallelism there will be in the task graph. This will set the number of tasks used to load inputs.
- `input files` - list of files to sort.

```
(NUM_WORKERS=6
NUM_SPLITS=6
python wc_ray.py $NUM_WORKERS $NUM_SPLITS /etc/*.conf)
```

## Sorting

This sort benchmark is based on the [TeraSort benchmark](http://hadoop.apache.org/docs/r2.7.3/api/org/apache/hadoop/examples/terasort/package-summary.html).

### Generate data files

Parameters are as follows:

- `num workers` - how many worker processes Ray is to launch.
- `num splits` - how many tasks will be used to generate random data.
- `num records` - how many records to produce. This is the aggregate number across all files.
- `file prefix` - prefix for generated file names.

```
(NUM_WORKERS=4
NUM_RECORDS=1000000
NUM_SPLITS=25
FILE_PREFIX=sort_test
python teragen.py $NUM_WORKERS $NUM_SPLITS $NUM_RECORDS $FILE_PREFIX)
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