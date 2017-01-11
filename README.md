# Ray Benchmarks

The Ray Benchmarks are simple examples designed to test basic algorithms in a distributed setting.

## Word Count

The word count benchmark counts the frequency of each word in a set of input documents.

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

This generates 64 files each containing 100,000 words.

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
NUM_RECORDS=64000000
NUM_SPLITS=64
FILE_PREFIX=sort_test_1m
FILE_FORMAT=numpy
python teragen.py $NUM_WORKERS $NUM_SPLITS $NUM_RECORDS $FILE_PREFIX $FILE_FORMAT)
```

This generates 64 files each containing 1 million 100-byte records to be sorted.

### Run the benchmark

Parameters are as follows:

- `num workers` - how many worker processes Ray is to launch.
- `num splits` - how much parallelism there will be in the task graph. This will set the number of tasks used to load inputs and the number of partitions in the sorted result.
- `input files` - list of files to sort.

```
(NUM_WORKERS=6
NUM_SPLITS=6
python sort_ray_np.py $NUM_WORKERS $NUM_SPLITS sort_test*)
```

We use Numpy for storing the text to sort as this allows for faster loading of text from files, and for faster transfers between Ray processes and the object store. The implementation proceeds by first sampling each input file to obtain an estimate of the distribution of keys. Using the estimate of the distribution of keys it sorts inputs and distributes them to approximately equal-size output partitions. Imbalanced output partitions degrade job completion time, so it is important to sample adequately (we use 1,000 samples per split). 

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

This implementation challenges Ray with indexing a large number of 100-byte objects. As implemented this test exercises Ray's ability to retrieve a python dict, as well as the task dispatch speed.

## Matrix Multiplication

### Generate data files

Parameters are as follows:
- `num workers` - how many worker processes Ray is to launch.
- `dim size` - dimension of the matrix
- `dim blocks` - how many block splits among each dimension
- `file prefix` - prefix for matrix file blocks.

```
(NUM_WORKERS=4
DIM_SIZE=16000
DIM_BLOCKS=8
python matgen.py $NUM_WORKERS $DIM_SIZE $DIM_BLOCKS mat_16k_8)
```

The program generates data files filled with random numbers. For the parameters provided here each block is 2,000 x 2,000 entries in size and the matrix is 8 x 8 blocks in size.

### Matrix multiplication

The benchmark program initializes two separate matrices `A` and `B`, both using the same values loaded from the input files. It then computes `C` = `A` * `B`.

```
(NUM_WORKERS=9
NUM_SPLITS=3
python matmul_ray.py $NUM_WORKERS $NUM_SPLITS mat_16k_8)
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