[![Build Status](https://travis-ci.org/mossadnik/pdpart.svg?branch=master)](https://travis-ci.org/mossadnik/pdpart)

# pdpart - biggish data on a laptop

Many data sets I encounter in practice could be categorized as _biggish data_: They do not fit into RAM, but can be processed with satisfactory performance on a single machine. The iterator capabilities of pandas allow to do many common operations on such data sets, but are limited when it comes to aggregations or joins on a key. While [dask]() is often helpful for these cases, it does not offer the same level of maturity and versatility as pandas.

pdpart is a small utility that helps to do out of core/parallel operations by writing dataframes into partitioned csv files. Partitions are defined by a deterministic hash of the key, so that all rows with the same key are guaranteed to be in the same partitions - even across dataframes.

Partitions effectively act as a hash index, so that aggregations and joins on the key become embarrassingly parallel. pdpart does not actually _do_ any of these operations, but enables using other libraries like [multiprocessing]().

Despite (or because?) of its narrow scope, I find that pdpart fits well into my workflow. Beware that at present, I view it as a working example rather than a mature piece of software, and e.g. unit tests are still to be written.

## Basic usage

```python
import pandas as pd
from pdpart import Partitioned

# test data to be partitioned by key
data = pd.DataFrame({
	'key': np.random.choice(['a', 'b', 'c', 'd', 'e', 'f', 'g'], size=100),
	'value': np.arange(100),
})

# create new directory in which partitions are put
parts = partitioned.create('/data/parts/', by='key', n_partition=3)

# add data to the partitions, each key is mapped to a partition deterministically
parts.append(data)

# do something with the partitions
for fn in parts.partitions:
    df = pd.read_csv(fn)
    # do something here ...
```
