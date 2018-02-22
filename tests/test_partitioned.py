import os
from pathlib import Path
import pandas as pd
import numpy as np
from pdpart import Partitioned
import tempfile


def make_test_data(n):
    chars = [chr(i) for i in range(ord('a'), ord('z') + 1)]
    return pd.DataFrame({
        "key": np.random.choice(chars, size=n),
        "value": np.arange(n)
    })


def test_create_append():
    """check that no data is lost or changed during partitioning"""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)
        dirname = path.joinpath('parts')
        data = make_test_data(401)

        parts = Partitioned.create(dirname, by='key', n_partition=13)

        parts.append(data)

        # reassemble data frame from parts
        df_parts = pd.concat([pd.read_csv(fn) for fn in parts.partitions], axis=0)

        # check dataframe has not changed
        assert np.all(np.equal(
            *[df.sort_values(['key', 'value']).reset_index(drop=True) for df in [data, df_parts]]
        )), 'values of dataframe not preserved'


def test_open():
    """check that opening existing parts works"""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)
        dirname = path.joinpath('parts')
        data = make_test_data(401)

        parts = Partitioned.create(dirname, by='key', n_partition=13)

        parts.append(data)

        # reassemble data frame from parts
        df_parts = pd.concat([pd.read_csv(fn) for fn in parts.partitions], axis=0)

        opened_parts = Partitioned.open(dirname)
        for k in parts.meta.keys():
            assert parts.meta[k] == opened_parts.meta[k], 'meta data item %s inconsistent' % k

        df_opened_parts = pd.concat([pd.read_csv(fn) for fn in opened_parts.partitions], axis=0)
        # check dataframe has not changed
        assert np.all(np.equal(
            *[df.sort_values(['key', 'value']).reset_index(drop=True) for df in [df_parts, df_opened_parts]]
        )), 'values of dataframe not preserved'



