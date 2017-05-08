import pandas as pd
import numpy as np
import os
import shutil
from zlib import adler32


def get_partition(data, n_partition):
    """get hash from series or frame"""
    if isinstance(data, pd.Series):
        return data.map(lambda x: adler32(str(x).encode('utf-8')) % n_partition).values
    else:
        raise NotImplementedError("DataFrame to come")
     
    
def to_parts(df, key, n_partition, dirname, append=True):
    for part, _df in df.groupby(get_partition(df[key], n_partition)):
        filename = os.path.join(dirname, make_filename(part))
        if append:
            # no header if appending to existing file
            header = not os.path.exists(filename)
            mode = "a"
        else:
            header = True
            mode = "w"
        with open(filename, mode) as f:
            _df.to_csv(f, header=header, index=False)


def init_dir(dirname):
    """cleanup data dir"""
    if os.path.exists(dirname):
        shutil.rmtree(dirname)
    os.makedirs(dirname)


def get_parts(dirname):
    """parts and filenames"""
    return sorted([(int(f.split(".")[0]), os.path.join(dirname, f)) for f in os.listdir(dirname)], key=lambda x: x[0])


def make_filename(part):
    return "%d.csv" % part


def iter_parts(dirname, **kwargs):
    """iterate over parts"""
    for part, filename in get_parts(dirname):
        yield pd.read_csv(filename, **kwargs)