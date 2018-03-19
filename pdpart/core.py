import pandas as pd
import os
import shutil
from zlib import adler32
import json
from math import log10, ceil


def get_partition(keys, n_partition):
    """compute partition id from hash of keys"""
    def map_series(x):
        return adler32(str(x).encode('utf-8')) % n_partition

    if isinstance(keys, pd.Series):
        return keys.map(map_series).values
    else:
        raise NotImplementedError("multi-column keys to come")



class Partitioned(object):
    @staticmethod
    def open(dirname):
        """open existing partitions directory"""
        try:
            with open(os.path.join(dirname, 'meta.json'), 'r') as fp:
                meta = json.load(fp)
                parts = Partitioned(dirname, **meta)
                parts._initialized = True
                return parts
        except:
            raise IOError('not a valid directory: %s' % dirname)

    @staticmethod
    def create(dirname, by, n_partition):
        """create new Partitioned instance and initialize directory.

        Equivalent to

        Partitioned(dirname, by, n_partition).init_dir()
        """
        return Partitioned(dirname, by, n_partition).init_dir()

    def __init__(self, dirname, by, n_partition):
        """Create new Partitioned instance.

        Usually there is no need for this, just use the static methods
        create or open instead.

        Parameters
        ----------
        dirname : str
            directory to put data, need not exist
        by : str
            column to use for partitioning
        n_partition : int
            number of partitions
        """
        self.n_partition = n_partition
        self.by = by
        self.dirname = os.path.abspath(dirname)

        digits = str(ceil(log10(n_partition)))
        self._filename_template = os.path.join(self.dirname, "%0" + digits + "d.csv")
        self._initialized = False


    @property
    def fn_meta(self):
        return os.path.join(self.dirname, "meta.json")

    def _fn_part(self, part):
        filename = self._filename_template % part
        return os.path.join(self.dirname, filename)

    @property
    def meta(self):
        """return meta data for creating similar Partitioned object"""
        return dict(n_partition=self.n_partition, by=self.by)


    def init_dir(self):
        """intialize directory

        If the directory exists its content is removed.
        """
        dirname = self.dirname
        if os.path.exists(dirname):
            shutil.rmtree(dirname)
        os.makedirs(dirname)
        with open(self.fn_meta, "w") as fp:
            json.dump(self.meta, fp)
        self._initialized = True
        return self

    def append(self, df):
        """write dataframe to partitions"""
        if not self._initialized:
            raise IOError("need to initialize directory first")

        kw = dict(index=False)
        # write header for parts that don't exist
        for filename in [f for f in self.partitions if not os.path.exists(f)]:
            df.iloc[:0].to_csv(filename, header=True, **kw)
        # write actual data
        groups = df.groupby(get_partition(df[self.by], self.n_partition))
        for part, _df in groups:
            _df.to_csv(self._fn_part(part), mode="a", header=False, **kw)

    @property
    def partitions(self):
        """iterable of filenames"""
        return (self._fn_part(part) for part in range(self.n_partition))

    def __repr__(self):
        return "<Partitioned(by={by}, n_partition={n_partition})>".format(**self.meta)
