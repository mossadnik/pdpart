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
    def _fn_meta(self):
        return os.path.join(self.dirname, "meta.json")

    def _fn_part(self, part):
        filename = self._filename_template % part
        return os.path.join(self.dirname, filename)

    @staticmethod
    def new_like(dirname, partitioned):
        """create new Partitioned using meta data from existng one."""
        return Partitioned(dirname,
                           by=None,
                           n_partition=partitioned.n_partition,
                           compression=partitioned.compression)

    def __init__(self, dirname, by=None, n_partition=None, compression=None, reuse=False):
        """Create new Partitioned instance

        Parameters
        ----------
        dirname : str
            directory to put data, need not exist
        by : str
            column to use for partitioning
        n_partition : int
            number of partitions
        compression : str
            either "gzip" or None for no compression
        reuse :
        """
        self._n_partition = n_partition
        self._compression = compression
        self.by = by
        self._dirname = os.path.abspath(dirname)

        suffix = {None: "", "gzip": ".gz"}[compression]
        digits = str(ceil(log10(n_partition)))
        self._filename_template = os.path.join(self.dirname, "%0" + digits + "d.csv" + suffix)

        # check whether this has been created already
        if reuse:
            try:
                with open(self._fn_meta(), "r") as fp:
                    meta = json.load(fp)
                self.n_partition = meta["n_partition"]
                self.compression = meta["compression"]
                self.by = meta["by"]
            except:
                raise ValueError("not a valid directory %s" % dirname)
            self._initialized = True
        else:
            if n_partition is None:
                raise ValueError("must specify n_partition if reuse is False")
            self._initialized = False

    def init_dir(self):
        """intialize directory

        If the directory exists its content is removed.
        """
        dirname = self.dirname
        if os.path.exists(dirname):
            shutil.rmtree(dirname)
        os.makedirs(dirname)
        with open(self._fn_meta(), "w") as fp:
            json.dump({"n_partition": self.n_partition, "compression": self.compression, "by": self.by}, fp)
        self._initialized = True
        return self

    def append(self, df):
        """write dataframe to partitions"""
        if self.by is None:
            raise ValueError("must set `by` in order to append")
        if not self._initialized:
            raise IOError("need to initialize directory first")

        kw = dict(index=False, compression=self.compression)
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

    @property
    def compression(self):
        return self._compression

    @property
    def n_partition(self):
        return self._n_partition

    @property
    def dirname(self):
        return self._dirname

    def __repr__(self):
        return "<Partitioned(dirname='%s')" % self.dirname

    def __str__(self):
        return self.__repr__()
