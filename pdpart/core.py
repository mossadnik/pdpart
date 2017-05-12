import pandas as pd
import os
import shutil
from zlib import adler32
import json


def get_partition(data, n_partition):
    """get hash from series or frame

    row -> [0..n_partition - 1]
    """
    def map_series(x):
        return adler32(str(x).encode('utf-8')) % n_partition

    if isinstance(data, pd.Series):
        return data.map(map_series).values
    else:
        raise NotImplementedError("DataFrame to come")


class Partitioned(object):
    def _fn_meta(self):
        return os.path.join(self.dirname, "meta.json")

    def _fn_part(self, part):
        suffix = {None: "", "gzip": ".gz"}[self.compression]
        filename = "%d.csv%s" % (part, suffix)
        return os.path.join(self.dirname, filename)

    @staticmethod
    def from_existing(dirname, partitioned):
        return Partitioned(dirname,
                           by=None,
                           n_partition=partitioned.n_partition,
                           compression=partitioned.compression)

    def __init__(self, dirname, by=None, n_partition=None, compression=None, reuse=False):
        """either in new dir or reuse existing dir

        TODO: separate new / load better
        """
        self.n_partition = n_partition
        self.compression = compression
        self.by = by
        self.dirname = dirname
        # check whether this has been created already
        if reuse:
            try:
                with open(self._fn_meta(), "r") as fp:
                    meta = json.load(fp)
                self.n_partition = meta["n_partition"]
                self.compression = meta["compression"]
            except:
                raise ValueError("not a valid directory %s" % dirname)
            self._initialized = True
        else:
            if n_partition is None:
                raise ValueError("must specify n_partition if not reuse")
            self._initialized = False

    def init_dir(self):
        """empty directory dirname, populate meta.json"""
        if os.path.exists(self.dirname):
            shutil.rmtree(self.dirname)
        os.makedirs(self.dirname)
        with open(self._fn_meta(), "w") as fp:
            json.dump({"n_partition": self.n_partition, "compression": self.compression}, fp)
        self._initialized = True
        return self

    def append(self, df):
        if self.by is None:
            raise ValueError("must set `by` in order to append")
        """write dataframe to partitions"""
        kw = dict(index=False, compression=self.compression)
        if not self._initialized:
            raise Exception("need to initialize directory first")
        # write header for parts that don't exist
        for filename in [f for f in self.partitions() if not os.path.exists(f)]:
            df.iloc[:0].to_csv(filename, header=True, **kw)
        # write actual data
        groups = df.groupby(get_partition(df[self.by], self.n_partition))
        for part, _df in groups:
            _df.to_csv(self._fn_part(part), mode="a", header=False, **kw)

    def partitions(self):
        """iterable of filenames"""
        return (self._fn_part(part) for part in range(self.n_partition))
