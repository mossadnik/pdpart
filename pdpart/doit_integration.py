"""helpers for integration with the doit library"""

from pathlib import Path
from .core import Partitioned


def init_task(parts, name='init-pdpart'):
    """Create a doit task to initialize a Partitioned directory.

    Parameters
    ----------
    parts : pdpart.Partitioned
        Partitioned object to be initialized
    name : str
        name of task, defaults to 'init-pdpart'
    """
    def _wrapper():
        """withhold return value for compatibility with doit"""
        parts.init_dir()

    return {
        'name': name,
        'actions': [(_wrapper, [], {})],
        'file_dep': [],
        'targets': [parts.fn_meta],
        'uptodate': [True],
    }


def partition_task(fn_in, path_out, by, n_partition=200, chunksize=int(1e6), read_csv_kw={}, preprocess=None):
    """Create a doit task to partition a csv file.

    The task name is the name of the directory

    Parameters
    ----------
    fn_in : str or pathlib.Path
        filename of input csv
    path_out : str or pathlib.Path
        directory name for partitions
    by : str
        name of column to partition on
    n_partition : int
        number of partitions
    chunksize : int
        chunksize for reading input file, added to kwargs for
        pandas.read_csv
    read_csv_kw : dict
        kwargs for pandas.read_csv. chunksize is overwritten
        by the chunksize argument.
    preprocess : callable
        a function for preprocessing the input. Must accept and
        return a pandas.DataFrame

    Returns
    -------
    task : dict
        doit task
    """
    def _partition_csv(fn_in, parts, chunksize=int(1e6), read_csv_kw={}, preprocess=None):
        """partition a csv"""
        kw = dict(read_csv_kw, chunksize=chunksize)
        parts.init_dir()
        for df in pd.read_csv(fn_in, **kw):
            if preprocess is not None:
                df = preprocess(df)
            parts.append(df)

    path_out = Path(path_out)
    parts = Partitioned(path_out, by=by, n_partition=n_partition)
    return {
        'name': path_out.name,
        'actions': [
            (_partition_csv, [fn_in, parts],
             {'chunksize': chunksize, 'read_csv_kw': read_csv_kw, 'preprocess': preprocess})
                   ],
        'file_dep': [fn_in],
        'targets':  [parts.fn_meta] + list(parts.partitions),
    }


def transformation_task(parts_in, path_out, func, read_csv_kwargs={}, func_args=[], func_kwargs={}):
    """Create doit tasks for transforming one or more partioned files into a new one.

    wraps a function func that transform input DataFrames into an output DataFrame to
    act on partitioned csv files, and creates tasks for doit to execute these transformations.

    Parameters
    ----------
    parts_in : Partitioned or list
        inputs
    path_out : str or pathlib.Path
        path to output Partitioned. Will be created during task
        execution.
    func : callable
        The transformation to be applied. Has to accept input DataFrames
        and output DataFrame as first arguments. Additional arguments can
        be provided with func_args and func_kwargs arguments.
    read_csv_kwargs : dict
        arguments for reading csv files
    func_args : sequence
        additional arguments for transformation
    func_kwargs : dict
        additional keyword arguments for transformation

    Returns
    -------
    tasks : generator of dicts
        tasks for creating output directories and mapping the transformation
        over partitions.
    """
    def _wrapper(fn_in, fn_out):
        df_in = [pd.read_csv(filename, **read_csv_kwargs) for filename in fn_in]
        res = func(*df_in, *func_args, **func_kwargs)
        res.to_csv(fn_out, index=False)

    if isinstance(parts_in, Partitioned):
        parts_in = [parts_in]
    parts_out = Partitioned(path_out, **parts_in[0].meta)
    filenames = (
        (f[:-1], f[-1]) for f in zip(*[p.partitions for p in parts_in], parts_out.partitions)
    )

    yield init_task(parts_out)
    for fn_in, fn_out in filenames:
        yield {
            'name': Path(fn_out).stem,
            'actions': [(_wrapper, [fn_in, fn_out], {})],
            'file_dep': list(fn_in) + [parts_out.fn_meta],
            'targets':  [fn_out],
        }
