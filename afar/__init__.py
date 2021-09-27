"""afar runs code within a context manager or IPython magic on a Dask cluster.

>>> with afar.run, remotely:
...     import dask_cudf
...     df = dask_cudf.read_parquet("s3://...")
...     result = df.sum().compute()

or to use an IPython magic:

>>> %load_ext afar
>>> %afar z = x + y

Read the documentation at https://github.com/eriknw/afar
"""

from ._core import get, run  # noqa
from ._version import get_versions
from ._where import later, locally, remotely  # noqa

__version__ = get_versions()["version"]
del get_versions


def load_ipython_extension(ip):
    from ._magic import AfarMagic

    ip.register_magics(AfarMagic)
