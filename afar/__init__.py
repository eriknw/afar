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

import importlib.metadata

from . import _utils
from ._core import get, run  # noqa
from ._where import later, locally, remotely  # noqa

try:
    __version__ = importlib.metadata.version("afar")
except Exception as exc:  # pragma: no cover (safety)
    raise AttributeError(
        "`afar.__version__` not available. This may mean "
        "afar was incorrectly installed or not installed at all. "
        "For local development, you may want to do an editable install via "
        "`python -m pip install -e path/to/afar`"
    ) from exc
del importlib

if _utils.is_ipython():
    from ._magic import new_magic  # noqa


def load_ipython_extension(ip):
    from ._magic import AfarMagic

    ip.register_magics(AfarMagic)
