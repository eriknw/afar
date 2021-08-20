from ._version import get_versions
from .core import get, later, locally, remotely, run  # noqa

__version__ = get_versions()["version"]
del get_versions
