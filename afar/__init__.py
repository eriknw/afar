from .core import run, get, remotely, locally, later  # noqa

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
