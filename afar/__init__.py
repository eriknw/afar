from ._core import get, run  # noqa
from ._version import get_versions
from ._where import later, locally, remotely  # noqa

__version__ = get_versions()["version"]
del get_versions


def load_ipython_extension(ip):
    from ._magic import AfarMagic

    ip.register_magics(AfarMagic)
