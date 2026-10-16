import builtins
import sys

from distributed.utils import is_kernel


def is_terminal():
    if not is_ipython():
        return False
    from IPython import get_ipython

    return type(get_ipython()).__name__ == "TerminalInteractiveShell"


def is_ipython():
    return hasattr(builtins, "__IPYTHON__") and "IPython" in sys.modules


def supports_async_output():
    if is_kernel() and not is_terminal():
        try:
            import ipywidgets  # noqa: F401
        except ImportError:
            return False
        return True
    return False
