import builtins
import sys
from types import CodeType

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
            import ipywidgets  # noqa
        except ImportError:
            return False
        return True
    return False


if hasattr(CodeType, "replace"):
    code_replace = CodeType.replace
else:

    def code_replace(code, *, co_code):
        return CodeType(
            code.co_argcount,
            code.co_kwonlyargcount,
            code.co_nlocals,
            code.co_stacksize,
            code.co_flags,
            co_code,
            code.co_consts,
            code.co_names,
            code.co_varnames,
            code.co_filename,
            code.co_name,
            code.co_firstlineno,
            code.co_lnotab,
            code.co_freevars,
            code.co_cellvars,
        )
