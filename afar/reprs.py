import dis
import sys
import traceback
from types import CodeType, FunctionType

try:
    import IPython

    def in_ipython():
        return IPython.get_ipython() is not None


except ImportError:

    def in_ipython():
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


def endswith_expr(func):
    """Does the function end with an expression (not assigment or return)"""
    code = func.__code__
    co_code = code.co_code
    return (
        len(co_code) > 6
        and co_code[-6] == dis.opmap["POP_TOP"]
        and co_code[-4] == dis.opmap["LOAD_CONST"]
        and co_code[-2] == dis.opmap["RETURN_VALUE"]
        and code.co_consts[co_code[-3]] is None
    )


def return_expr(func):
    """Create a new function from func that returns the final expression"""
    code = func.__code__
    # remove POP_TOP and LOAD_CONST (None)
    co_code = code.co_code[:-6] + code.co_code[-2:]
    code = code_replace(code, co_code=co_code)
    rv = FunctionType(
        code,
        func.__globals__,
        name=func.__name__,
        argdefs=func.__defaults__,
        closure=func.__closure__,
    )
    rv.__kwdefaults__ = func.__kwdefaults__
    return rv


class AttrRecorder:
    """Record which attributes are accessed.

    This is used to determine what repr methods IPython/Jupyter is trying to
    use, and in what order.
    """

    def __init__(self):
        self._attrs = []

    def __getattr__(self, attr):
        if "canary" not in attr:
            self._attrs.append(attr)
        raise AttributeError(attr)


def get_repr_methods():
    """List of repr methods that IPython/Jupyter tries to use"""
    ip = IPython.get_ipython()
    if ip is None:
        return
    attr_recorder = AttrRecorder()
    ip.display_formatter.format(attr_recorder)
    repr_methods = attr_recorder._attrs
    repr_methods.append("__repr__")
    return repr_methods


def repr_afar(val, repr_methods):
    """Compute the repr of an object for IPython/Jupyter.

    We call this on a remote object.
    """
    for method_name in repr_methods:
        method = getattr(val, method_name, None)
        if method is None:
            continue
        if method_name == "_ipython_display_":
            return val, method_name, False
        try:
            rv = method()
        except NotImplementedError:
            continue
        except Exception:
            exc_info = sys.exc_info()
            rv = traceback.format_exception(*exc_info)
            return rv, method_name, True
        else:
            return rv, method_name, False
    return None, "__repr__", False


def display_repr(results):
    """Display results from `repr_afar` locally in IPython/Jupyter"""
    val, method_name, is_exception = results
    if is_exception:
        print(val, file=sys.stderr)
        return
    if method_name == "_ipython_display_":
        val._ipython_display_()
    else:
        mimic = MimicRepr(val, method_name)
        IPython.display.display(mimic)


class MimicRepr:
    def __init__(self, val, method_name):
        self.val = val
        self.method_name = method_name

    def __getattr__(self, attr):
        if attr != self.method_name:
            raise AttributeError(attr)
        return self._call

    def _call(self, *args, **kwargs):
        return self.val

    def __dir__(self):
        return [self.method_name]

    def __repr__(self):
        return self.val
