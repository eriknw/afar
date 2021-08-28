import sys
import traceback


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
    from IPython import get_ipython

    ip = get_ipython()
    if ip is None:
        return
    attr_recorder = AttrRecorder()
    ip.display_formatter.format(attr_recorder)
    return attr_recorder._attrs


def repr_afar(val, repr_methods):
    """Compute the repr of an object for IPython/Jupyter.

    We call this on a remote object.
    """
    if val is None:
        return None
    for method_name in repr_methods:
        method = getattr(val, method_name, None)
        if method is None:
            continue
        if method_name == "_ipython_display_":
            # Custom display!  Send the object to the client
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
            if rv is None:
                continue
            if method_name == "_repr_mimebundle_":
                if not isinstance(rv, (dict, tuple)):
                    continue
            elif not isinstance(rv, str):
                continue
            return rv, method_name, False
    return repr(val), "__repr__", False


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


def display_repr(results, out=None):
    """Display results from `repr_afar` locally in IPython/Jupyter"""
    val, method_name, is_exception = results
    if is_exception:
        if out is None:
            print(val, file=sys.stderr)
        else:
            out.append_stderr(val)
        return
    if val is None and method_name is None:
        return

    from IPython.display import display

    if method_name == "_ipython_display_":
        if out is None:
            display(val)
        else:
            out.append_display_data(val)
    else:
        mimic = MimicRepr(val, method_name)
        if out is None:
            display(mimic)
        else:
            out.append_display_data(mimic)
