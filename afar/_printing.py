import builtins
import sys
from io import StringIO
from threading import Lock, local

from ._reprs import display_repr


# Here's the plan: we'll capture all print statements to stdout and stderr
# on the current thread.  But, we need to leave the other threads alone!
# So, use `threading.local` and a lock for some ugly capturing.
class LocalPrint(local):
    printer = None

    def __call__(self, *args, **kwargs):
        return self.printer(*args, **kwargs)


class PrintRecorder:
    n = 0
    local_print = LocalPrint()
    print_lock = Lock()

    def __init__(self):
        self.stdout = StringIO()
        self.stderr = StringIO()

    def __enter__(self):
        with self.print_lock:
            if PrintRecorder.n == 0:
                LocalPrint.printer = builtins.print
                builtins.print = self.local_print
            PrintRecorder.n += 1
        self.local_print.printer = self
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        with self.print_lock:
            PrintRecorder.n -= 1
            if PrintRecorder.n == 0:
                builtins.print = LocalPrint.printer
        self.local_print.printer = LocalPrint.printer
        return False

    def __call__(self, *args, file=None, **kwargs):
        if file is None or file is sys.stdout:
            file = self.stdout
        elif file is sys.stderr:
            file = self.stderr
        LocalPrint.printer(*args, **kwargs, file=file)


def print_outputs(stdout_future, stderr_future, repr_future):
    """Print results to the user"""
    stdout_val = stdout_future.result()
    stdout_future.release()
    if stdout_val:
        print(stdout_val, end="")
    stderr_val = stderr_future.result()
    stderr_future.release()
    if stderr_val:
        print(stderr_val, end="", file=sys.stderr)
    if repr_future is not None:
        repr_val = repr_future.result()
        repr_future.release()
        if repr_val is not None:
            display_repr(repr_val)


def print_outputs_async(out, stderr_future, repr_future, stdout_future):
    """Display output streams and final expression to the user.

    This is used as a callback to `stdout_future`.
    """
    try:
        stdout_val = stdout_future.result()
        # out.clear_output()  # Not thread-safe!
        # See: https://github.com/jupyter-widgets/ipywidgets/issues/3260
        out.outputs = type(out.outputs)()  # current workaround
        if stdout_val:
            out.append_stdout(stdout_val)
        stderr_val = stderr_future.result()
        if stderr_val:
            out.append_stderr(stderr_val)
        if repr_future is not None:
            repr_val = repr_future.result()
            if repr_val is not None:
                display_repr(repr_val, out=out)
    except Exception as exc:
        print(exc, file=sys.stderr)
        raise
