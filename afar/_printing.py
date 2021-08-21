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


class RecordPrint:
    n = 0
    local_print = LocalPrint()
    print_lock = Lock()

    def __init__(self):
        self.stdout = StringIO()
        self.stderr = StringIO()

    def __enter__(self):
        with self.print_lock:
            if RecordPrint.n == 0:
                LocalPrint.printer = builtins.print
                builtins.print = self.local_print
            RecordPrint.n += 1
        self.local_print.printer = self
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        with self.print_lock:
            RecordPrint.n -= 1
            if RecordPrint.n == 0:
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
    stdout_val = stdout_future.result()
    stderr_val = stderr_future.result()
    if repr_future is not None:
        repr_val = repr_future.result()
    else:
        repr_val = None
    out.clear_output()
    if stdout_val or stderr_val or repr_val is not None:
        with out:
            if stdout_val:
                print(stdout_val, end="")
            if stderr_val:
                print(stderr_val, end="", file=sys.stderr)
            if repr_val is not None:
                display_repr(repr_val)
