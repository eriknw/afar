"""Classes used to capture print statements within a Dask task."""
import builtins
import sys
from io import StringIO
from threading import Lock, local

from dask.distributed import get_worker


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

    def __init__(self, channel, key):
        self.channel = channel
        self.key = key

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
            file = StringIO()
            stream_name = "stdout"
        elif file is sys.stderr:
            file = StringIO()
            stream_name = "stderr"
        else:
            stream_name = None
        LocalPrint.printer(*args, **kwargs, file=file)
        if stream_name is not None:
            try:
                worker = get_worker()
            except ValueError:
                pass
            else:
                worker.log_event(self.channel, (self.key, stream_name, file.getvalue()))
            # Print locally too
            stream = sys.stdout if stream_name == "stdout" else sys.stderr
            LocalPrint.printer(file.getvalue(), end="", file=stream)
