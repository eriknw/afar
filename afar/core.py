import builtins
import dis
import inspect
import io
import sys
import threading
import weakref
from functools import partial

import innerscope
from dask import distributed

from . import reprs


def _supports_async_output():
    if reprs.is_kernel() and not reprs.in_terminal():
        try:
            import ipywidgets  # noqa
        except ImportError:
            return False
        return True
    return False


_errors_to_locations = {}
try:
    remotely
except NameError as exc:
    _errors_to_locations[exc.args[0]] = "remotely"

try:
    locally
except NameError as exc:
    _errors_to_locations[exc.args[0]] = "locally"

try:
    later
except NameError as exc:
    _errors_to_locations[exc.args[0]] = "later"


class AfarException(Exception):
    """Used to explicitly indicate where and how to execute the code of a context"""


class Where:
    def __init__(self, where, client=None, submit_kwargs=None):
        self.where = where
        self.client = client
        self.submit_kwargs = submit_kwargs

    def __enter__(self):
        raise AfarException(self)

    def __exit__(self, exc_type, exc_value, exc_traceback):  # pragma: no cover
        return False

    def __call__(self, client=None, **submit_kwargs):
        return Where(self.where, client, submit_kwargs)


remotely = Where("remotely")
locally = Where("locally")
later = Where("later")


def get_body_start(lines, with_start):
    line = lines[with_start]
    stripped = line.lstrip()
    body = line[: len(line) - len(stripped)] + " pass\n"
    body *= 2
    with_lines = [stripped]
    try:
        code = compile(stripped, "<exec>", "exec")
    except Exception:
        pass
    else:
        raise RuntimeError(
            "Failed to analyze the context!  When using afar, "
            "please put the context body on a new line."
        )
    for i, line in enumerate(lines[with_start:]):
        if i > 0:
            with_lines.append(line)
        if ":" in line:
            source = "".join(with_lines) + body
            try:
                code = compile(source, "<exec>", "exec")
            except Exception:
                pass
            else:
                num_with = code.co_code.count(dis.opmap["SETUP_WITH"])
                body_start = with_start + i + 1
                return num_with, body_start
    raise RuntimeError("Failed to analyze the context!")


def get_body(lines):
    head = "def f():\n with x:\n  "
    tail = " pass\n pass\n"
    while lines:
        source = head + "  ".join(lines) + tail
        try:
            compile(source, "<exec>", "exec")
        except Exception:
            lines.pop()
        else:
            return lines
    raise RuntimeError("Failed to analyze the context body!")


class Run:
    _gather_data = False

    def __init__(self, *names, data=None):
        self.names = names
        self.data = data
        self.context_body = None
        # afar.run can be used as a singleton without calling it.
        # If we do this, we shouldn't keep data around.
        self._is_singleton = data is None
        self._frame = None
        # Used to cancel work
        self._client_to_futures = weakref.WeakKeyDictionary()
        # For now, save the following to help debug
        self._where = None
        self._magic_func = None
        self._body_start = None
        self._lines = None

    def __call__(self, *names, data=None):
        if data is None:
            if self.data is None:
                data = {}
            else:
                data = self.data
        return type(self)(*names, data=data)

    def __enter__(self):
        self._frame = inspect.currentframe().f_back
        with_lineno = self._frame.f_lineno - 1
        if self._is_singleton:
            if self.data:
                raise RuntimeError("uh oh!")
            self.data = {}
        try:
            lines, offset = inspect.findsource(self._frame)
        except OSError:
            # Try to fine the source if we are in %%time or %%timeit magic
            if (
                self._frame.f_code.co_filename in {"<timed exec>", "<magic-timeit>"}
                and reprs.is_kernel()
            ):
                from IPython import get_ipython

                ip = get_ipython()
                if ip is None:
                    raise
                cell = ip.history_manager._i00  # The current cell!
                lines = cell.splitlines(keepends=True)
                # strip the magic
                for i, line in enumerate(lines):
                    if line.strip().startswith("%%time"):
                        lines = lines[i + 1 :]
                        break
                else:
                    raise
                # strip blank lines
                for i, line in enumerate(lines):
                    if line.strip():
                        if i:
                            lines = lines[i:]
                        lines[-1] += "\n"
                        break
                else:
                    raise
            else:
                raise

        while not lines[with_lineno].lstrip().startswith("with"):
            with_lineno -= 1
            if with_lineno < 0:
                raise RuntimeError("Failed to analyze the context!")

        num_with, body_start = get_body_start(lines, with_lineno)
        if num_with < 2:
            # Best effort detection.  This fails if there is a context *before* afar.run
            within = type(self).__name__.lower()
            raise RuntimeError(
                f"`afar.{within}` is missing a location.  For example:\n\n"
                f">>> with afar.{within}, remotely:\n"
                f"...     pass\n\n"
                f"Please specify a location such as adding `, remotely`."
            )
        self._body_start = body_start
        self._lines = lines
        return self.data

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._where = None
        try:
            return self._exit(exc_type, exc_value, exc_traceback)
        except KeyboardInterrupt:
            # Cancel all pending tasks
            if self._where == "remotely":
                self.cancel()
            raise
        finally:
            self._frame = None
            self._lines = None
            if self._is_singleton:
                self.data = None

    def _exit(self, exc_type, exc_value, exc_traceback):
        frame = self._frame
        if self.data is None:
            if exc_type is None:
                raise RuntimeError("uh oh!")
            return False
        if exc_type is None or exc_traceback.tb_frame is not frame:
            return False

        if issubclass(exc_type, AfarException):
            where = exc_value.args[0]
            self._where = where.where
            submit_kwargs = where.submit_kwargs or {}
            client = where.client
        elif issubclass(exc_type, NameError) and exc_value.args[0] in _errors_to_locations:
            self._where = _errors_to_locations[exc_value.args[0]]
            submit_kwargs = {}
            client = None
        else:
            # The exception is valid
            return False

        # What line does the context end?
        maxline = self._body_start
        for offset, line in dis.findlinestarts(frame.f_code):
            if line > maxline:
                maxline = line
            if offset > frame.f_lasti:
                endline = max(maxline, maxline - 1)
                break
        else:
            endline = maxline + 5  # give us some wiggle room

        self.context_body = get_body(self._lines[self._body_start : endline])
        self._magic_func, names, futures = abracadabra(self)
        display_expr = self._magic_func._display_expr

        if self._where == "remotely":
            if client is None:
                client = distributed.client._get_global_client()
            if client not in self._client_to_futures:
                weak_futures = weakref.WeakSet()
                self._client_to_futures[client] = weak_futures
            else:
                weak_futures = self._client_to_futures[client]

            has_print = "print" in self._magic_func._scoped.builtin_names
            capture_print = (
                self._gather_data  # we're blocking anyway to gather data
                or display_expr  # we need to display an expression (sync or async)
                or has_print  # print is in the context body
                or _supports_async_output()  # no need to block, so why not?
            )

            to_scatter = self.data.keys() & self._magic_func._scoped.outer_scope.keys()
            if to_scatter:
                # Scatter value in `self.data` that we need in this calculation.
                # This moves data from local to remote, then keeps it remote.
                # Things in `self.data` may get reused, so it can be helpful to
                # move them.  We could move everything in `self.data`, but we
                # only move the things we need.  We could also scatter everything
                # in `self._magic_func._scoped.outer_scope`, but we can't reuse
                # them, because they may get modified locally.
                to_scatter = list(to_scatter)
                # I'm afraid to hash, because users may accidentally mutate things.
                scattered = client.scatter([self.data[key] for key in to_scatter], hash=False)
                scattered = dict(zip(to_scatter, scattered))
                futures.update(scattered)
                self.data.update(scattered)
                for key in to_scatter:
                    del self._magic_func._scoped.outer_scope[key]
            # Scatter magic_func to avoid "Large object" UserWarning
            magic_func = client.scatter(self._magic_func)
            weak_futures.add(magic_func)
            remote_dict = client.submit(
                run_afar, magic_func, names, futures, capture_print, pure=False, **submit_kwargs
            )
            weak_futures.add(remote_dict)
            magic_func.release()  # Let go ASAP
            if display_expr:
                repr_future = client.submit(
                    reprs.repr_afar,
                    client.submit(get_afar, remote_dict, "_afar_return_value_"),
                    self._magic_func._repr_methods,
                )
                weak_futures.add(repr_future)
            else:
                repr_future = None
            if capture_print:
                stdout_future = client.submit(get_afar, remote_dict, "_afar_stdout_")
                weak_futures.add(stdout_future)
                stderr_future = client.submit(get_afar, remote_dict, "_afar_stderr_")
                weak_futures.add(stderr_future)
            if self._gather_data:
                futures_to_name = {
                    client.submit(get_afar, remote_dict, name, **submit_kwargs): name
                    for name in names
                }
                weak_futures.update(futures_to_name)
                remote_dict.release()  # Let go ASAP
                for future, result in distributed.as_completed(futures_to_name, with_results=True):
                    self.data[futures_to_name[future]] = result
            else:
                for name in names:
                    future = client.submit(get_afar, remote_dict, name, **submit_kwargs)
                    weak_futures.add(future)
                    self.data[name] = future
                remote_dict.release()  # Let go ASAP

            if capture_print and _supports_async_output():
                # Display in `out` cell when data is ready: non-blocking
                from IPython.display import display
                from ipywidgets import Output

                out = Output()
                display(out)
                # Can we show `distributed.progress` right here?
                stdout_future.add_done_callback(
                    partial(_display_outputs, out, stderr_future, repr_future)
                )
            elif capture_print:
                # blocks!
                stdout_val = stdout_future.result()
                stdout_future.release()
                if stdout_val:
                    print(stdout_val, end="")
                stderr_val = stderr_future.result()
                stderr_future.release()
                if stderr_val:
                    print(stderr_val, end="", file=sys.stderr)
                if display_expr:
                    repr_val = repr_future.result()
                    repr_future.release()
                    if repr_val is not None:
                        reprs.display_repr(repr_val)
        elif self._where == "locally":
            # Run locally.  This is handy for testing and debugging.
            results = self._magic_func()
            for name in names:
                self.data[name] = results[name]
            if display_expr:
                from IPython.dislpay import display

                display(results.return_value)
        elif self._where == "later":
            return True
        else:
            raise ValueError(f"Don't know where {self._where!r} is")

        # Try to update the variables in the frame.
        # This currently only works if f_locals is f_globals, or if tracing (don't ask).
        frame.f_locals.update((name, self.data[name]) for name in names)
        return True

    def cancel(self, *, client=None, force=False):
        """Cancel pending tasks"""
        if client is not None:
            items = [(client, self._client_to_futures[client])]
        else:
            items = self._client_to_futures.items()
        for client, weak_futures in items:
            client.cancel(
                [future for future in weak_futures if future.status == "pending"], force=force
            )
            weak_futures.clear()


class Get(Run):
    """Unlike ``run``, ``get`` automatically gathers the data locally"""

    _gather_data = True


def _display_outputs(out, stderr_future, repr_future, stdout_future):
    stdout_val = stdout_future.result()
    stderr_val = stderr_future.result()
    if repr_future is not None:
        repr_val = repr_future.result()
    else:
        repr_val = None
    if stdout_val or stderr_val or repr_val is not None:
        with out:
            if stdout_val:
                print(stdout_val, end="")
            if stderr_val:
                print(stderr_val, end="", file=sys.stderr)
            if repr_val is not None:
                reprs.display_repr(repr_val)


def abracadabra(runner):
    # Create a new function from the code block of the context.
    # For now, we require that the source code is available.
    source = "def _afar_magic_():\n" + "".join(runner.context_body)
    func, display_expr = create_func(source, runner._frame.f_globals, reprs.is_kernel())

    # If no variable names were given, only get the last assignment
    names = runner.names
    if not names:
        for inst in list(dis.get_instructions(func)):
            if inst.opname in {"STORE_NAME", "STORE_FAST", "STORE_DEREF", "STORE_GLOBAL"}:
                names = (inst.argval,)

    # Use innerscope!  We only keep the globals, locals, and closures we need.
    scoped = innerscope.scoped_function(func, runner.data)
    if scoped.missing:
        # Gather the necessary closures and locals
        f_locals = runner._frame.f_locals
        update = {key: f_locals[key] for key in scoped.missing if key in f_locals}
        scoped = scoped.bind(update)

    if runner._where == "remotely":
        # Get ready to submit to dask.distributed by separating the Futures.
        futures = {
            key: val
            for key, val in scoped.outer_scope.items()
            if isinstance(val, distributed.Future)
            # TODO: what can/should we do if the future is in a bad state?
        }
        for key in futures:
            del scoped.outer_scope[key]
    else:
        futures = None
    magic_func = MagicFunction(source, scoped, display_expr)
    return magic_func, names, futures


def create_func(source, globals_dict, is_in_ipython):
    code = compile(
        source,
        "<afar>",
        "exec",
    )
    locals_dict = {}
    exec(code, globals_dict, locals_dict)
    func = locals_dict["_afar_magic_"]
    display_expr = is_in_ipython and reprs.endswith_expr(func)
    if display_expr:
        func = reprs.return_expr(func)
    return func, display_expr


class MagicFunction:
    def __init__(self, source, scoped, display_expr):
        self._source = source
        self._scoped = scoped
        self._display_expr = display_expr
        if display_expr:
            self._repr_methods = reprs.get_repr_methods()
        else:
            self._repr_methods = None

    def __call__(self):
        return self._scoped()

    def __getstate__(self):
        # Instead of trying to serialize the function we created with `compile` and `exec`,
        # let's save the source and recreate the function (and self._scoped) again.
        state = dict(self.__dict__)
        del state["_scoped"]
        state["outer_scope"] = self._scoped.outer_scope
        return state

    def __setstate__(self, state):
        outer_scope = state.pop("outer_scope")
        self.__dict__.update(state)
        func, _ = create_func(self._source, {}, self._display_expr)
        self._scoped = innerscope.scoped_function(func, outer_scope)


# Here's the plan: we'll capture all print statements to stdout and stderr
# on the current thread.  But, we need to leave the other threads alone!
# So, use `threading.local` and a lock for some ugly capturing.
class LocalPrint(threading.local):
    printer = None

    def __call__(self, *args, **kwargs):
        return self.printer(*args, **kwargs)


class RecordPrint:
    n = 0
    local_print = LocalPrint()
    print_lock = threading.Lock()

    def __init__(self):
        self.stdout = io.StringIO()
        self.stderr = io.StringIO()

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


def run_afar(magic_func, names, futures, capture_print):
    if capture_print:
        rec = RecordPrint()
        if "print" in magic_func._scoped.builtin_names and "print" not in futures:
            sfunc = magic_func._scoped.bind(futures, print=rec)
        else:
            sfunc = magic_func._scoped.bind(futures)
        with rec:
            results = sfunc()
    else:
        sfunc = magic_func._scoped.bind(futures)
        results = sfunc()

    rv = {key: results[key] for key in names}
    if magic_func._display_expr:
        rv["_afar_return_value_"] = results.return_value
    if capture_print:
        rv["_afar_stdout_"] = rec.stdout.getvalue()
        rv["_afar_stderr_"] = rec.stderr.getvalue()
    return rv


def get_afar(d, k):
    return d[k]


run = Run()
get = Get()
