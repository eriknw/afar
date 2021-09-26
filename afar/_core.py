import dis
import sys
from functools import partial
from inspect import currentframe, findsource
from uuid import uuid4
from weakref import WeakKeyDictionary, WeakSet

from dask import distributed
from dask.distributed import get_worker

from ._abra import cadabra
from ._printing import PrintRecorder
from ._reprs import display_repr, repr_afar
from ._utils import is_kernel, supports_async_output
from ._where import find_where


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
    # Used to update outputs asynchronously
    _outputs = {}

    def __init__(self, *names, client=None, data=None):
        self.names = names
        self.data = data
        self.client = client
        self.context_body = None
        # afar.run can be used as a singleton without calling it.
        # If we do this, we shouldn't keep data around.
        self._is_singleton = data is None
        self._frame = None
        # Used to cancel work
        self._client_to_futures = WeakKeyDictionary()
        # For now, save the following to help debug
        self._where = None
        self._magic_func = None
        self._body_start = None
        self._lines = None

    def __call__(self, *names, client=None, data=None):
        if data is None:
            if self.data is None:
                data = {}
            else:
                data = self.data
        if client is None:
            client = self.client
        return type(self)(*names, client=client, data=data)

    def __enter__(self):
        self._frame = currentframe().f_back
        with_lineno = self._frame.f_lineno - 1
        if self._is_singleton:
            if self.data:
                raise RuntimeError("uh oh!")
            self.data = {}
        try:
            lines, offset = findsource(self._frame)
        except OSError:
            # Try to fine the source if we are in %%time or %%timeit magic
            if self._frame.f_code.co_filename in {"<timed exec>", "<magic-timeit>"} and is_kernel():
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
        if self.data is None:
            if exc_type is None:
                raise RuntimeError("uh oh!")
            return False
        if exc_type is None or exc_traceback.tb_frame is not self._frame:
            return False
        where = find_where(exc_type, exc_value)
        if where is None:
            # The exception is valid
            return False

        try:
            return self._exit(where)
        except KeyboardInterrupt as exc:
            # Cancel all pending tasks
            if self._where == "remotely":
                self.cancel()
            raise exc from None
        except Exception as exc:
            raise exc from None
        finally:
            self._frame = None
            self._lines = None
            if self._is_singleton:
                self.data = None

    def _exit(self, where):
        frame = self._frame
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

        context_body = get_body(self._lines[self._body_start : endline])
        self._run(
            where.where,
            context_body,
            self.names,
            self.data,
            client=self.client or where.client,
            submit_kwargs=where.submit_kwargs,
            global_ns=frame.f_globals,
            local_ns=frame.f_locals,
        )
        return True

    def _run(
        self,
        where,
        context_body,
        names,
        data,
        *,
        global_ns,
        local_ns,
        client=None,
        submit_kwargs=None,
        return_expr=False,
    ):
        self._where = where
        self.context_body = context_body
        if submit_kwargs is None:
            submit_kwargs = {}

        self._magic_func, names, futures = cadabra(
            context_body, where, names, data, global_ns, local_ns
        )
        display_expr = self._magic_func._display_expr
        return_future = None

        if where == "remotely":
            if client is None:
                client = distributed.client._get_global_client()
                if client is None:
                    raise TypeError(
                        "No dask.distributed client found.  "
                        "You must create and connect to a Dask cluster before using afar."
                    )
            if client not in self._client_to_futures:
                weak_futures = WeakSet()
                self._client_to_futures[client] = weak_futures
            else:
                weak_futures = self._client_to_futures[client]

            has_print = "print" in self._magic_func._scoped.builtin_names
            capture_print = (
                self._gather_data  # we're blocking anyway to gather data
                or display_expr  # we need to display an expression (sync or async)
                or has_print  # print is in the context body
                or supports_async_output()  # no need to block, so why not?
            )

            to_scatter = data.keys() & self._magic_func._scoped.outer_scope.keys()
            if to_scatter:
                # Scatter value in `data` that we need in this calculation.
                # This moves data from local to remote, then keeps it remote.
                # Things in `data` may get reused, so it can be helpful to
                # move them.  We could move everything in `data`, but we
                # only move the things we need.  We could also scatter everything
                # in `self._magic_func._scoped.outer_scope`, but we can't reuse
                # them, because they may get modified locally.
                to_scatter = list(to_scatter)
                # I'm afraid to hash, because users may accidentally mutate things.
                scattered = client.scatter([data[key] for key in to_scatter], hash=False)
                scattered = dict(zip(to_scatter, scattered))
                futures.update(scattered)
                data.update(scattered)
                for key in to_scatter:
                    del self._magic_func._scoped.outer_scope[key]

            if capture_print and "afar-print" not in client._event_handlers:
                client.subscribe_topic("afar-print", self._handle_print)
            async_print = capture_print and supports_async_output()
            if capture_print:
                unique_key = uuid4().hex
                self._setup_print(unique_key, async_print)
            else:
                unique_key = None

            # Scatter magic_func to avoid "Large object" UserWarning
            magic_func = client.scatter(self._magic_func, hash=False)
            weak_futures.add(magic_func)

            remote_dict = client.submit(
                run_afar,
                magic_func,
                names,
                futures,
                capture_print,
                unique_key,
                pure=False,
                **submit_kwargs,
            )
            weak_futures.add(remote_dict)
            magic_func.release()  # Let go ASAP
            if display_expr:
                return_future = client.submit(get_afar, remote_dict, "_afar_return_value_")
                repr_future = client.submit(
                    repr_afar,
                    return_future,
                    self._magic_func._repr_methods,
                )
                weak_futures.add(repr_future)
                if return_expr:
                    weak_futures.add(return_future)
                else:
                    return_future.release()  # Let go ASAP
                    return_future = None
            else:
                repr_future = None

            if capture_print:
                obj = repr_future if display_expr else remote_dict
                obj.add_done_callback(
                    partial(self._finalize_print, self._outputs[unique_key], display_expr)
                )

            if self._gather_data:
                futures_to_name = {
                    client.submit(get_afar, remote_dict, name, **submit_kwargs): name
                    for name in names
                }
                weak_futures.update(futures_to_name)
                remote_dict.release()  # Let go ASAP
                for future, result in distributed.as_completed(futures_to_name, with_results=True):
                    data[futures_to_name[future]] = result
            else:
                for name in names:
                    future = client.submit(get_afar, remote_dict, name, **submit_kwargs)
                    weak_futures.add(future)
                    data[name] = future
                remote_dict.release()  # Let go ASAP
        elif where == "locally":
            # Run locally.  This is handy for testing and debugging.
            results = self._magic_func()
            for name in names:
                data[name] = results[name]
            if display_expr:
                from IPython.dislpay import display

                display(results.return_value)
                if return_expr:
                    return_future = results.return_value
        elif where == "later":
            return
        else:
            raise ValueError(f"Don't know where {where!r} is")

        # Try to update the variables in the frame.
        # This currently only works if f_locals is f_globals, or if tracing (don't ask).
        local_ns.update((name, data[name]) for name in names)
        return return_future

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

    def _setup_print(self, key, async_print):
        if async_print:
            from IPython.display import display
            from ipywidgets import Output

            out = Output()
            display(out)
            out.append_stdout("\N{SPARKLES} Running afar... \N{SPARKLES}")
        else:
            out = None
        self._outputs[key] = [out, False]  # False means has not been updated

    @classmethod
    def _handle_print(cls, event):
        # XXX: can we assume all messages from a single task arrive in FIFO order?
        _, msg = event
        key, stream_name, string = msg
        out, is_updated = cls._outputs[key]
        if out is not None:
            if not is_updated:
                # Clear the "Running afar..." message
                out.outputs = type(out.outputs)()
                cls._outputs[key][1] = True  # is updated
            if stream_name == "stdout":
                out.append_stdout(string)
            elif stream_name == "stderr":
                out.append_stderr(string)
        elif stream_name == "stdout":
            print(string, end="")
        elif stream_name == "stderr":
            print(string, end="", file=sys.stderr)
        if stream_name == "finish":
            del cls._outputs[key]

    def _finalize_print(self, info, display_expr, future):
        # Can we move this to `_handle_print`?
        # _handle_print for this key may get called *after* _finalize_print
        out, is_updated = info
        if out is not None and not is_updated:
            # Clear the "Running afar..." message to indicate it's finished
            # out.clear_output()  # Not thread-safe!
            # See: https://github.com/jupyter-widgets/ipywidgets/issues/3260
            out.outputs = type(out.outputs)()  # current workaround
            info[1] = True  # is updated
        if display_expr:
            repr_val = future.result()
            if repr_val is not None:
                display_repr(repr_val, out=out)


class Get(Run):
    """Unlike ``run``, ``get`` automatically gathers the data locally"""

    _gather_data = True


def run_afar(magic_func, names, futures, capture_print, unique_key):
    try:
        if capture_print:
            rec = PrintRecorder(unique_key)
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
    finally:
        if capture_print:
            try:
                worker = get_worker()
            except ValueError:
                pass
            else:
                worker.log_event("afar-print", (unique_key, "finish", None))
    return rv


def get_afar(d, k):
    return d[k]


run = Run()
get = Get()
