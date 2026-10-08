"""Define the user-facing `run` object; this is where it all comes together."""

import sys
from functools import partial
from inspect import currentframe
from uuid import uuid4
from weakref import WeakKeyDictionary, WeakSet

from dask import distributed
from dask.distributed import get_worker

from ._abra import cadabra
from ._inspect import analyze_with, find_with_start, get_lines
from ._printing import PrintRecorder
from ._reprs import display_repr, repr_afar
from ._utils import supports_async_output
from ._where import find_where


class Run:
    _gather_data = False
    # Used to update outputs asynchronously
    _outputs = {}
    # Futures of remote contexts that have not finished; see `_run`
    _running = set()
    _channel = "afar-" + uuid4().hex

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
        self._body_end = None
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
        frame = currentframe().f_back
        if self._is_singleton and self.data:
            raise RuntimeError("uh oh!")

        lines = get_lines(frame)
        with_start = find_with_start(lines, frame.f_lineno)
        num_with, body_start, body_end = analyze_with(lines, with_start)
        if num_with < 2:
            # Best effort detection.  This fails if there is a context *before* afar.run
            within = type(self).__name__.lower()
            raise RuntimeError(
                f"`afar.{within}` is missing a location.  For example:\n\n"
                f">>> with afar.{within}, remotely:\n"
                f"...     pass\n\n"
                f"Please specify a location such as adding `, remotely`."
            )
        # Keep state only once the context can run, so a failed `__enter__` leaves none behind
        self._frame = frame
        if self._is_singleton:
            self.data = {}
        self._body_start = body_start
        self._body_end = body_end
        self._lines = lines
        return self.data

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._where = None
        if self.data is None:
            if exc_type is None:
                raise RuntimeError("uh oh!")
            return False
        where = None
        if exc_type is not None and exc_traceback.tb_frame is self._frame:
            where = find_where(exc_type, exc_value)
        try:
            if where is None:
                # No exception, or a real one: let it propagate, but still reset the state
                return False
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
        context_body = self._lines[self._body_start : self._body_end]
        self._run(
            where.where,
            context_body,
            self.names,
            self.data,
            client=self.client or where.client,
            submit_kwargs=where.submit_kwargs,
            global_ns=frame.f_globals,
            local_ns=frame.f_locals,
            free_names=frame.f_code.co_freevars,
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
        free_names=(),
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
                scattered = dict(zip(to_scatter, scattered, strict=True))
                futures.update(scattered)
                data.update(scattered)
                for key in to_scatter:
                    del self._magic_func._scoped.outer_scope[key]

            capture_print = True
            if capture_print and self._channel not in client._event_handlers:
                client.subscribe_topic(self._channel, self._handle_print)
                # When would be a good time to unsubscribe?
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
                self._channel,
                unique_key,
                return_expr and display_expr,
                pure=False,
                **submit_kwargs,
            )
            weak_futures.add(remote_dict)
            # Hold `remote_dict` until it finishes, then let go.  (Calling `release` instead
            # cancels it if no futures depend on it, as when a context only prints.)
            self._running.add(remote_dict)
            remote_dict.add_done_callback(partial(self._handle_done, unique_key))
            magic_func.release()  # Let go ASAP

            if self._gather_data:
                futures_to_name = {
                    client.submit(get_afar, remote_dict, name, **submit_kwargs): name
                    for name in names
                }
                weak_futures.update(futures_to_name)
                for future, result in distributed.as_completed(futures_to_name, with_results=True):
                    data[futures_to_name[future]] = result
            else:
                for name in names:
                    future = client.submit(get_afar, remote_dict, name, **submit_kwargs)
                    weak_futures.add(future)
                    data[name] = future
            if return_expr and display_expr:
                # The line magic, as in `z = %afar x + y`, returns the final expression
                return_future = client.submit(
                    get_afar, remote_dict, RETURN_VALUE_KEY, **submit_kwargs
                )
                weak_futures.add(return_future)
                if self._gather_data:
                    return_future = return_future.result()
        elif where == "locally":
            # Run locally.  This is handy for testing and debugging.
            results = self._magic_func()
            for name in names:
                data[name] = results[name]
            if display_expr:
                if results.return_value is not None and not self._magic_func._quiet:
                    # As remotely (see `repr_afar`) and as IPython does, don't display None
                    from IPython.display import display

                    display(results.return_value)
                if return_expr:
                    return_future = results.return_value
        elif where == "later":
            return
        else:
            raise ValueError(f"Don't know where {where!r} is")

        # Try to update the variables in the frame.  This works if f_locals is f_globals
        # (module scope and IPython), or in functions since Python 3.13 (PEP 667), where
        # f_locals is a write-through proxy whose `update` requires a dict.  The proxy also
        # writes to variables of enclosing functions, which a function may rebind only with
        # `nonlocal`, and then the body binds them too.  Leave the others alone.
        inner_names = self._magic_func._scoped.inner_names
        local_ns.update(
            {name: data[name] for name in names if name not in free_names or name in inner_names}
        )
        return return_future

    def cancel(self, *, client=None, force=False):
        """Cancel pending tasks"""
        if client is not None:
            items = [(client, self._client_to_futures[client])]
        else:
            items = self._client_to_futures.items()
        for cur_client, weak_futures in items:
            cur_client.cancel(
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
        key, action, payload = msg
        entry = cls._outputs.get(key)
        if entry is None:
            return
        out, is_updated = entry
        if out is not None:
            if action == "begin":
                if is_updated:
                    out.outputs = type(out.outputs)()
                    out.append_stdout("\N{SPARKLES} Running afar... (restarted) \N{SPARKLES}")
                    entry[1] = False  # is not updated
            else:
                if not is_updated:
                    # Clear the "Running afar..." message
                    out.outputs = type(out.outputs)()
                    entry[1] = True  # is updated
                # ipywidgets.Output is pretty slow if there are lots of messages
                if action == "stdout":
                    out.append_stdout(payload)
                elif action == "stderr":
                    out.append_stderr(payload)
        elif action == "stdout":
            print(payload, end="")  # noqa: T201
        elif action == "stderr":
            print(payload, end="", file=sys.stderr)  # noqa: T201
        if action == "display_expr":
            display_repr(payload, out=out)
            cls._outputs.pop(key, None)
        elif action == "finish":
            cls._outputs.pop(key, None)

    @classmethod
    def _handle_done(cls, key, future):
        cls._running.discard(future)
        if future.status == "cancelled":
            # A task cancelled before it runs never sends "finish", so stop waiting for it
            entry = cls._outputs.pop(key, None)
            if entry is not None and entry[0] is not None:
                out = entry[0]
                out.outputs = type(out.outputs)()
                out.append_stderr("afar was cancelled\n")


class Get(Run):
    """Unlike ``run``, ``get`` automatically gathers the data locally"""

    _gather_data = True


def run_afar(magic_func, names, futures, capture_print, channel, unique_key, return_expr=False):
    worker = None
    send_finish = False
    if capture_print:
        try:
            worker = get_worker()
            send_finish = True
        except ValueError:
            pass
    try:
        if capture_print and worker is not None:
            worker.log_event(channel, (unique_key, "begin", None))
            rec = PrintRecorder(channel, unique_key)
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
        if return_expr:
            rv[RETURN_VALUE_KEY] = results.return_value

        if magic_func._display_expr and not magic_func._quiet and worker is not None:
            # Hopefully computing the repr is fast.  If it is slow, perhaps it would be
            # better to add the return value to rv and call repr_afar as a separate task.
            # Also, pretty_repr must be msgpack serializable if done via events.  Hence,
            # custom _ipython_display_ doesn't work, and we resort to using a basic repr.
            pretty_repr = repr_afar(results.return_value, magic_func._repr_methods)
            if pretty_repr is not None:
                worker.log_event(channel, (unique_key, "display_expr", pretty_repr))
                send_finish = False
    finally:
        if capture_print and worker is not None and send_finish:
            worker.log_event(channel, (unique_key, "finish", None))
    return rv


# Not a valid identifier, so it cannot be the name of a variable
RETURN_VALUE_KEY = "afar return value"


def get_afar(d, k):
    return d[k]


run = Run()
get = Get()
