import dis
import inspect
import innerscope
from dask import distributed


_errors_to_locations = {}
try:
    remotely
except NameError as exc:
    _errors_to_locations[exc.args[0]] = "remotely"

try:
    locally
except NameError as exc:
    _errors_to_locations[exc.args[0]] = "locally"


class AfarException(Exception):
    """Used to explicitly indicate where and how to execute the code of a context"""


class Where:
    def __init__(self, where, submit_kwargs=None):
        self.where = where
        self.submit_kwargs = submit_kwargs

    def __enter__(self):
        raise AfarException(self)

    def __exit__(self, exc_type, exc_value, exc_traceback):  # pragma: no cover
        return False

    def __call__(self, **submit_kwargs):
        return Where(self.where, submit_kwargs)


remotely = Where("remotely")
locally = Where("locally")


class Run:
    _gather_data = False

    def __init__(self, *names, data=None):
        self.names = names
        self.data = data
        # afar.run can be used as a singleton without calling it.
        # If we do this, we shouldn't keep data around.
        self._is_singleton = data is None
        self._frame = None
        # For now, save the following to help debug
        self._where = None
        self._scoped = None
        self._with_lineno = None

    def __call__(self, *names, data=None):
        if data is None:
            if self.data is None:
                data = {}
            else:
                data = self.data
        return type(self)(*names, data=data)

    def __enter__(self):
        self._frame = inspect.currentframe().f_back
        self._with_lineno = self._frame.f_lineno
        if self._is_singleton:
            if self.data is not None:
                raise RuntimeError("uh oh!")
            self.data = {}
        return self.data

    def __exit__(self, exc_type, exc_value, exc_traceback):
        try:
            return self._exit(exc_type, exc_value, exc_traceback)
        finally:
            self._frame = None
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
        elif issubclass(exc_type, NameError) and exc_value.args[0] in _errors_to_locations:
            self._where = _errors_to_locations[exc_value.args[0]]
            submit_kwargs = {}
        else:
            # The exception is valid
            return False

        # What line does the context end?
        for offset, line in dis.findlinestarts(frame.f_code):
            if offset > frame.f_lasti:
                endline = line
                break
        else:
            endline = line + 1

        # What line does the context begin?
        offset = -1
        it = dis.findlinestarts(frame.f_code)
        while offset <= exc_traceback.tb_lasti:
            offset, startline = next(it)
        while True:
            while startline <= self._with_lineno:
                offset, startline = next(it)
            line = startline
            while line < endline and line > self._with_lineno:
                try:
                    offset, line = next(it)
                except StopIteration:
                    line = endline
                    break
            if line <= self._with_lineno:
                startline = line
                continue
            break

        # Create a new function from the code block of the context.
        # For now, we require that the source code is available.
        # There may be a more reliable way to get the context block,
        # but let's see how far this can take us!
        lines = inspect.findsource(frame)[0][startline - 1 : endline - 1]  # why -1 for each limit?
        source = "def _magic_function_():\n" + "".join(lines)
        c = compile(
            source,
            frame.f_code.co_filename,
            "exec",
        )
        d = {}
        exec(c, frame.f_globals, d)
        self._func = d["_magic_function_"]

        # If no variable names were given, only get the last assignment
        names = self.names
        if not names:
            for inst in list(dis.get_instructions(self._func)):
                if inst.opname in {"STORE_NAME", "STORE_FAST", "STORE_DEREF", "STORE_GLOBAL"}:
                    names = (inst.argval,)

        # Use innerscope!  We only keep the globals, locals, and closures we need.
        self._scoped = innerscope.scoped_function(self._func, self.data)
        if self._scoped.missing:
            # Gather the necessary closures and locals
            f_locals = frame.f_locals
            update = {key: f_locals[key] for key in self._scoped.missing if key in f_locals}
            self._scoped = self._scoped.bind(update)

        if self._where == "remotely":
            # Submit to dask.distributed!  First, separate the Futures.
            futures = {
                key: val
                for key, val in self._scoped.outer_scope.items()
                if isinstance(val, distributed.Future)
            }
            for key in futures:
                del self._scoped.outer_scope[key]

            client = distributed.client._get_global_client()
            remote_dict = client.submit(afar_run, self._scoped, names, futures, **submit_kwargs)
            if self._gather_data:
                futures_to_name = {
                    client.submit(afar_get, remote_dict, name, **submit_kwargs): name
                    for name in names
                }
                for future, result in distributed.as_completed(futures_to_name, with_results=True):
                    self.data[futures_to_name[future]] = result
            else:
                for name in names:
                    self.data[name] = client.submit(afar_get, remote_dict, name, **submit_kwargs)
        else:
            # Run locally.  This is handy for testing and debugging.
            results = self._scoped()
            for name in names:
                self.data[name] = results[name]

        # Try to update the variables in the frame.
        # This currently only works if f_locals is f_globals, or if tracing (don't ask).
        frame.f_locals.update((name, self.data[name]) for name in names)
        return True


class Get(Run):
    """Unlike ``run``, ``get`` automatically gathers the data locally"""

    _gather_data = True


def afar_run(sfunc, names, futures):
    sfunc = sfunc.bind(futures)
    results = sfunc()
    return {key: results[key] for key in names}


def afar_get(d, k):
    return d[k]


run = Run()
get = Get()
