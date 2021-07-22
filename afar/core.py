import dis
import inspect
import innerscope
from dask import distributed
from . import reprs


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
        lines, offset = inspect.findsource(self._frame)

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
        try:
            return self._exit(exc_type, exc_value, exc_traceback)
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
        elif issubclass(exc_type, NameError) and exc_value.args[0] in _errors_to_locations:
            self._where = _errors_to_locations[exc_value.args[0]]
            submit_kwargs = {}
        else:
            # The exception is valid
            return False

        # What line does the context end?
        maxline = self._body_start
        for offset, line in dis.findlinestarts(frame.f_code):
            if line > maxline:
                maxline = line
            if offset > frame.f_lasti:
                endline = maxline - 1
                break
        else:
            endline = maxline + 5  # give us some wiggle room

        self.context_body = get_body(self._lines[self._body_start : endline])
        self._magic_func, names, futures = abracadabra(self)
        display_expr = self._magic_func._display_expr

        if self._where == "remotely":
            client = distributed.client._get_global_client()
            remote_dict = client.submit(run_afar, self._magic_func, names, futures, **submit_kwargs)
            if display_expr:
                repr_val = client.submit(
                    reprs.repr_afar,
                    client.submit(get_afar, remote_dict, "_afar_return_value_"),
                    self._magic_func._repr_methods,
                )
            if self._gather_data:
                futures_to_name = {
                    client.submit(get_afar, remote_dict, name, **submit_kwargs): name
                    for name in names
                }
                del remote_dict  # Let go ASAP
                for future, result in distributed.as_completed(futures_to_name, with_results=True):
                    self.data[futures_to_name[future]] = result
            else:
                for name in names:
                    self.data[name] = client.submit(get_afar, remote_dict, name, **submit_kwargs)
                del remote_dict  # Let go ASAP
            if display_expr:
                reprs.display_repr(repr_val.result())  # This blocks!
        elif self._where == "locally":
            # Run locally.  This is handy for testing and debugging.
            results = self._magic_func()
            for name in names:
                self.data[name] = results[name]
            if display_expr:
                reprs.IPython.display.display(results.return_value)
        elif self._where == "later":
            return True
        else:
            raise ValueError(f"Don't know where {self._where!r} is")

        # Try to update the variables in the frame.
        # This currently only works if f_locals is f_globals, or if tracing (don't ask).
        frame.f_locals.update((name, self.data[name]) for name in names)
        return True


class Get(Run):
    """Unlike ``run``, ``get`` automatically gathers the data locally"""

    _gather_data = True


def abracadabra(runner):
    # Create a new function from the code block of the context.
    # For now, we require that the source code is available.
    source = "def _afar_magic_():\n" + "".join(runner.context_body)
    func, display_expr = create_func(source, runner._frame.f_globals, reprs.in_ipython())

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


def run_afar(magic_func, names, futures):
    sfunc = magic_func._scoped.bind(futures)
    results = sfunc()
    rv = {key: results[key] for key in names}
    if magic_func._display_expr:
        rv["_afar_return_value_"] = results.return_value
    return rv


def get_afar(d, k):
    return d[k]


run = Run()
get = Get()
