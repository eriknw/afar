import dis
import inspect
import innerscope


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
    """Used to explicitly indicates where and how to execute the code of a context"""


class Where:
    def __init__(self, where):
        self.where = where

    def __enter__(self):
        raise AfarException(self.where)

    def __exit__(self, exc_type, exc_value, exc_traceback):  # pragma: no cover
        return False


remotely = Where("remotely")
locally = Where("locally")


class run:
    def __enter__(self):
        self._frame = inspect.currentframe().f_back
        self._with_lineno = self._frame.f_lineno
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        frame = self._frame
        self._frame = None
        if exc_type is None or exc_traceback.tb_frame is not frame:
            return False

        if issubclass(exc_type, AfarException):
            self._where = exc_value.args[0]
        elif issubclass(exc_type, NameError) and exc_value.args[0] in _errors_to_locations:
            self._where = _errors_to_locations[exc_value.args[0]]
        else:
            # The exception is valid
            return False

        # What line does the context begin?
        endline = frame.f_lineno
        offset = -1
        it = dis.findlinestarts(frame.f_code)
        while offset <= exc_traceback.tb_lasti:
            offset, startline = next(it)
        while True:
            while startline <= self._with_lineno:
                offset, startline = next(it)
            line = startline
            while line < endline and line > self._with_lineno:
                offset, line = next(it)
            if line <= self._with_lineno:
                startline = line
                continue
            break

        # Create a new function from the code block of the context.
        # For now, we require that the source code is available.
        # There may be a more reliable way to get the context block,
        # but let's see how far this can take us!
        lines = inspect.getframeinfo(frame, endline - startline + 1)[3]
        source = "def _magic_function_():\n" + "".join(lines)
        c = compile(
            source,
            frame.f_code.co_filename,
            "exec",
        )
        d = {}
        exec(c, frame.f_globals, d)
        self._func = d["_magic_function_"]

        # Use innerscope!  We only keep the globals, locals, and closures we need.
        self._scoped = innerscope.scoped_function(self._func)
        if self._scoped.missing:
            # Gather the necessary closures and locals
            f_locals = frame.f_locals
            update = {key: f_locals[key] for key in self._scoped.missing if key in f_locals}
            self._scoped = self._scoped.bind(update)
        # For now, just run the function and set the attributes
        results = self._scoped()
        self.__dict__.update(results.inner_scope)
        return True
