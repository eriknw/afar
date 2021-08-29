import dis
from types import FunctionType

from dask.distributed import Future
from innerscope import scoped_function

from ._reprs import get_repr_methods
from ._utils import code_replace, is_kernel


def endswith_expr(func):
    """Does the function end with an expression (not assigment or return)"""
    code = func.__code__
    co_code = code.co_code
    return (
        len(co_code) > 6
        and co_code[-6] == dis.opmap["POP_TOP"]
        and co_code[-4] == dis.opmap["LOAD_CONST"]
        and co_code[-2] == dis.opmap["RETURN_VALUE"]
        and code.co_consts[co_code[-3]] is None
    )


def return_expr(func):
    """Create a new function from func that returns the final expression"""
    code = func.__code__
    # remove POP_TOP and LOAD_CONST (None)
    co_code = code.co_code[:-6] + code.co_code[-2:]
    code = code_replace(code, co_code=co_code)
    rv = FunctionType(
        code,
        func.__globals__,
        name=func.__name__,
        argdefs=func.__defaults__,
        closure=func.__closure__,
    )
    rv.__kwdefaults__ = func.__kwdefaults__
    return rv


def create_func(source, globals_dict, is_in_ipython):
    code = compile(
        source,
        "<afar>",
        "exec",
    )
    locals_dict = {}
    exec(code, globals_dict, locals_dict)
    func = locals_dict["_afar_magic_"]
    display_expr = is_in_ipython and endswith_expr(func)
    if display_expr:
        func = return_expr(func)
    return func, display_expr


class MagicFunction:
    def __init__(self, source, scoped, display_expr):
        self._source = source
        self._scoped = scoped
        self._display_expr = display_expr
        if display_expr:
            self._repr_methods = get_repr_methods()
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
        self._scoped = scoped_function(func, outer_scope)


def cadabra(context_body, where, names, data, global_ns, local_ns):
    # Create a new function from the code block of the context.
    # For now, we require that the source code is available.
    source = "def _afar_magic_():\n" + "".join(context_body)
    func, display_expr = create_func(source, global_ns, is_kernel())

    # If no variable names were given, only get the last assignment
    if not names:
        for inst in list(dis.get_instructions(func)):
            if inst.opname in {"STORE_NAME", "STORE_FAST", "STORE_DEREF", "STORE_GLOBAL"}:
                names = (inst.argval,)

    # Use innerscope!  We only keep the globals, locals, and closures we need.
    scoped = scoped_function(func, data)
    if scoped.missing:
        # Gather the necessary closures and locals
        update = {key: local_ns[key] for key in scoped.missing if key in local_ns}
        scoped = scoped.bind(update)

    if where == "remotely":
        # Get ready to submit to dask.distributed by separating the Futures.
        futures = {
            key: val
            for key, val in scoped.outer_scope.items()
            if isinstance(val, Future)
            # TODO: what can/should we do if the future is in a bad state?
        }
        for key in futures:
            del scoped.outer_scope[key]
    else:
        futures = None
    magic_func = MagicFunction(source, scoped, display_expr)
    return magic_func, names, futures
