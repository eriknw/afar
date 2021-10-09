"""Define the IPython magic for using afar"""
from textwrap import indent

from dask.distributed import Client
from IPython import get_ipython
from IPython.core.error import UsageError
from IPython.core.magic import Magics, line_cell_magic, magics_class, needs_local_scope

from ._core import Run, get, run
from ._where import Where, remotely

DOC_TEMPLATE = """Execute the cell on a dask.distributed cluster.

Usage, in line mode:
    %{name} [{get_arg}{run_arg}{data_arg}{where_arg}{client_arg}] code_to_run
Usage, in cell mode
    %%{name} [{get_arg}{run_arg}{data_arg}{where_arg}{client_arg} <variable_names>]
    code...
    code...

Options:
{get_desc} {run_desc} {data_desc} {where_desc} {client_desc}
  <variable_names>
    Variable names (space- or comma-separated) from the cell to copy to the local
    namespace as dask Future objects.

All arguments given in options must be variable names in the local namespace.

Examples
--------
::

    In [1]: %%{name} x, y
       ...: x = 1
       ...: y = x + 1

    In [2]: z = %{name} x + y

"""
DOC_DEFAULTS = {
    "name": "afar",
    "get_arg": "-g",
    "run_arg": " -r run",
    "data_arg": " -d data",
    "where_arg": " -w where",
    "client_arg": " -c client",
    "get_desc": (
        "\n  -g/--get\n"
        "    Get results as values, not as futures.  This uses `afar.get` instead of `afar.run`.\n"
    ),
    "run_desc": (
        "\n  -r/--run <run>\n"
        "    A `Run` object from the local namespace such as `run = afar.run(data=mydata)`\n"
    ),
    "data_desc": (
        "\n  -d/--data <data>\n"
        "    A MutableMapping to use for the data; typically part of a `Run` object.\n"
    ),
    "where_desc": (
        "\n  -w/--where <where>\n"
        '    A `Where` object such as `where = remotely(resources={"GPU": 1})`\n'
    ),
    "client_desc": (
        "\n  -c/--client <client>\n"
        "    A `dask.distributed.Client` object from the local namespace.\n"
    ),
}


class AfarMagicBase(Magics):
    def _run(self, line, cell=None, *, local_ns, runner=None, data=None, where=None, client=None):
        options = ""
        args = []
        if runner is None:
            options += "gr:"
            args.append("get")
            args.append("run=")
        if data is None:
            options += "d:"
            args.append("data=")
        if where is None:
            options += "w:"
            args.append("where=")
        if client is None:
            options += "c:"
            args.append("client=")
        opts, line = self.parse_options(
            line,
            options,
            *args,
            posix=False,
            strict=False,
            preserve_non_opts=True,
        )
        if "r" in opts and "run" in opts:
            raise UsageError("-r and --run options may not be used at the same time")
        if "d" in opts and "data" in opts:
            raise UsageError("-d and --data options may not be used at the same time")
        if "w" in opts and "where" in opts:
            raise UsageError("-w and --where options may not be used at the same time")
        if "c" in opts and "client" in opts:
            raise UsageError("-c and --client options may not be used at the same time")

        not_found = "argument not found in local namespace"
        if runner is not None:
            pass
        elif "r" in opts or "run" in opts:
            runner = opts.get("r", opts.get("run"))
            if runner not in local_ns:
                raise UsageError(f"Variable name {runner!r} for -r or --run {not_found}")
            if not isinstance(runner, Run):
                raise UsageError(f"-r or --run argument must be of type Run; got: {type(runner)}")
        elif "g" in opts or "get" in opts:
            runner = get()
        else:
            runner = run()

        if data is not None:
            pass
        elif "d" in opts or "data" in opts:
            data = opts.get("d", opts.get("data"))
            if data not in local_ns:
                raise UsageError(f"Variable name {data!r} for -d or --data {not_found}")
            data = local_ns[data]
        else:
            data = runner.data
        if data is None:
            data = {}

        if where is not None:
            pass
        elif "w" in opts or "where" in opts:
            where = opts.get("w", opts.get("where"))
            if where not in local_ns:
                raise UsageError(f"Variable name {where!r} for -w or --where {not_found}")
            where = local_ns[where]
            if not isinstance(where, Where):
                raise UsageError(
                    f"-w or --where argument must be of type Where; got: {type(where)}"
                )
        else:
            where = remotely

        if client is not None:
            pass
        elif "c" in opts or "client" in opts:
            client = opts.get("c", opts.get("client"))
            if client not in local_ns:
                raise UsageError(f"Variable name {client!r} for -c or --client {not_found}")
            client = local_ns[client]
            if not isinstance(client, Client):
                raise UsageError(
                    f"-c or --client argument must be of type Client; got: {type(client)}"
                )
        else:
            client = runner.client or where.client

        if cell is None:
            names = ()
            context_body = line
        else:
            # Comma-separated and/or space-separated variable names
            names = [
                name
                for item in line.split("#")[0].split(",")
                for name in item.strip().split(" ")
                if name
            ]
            bad_names = [name for name in names if not name.isidentifier()]
            if bad_names:
                raise UsageError(
                    f"The following are bad variable names: {bad_names}\n"
                    "The %%afar magic accepts a list of variable names (after any options) "
                    "to bring back to local scope.  Example usage:\n\n"
                    "%%afar x, y\nx = 1\ny = x + 1"
                )
            context_body = cell
        if not names:
            names = runner.names
        context_body = indent(context_body, "    ")
        return runner._run(
            where.where,
            context_body,
            names,
            data,
            global_ns=local_ns,
            local_ns=local_ns,
            client=client,
            submit_kwargs=where.submit_kwargs,
            return_expr=cell is None,
        )


@magics_class
class AfarMagic(AfarMagicBase):
    @needs_local_scope
    @line_cell_magic
    def afar(self, line, cell=None, *, local_ns):
        return self._run(line, cell, local_ns=local_ns)

    afar.__doc__ = DOC_TEMPLATE.format(**DOC_DEFAULTS)


def new_magic(name, *, run=None, data=None, where=None, client=None):
    """Create new IPython magic to run afar with the given arguments.

    This is like `%%afar` magic with arguments curried or "baked in".

    Examples
    --------
    ::

        In [1]: afar.new_magic("on_gpus", where=remotely(resources={"GPU": 1}))

        In [2]: %%on_gpus x, y
           ...: x = 1
           ...: y = x + 1

        In [3]: z = %on_gpus x + y

    """

    def magic_method(self, line, cell=None, *, local_ns):
        return self._run(
            line, cell, local_ns=local_ns, runner=run, data=data, where=where, client=client
        )

    d = dict(DOC_DEFAULTS, name=name)
    if run is not None:
        d["get_arg"] = ""
        d["get_desc"] = ""
        d["run_arg"] = ""
        d["run_desc"] = ""
    if data is not None:
        d["data_arg"] = ""
        d["data_desc"] = ""
    if where is not None:
        d["where_arg"] = ""
        d["where_desc"] = ""
    if client is not None:
        d["client_arg"] = ""
        d["client_desc"] = ""
    magic_method.__name__ = name
    magic_method.__doc__ = DOC_TEMPLATE.format(**d)

    @magics_class
    class AfarNewMagic(AfarMagicBase):
        locals()[name] = needs_local_scope(line_cell_magic(magic_method))

    ip = get_ipython()
    ip.register_magics(AfarNewMagic)
