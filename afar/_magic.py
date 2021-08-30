from textwrap import indent

from dask.distributed import Client
from IPython.core.error import UsageError
from IPython.core.magic import Magics, line_cell_magic, magics_class, needs_local_scope

from ._core import Run, get, run
from ._where import Where, remotely


@magics_class
class AfarMagic(Magics):
    @needs_local_scope
    @line_cell_magic
    def afar(self, line, cell=None, *, local_ns):
        """Execute the cell on a dask.distributed cluster.

        Usage, in line mode:
            %afar [-g -r run -d data -w where -c client] code_to_run
        Usage, in cell mode
            %%afar [-g -r run -d data -w where -c client <variable_names>]
            code...
            code...

        Options:

          -g/--get
            Get results as values, not as futures.  This uses `afar.get` instead of `afar.run`.

          -r/--run <run>
            A `Run` object from the local namespace such as `run = afar.run(data=mydata)`

          -d/--data <data>
            A MutableMapping to use for the data; typically part of a `Run` object.

          -w/--where <where>
            A `Where` object such as `where = remotely(resources={"GPU": 1})`

          -c/--client <client>
            A `dask.distributed.Client` object from the local namespace.

          <variable_names>
            Variable names (space- or comma-separated) from the cell to copy to the local
            namespace as dask Future objects.

        All arguments given in options must be variable names in the local namespace.

        Examples
        --------
        ::

            In [1]: %%afar x, y
               ...: x = 1
               ...: y = x + 1

            In [2]: z = %afar x + y

        """
        opts, line = self.parse_options(
            line,
            "gr:d:w:c:",
            "get",
            "run=",
            "data=",
            "where=",
            "client=",
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
        where = remotely

        not_found = "argument not found in local namespace"
        if "r" in opts or "run" in opts:
            runner = opts.get("r", opts.get("run"))
            if runner not in local_ns:
                raise UsageError(f"Variable name {runner!r} for -r or --run {not_found}")
            if not isinstance(runner, Run):
                raise UsageError(f"-r or --run argument must be of type Run; got: {type(runner)}")
        elif "g" in opts or "get" in opts:
            runner = get()
        else:
            runner = run()
        client = runner.client

        data = runner.data
        if "d" in opts or "data" in opts:
            data = opts.get("d", opts.get("data"))
            if data not in local_ns:
                raise UsageError(f"Variable name {data!r} for -d or --data {not_found}")
            data = local_ns[data]
        if data is None:
            data = {}

        if "w" in opts or "where" in opts:
            where = opts.get("w", opts.get("where"))
            if where not in local_ns:
                raise UsageError(f"Variable name {where!r} for -w or --where {not_found}")
            where = local_ns[where]
            if not isinstance(where, Where):
                raise UsageError(
                    f"-w or --where argument must be of type Where; got: {type(where)}"
                )
            if client is None:
                client = where.client

        if "c" in opts or "client" in opts:
            client = opts.get("c", opts.get("client"))
            if client not in local_ns:
                raise UsageError(f"Variable name {client!r} for -c or --client {not_found}")
            client = local_ns[client]
            if not isinstance(client, Client):
                raise UsageError(
                    f"-c or --client argument must be of type Client; got: {type(client)}"
                )

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
