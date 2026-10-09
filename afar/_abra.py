"""Perform a magic trick: given lines of code, create a function to run remotely.

This callable object is able to provide the values of the requested argument
names and return the final expression so it can be displayed.
"""

import ast
import io
import tokenize

from dask.distributed import Future
from innerscope import scoped_function

from ._reprs import get_repr_methods
from ._utils import is_ipython


class _AssignedNames(ast.NodeVisitor):
    """Collect the names a block of statements binds, in source order (and in evaluation
    order within a statement).

    Nested scopes (functions, lambdas, classes, and comprehensions) bind names of their own,
    so only their names (for functions and classes) and the parts evaluated in the enclosing
    scope are visited.
    """

    def __init__(self):
        self.names = []

    def visit_Name(self, node):
        if isinstance(node.ctx, ast.Store):
            self.names.append(node.id)

    def visit_Assign(self, node):
        self.visit(node.value)
        for target in node.targets:
            self.visit(target)

    def visit_AugAssign(self, node):
        self.visit(node.value)
        self.visit(node.target)

    def visit_AnnAssign(self, node):
        # `x: int` declares `x` without binding it
        if node.value is not None:
            self.visit(node.value)
            self.visit(node.target)

    def visit_NamedExpr(self, node):
        self.visit(node.value)
        self.visit(node.target)

    def visit_For(self, node):
        self.visit(node.iter)
        self.visit(node.target)
        for stmt in node.body + node.orelse:
            self.visit(stmt)

    visit_AsyncFor = visit_For

    def visit_Import(self, node):
        for alias in node.names:
            self.names.append(alias.asname or alias.name.partition(".")[0])

    def visit_ImportFrom(self, node):
        for alias in node.names:
            self.names.append(alias.asname or alias.name)

    def visit_ExceptHandler(self, node):
        # Skip `node.name`: Python deletes it when the handler finishes
        if node.type is not None:
            self.visit(node.type)
        for stmt in node.body:
            self.visit(stmt)

    def visit_FunctionDef(self, node):
        for expr in node.decorator_list:
            self.visit(expr)
        self.visit(node.args)
        self.names.append(node.name)

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_arguments(self, node):
        # Only the defaults are evaluated in the enclosing scope
        for expr in node.defaults + node.kw_defaults:
            if expr is not None:
                self.visit(expr)

    def visit_Lambda(self, node):
        self.visit(node.args)

    def visit_ClassDef(self, node):
        for expr in node.decorator_list + node.bases + node.keywords:
            self.visit(expr)
        self.names.append(node.name)

    def _visit_comprehension(self, node):
        # Only the first iterable is evaluated in the enclosing scope, but a walrus in a
        # comprehension binds its name there too (PEP 572)
        self.visit(node.generators[0].iter)
        walrus = _WalrusNames()
        walrus.visit(node)
        self.names.extend(walrus.names)

    visit_ListComp = visit_SetComp = visit_DictComp = visit_GeneratorExp = _visit_comprehension

    def _visit_capture(self, node):
        # `case [x, *rest]`, `case {**rest}`, and `case Point() as p` bind names
        self.generic_visit(node)
        name = node.rest if isinstance(node, ast.MatchMapping) else node.name
        if name is not None:
            self.names.append(name)

    visit_MatchAs = visit_MatchStar = visit_MatchMapping = _visit_capture


class _WalrusNames(ast.NodeVisitor):
    """Collect the names that walruses in a comprehension bind, which is in the enclosing scope"""

    def __init__(self):
        self.names = []

    def visit_NamedExpr(self, node):
        self.visit(node.value)
        self.names.append(node.target.id)

    def visit_Lambda(self, node):
        # A walrus in a lambda binds in the lambda
        pass


def last_assigned_name(stmts):
    """The name bound last by the statements and not deleted after, or None"""
    visitor = _AssignedNames()
    for stmt in stmts:
        if isinstance(stmt, ast.Delete):
            # Only a `del` among the statements themselves surely runs, not one in a branch
            deleted = {
                node.id
                for target in stmt.targets
                for node in ast.walk(target)
                if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Del)
            }
            visitor.names = [name for name in visitor.names if name not in deleted]
        else:
            visitor.visit(stmt)
    return visitor.names[-1] if visitor.names else None


def create_func(source, globals_dict, is_in_ipython):
    tree = ast.parse(source, "<afar>")
    funcdef = tree.body[0]
    for node in ast.walk(funcdef):
        # Python 3.15 (PEP 810) allows `lazy import` at module level, but not in a function
        if isinstance(node, (ast.Import, ast.ImportFrom)) and getattr(node, "is_lazy", False):
            node.is_lazy = 0
    last = funcdef.body[-1]
    display_expr = is_in_ipython and isinstance(last, ast.Expr)
    if display_expr:
        # Return the final expression so it can be displayed, like the last line of a cell
        funcdef.body[-1] = ast.copy_location(ast.Return(last.value), last)
    code = compile(tree, "<afar>", "exec")
    locals_dict = {}
    exec(code, globals_dict, locals_dict)
    func = locals_dict["_afar_magic_"]
    return func, display_expr, funcdef


def ends_with_semicolon(source):
    """Whether the last token is `;`, which tells IPython not to display the final expression"""
    skip = {
        tokenize.ENDMARKER,
        tokenize.NL,
        tokenize.NEWLINE,
        tokenize.COMMENT,
        tokenize.INDENT,
        tokenize.DEDENT,
    }
    for tok in reversed(list(tokenize.generate_tokens(io.StringIO(source).readline))):
        if tok.type not in skip:
            return tok.type == tokenize.OP and tok.string == ";"
    return False


class MagicFunction:
    def __init__(self, source, scoped, display_expr, quiet=False):
        self._source = source
        self._scoped = scoped
        # The function returns its final expression; display it unless `quiet`
        self._display_expr = display_expr
        self._quiet = quiet
        if display_expr and not quiet:
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
        func, _, _ = create_func(self._source, {}, self._display_expr)
        self._scoped = scoped_function(func, outer_scope)


def cadabra(context_body, where, names, data, global_ns, local_ns):
    # Create a new function from the code block of the context.
    # For now, we require that the source code is available.
    source = "def _afar_magic_():\n" + "".join(context_body)
    func, display_expr, funcdef = create_func(source, global_ns, is_ipython())

    # If no variable names were given, only get the last assignment
    if not names:
        name = last_assigned_name(funcdef.body)
        if name is not None:
            names = (name,)

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
    quiet = display_expr and ends_with_semicolon(source)
    magic_func = MagicFunction(source, scoped, display_expr, quiet)
    return magic_func, names, futures
