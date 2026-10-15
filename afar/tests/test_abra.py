"""Turn a context body into a function, and choose what to bring back."""

import ast
import textwrap

import pytest

import afar
from afar._abra import create_func, ends_with_semicolon, last_assigned_name


def last_name(source):
    return last_assigned_name(ast.parse(textwrap.dedent(source)).body)


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("x = 1", "x"),
        ("x = y = 1", "y"),
        ("a, (b, *c) = 1, (2, 3)", "c"),
        ("x = 1\nx += 1", "x"),
        ("x: int = 1", "x"),
        ("y = 1\nx: int", "y"),
        ("x = 1\nobj.attr = 2\nd['key'] = 3", "x"),
        ("x = (y := 1)", "x"),
        ("f(y := 1)", "y"),
        ("import os.path", "os"),
        ("import numpy as np", "np"),
        ("from os import path, sep as separator", "separator"),
        ("x = 1\nimport os", "os"),
        ("def f(z=1):\n    w = 2", "f"),
        ("x = 1\ndef f(z=(y := 2)):\n    w = 2", "f"),
        ("class C:\n    w = 2", "C"),
        ("for i in range(3):\n    total = i", "total"),
        ("for i in range(3):\n    pass", "i"),
        ("with open(f) as fh:\n    pass", "fh"),
        ("if c:\n    x = 1\nelse:\n    y = 2", "y"),
        ("x = 1\ndel x", None),
        ("x = 1\ny = 2\ndel y", "x"),
        ("x = 1\ndel x\nx = 2", "x"),
        ("x = y = 1\ndel x, d[y]", "y"),
        ("for item in range(3):\n    total = item\ndel item", "total"),
        ("a = b = 1\ndel (a, [b])", None),
        # A `del` that may not run does not hide the name
        ("x = 1\nif flag:\n    del x", "x"),
        ("for i in range(3):\n    tmp = i\n    del tmp", "tmp"),
        ("pass", None),
        ("print(1)", None),
        # Python 3.12+ inlines comprehensions (PEP 709); the iteration variable is not a result
        ("y = [i for i in range(3)]", "y"),
        ("y = {k: v for k, v in d.items()}", "y"),
        ("y = sum(i for i in range(3))", "y"),
        ("[i for i in range(3)]", None),
        # A walrus in a comprehension binds in the enclosing scope (PEP 572), even when nested
        ("x = 1\nany((hit := v) > 1 for v in x)", "hit"),
        ("x = 1\n[[(last := j) for j in row] for row in x]", "last"),
        ("y = [(last := i) for i in range(3)]", "y"),
        ("x = 1\n[lambda: (w := 1) for i in range(3)]", "x"),
        ("f = lambda q=1: (r := q)", "f"),
        # The name of an exception is deleted at the end of the handler
        ("try:\n    x = 1\nexcept Exception as e:\n    pass", "x"),
        ("try:\n    x = 1\nexcept Exception as e:\n    y = e", "y"),
        ("match p:\n    case [a, *rest]:\n        pass", "rest"),
        ("match p:\n    case {'k': a, **rest}:\n        pass", "rest"),
        ("match p:\n    case Point(x=a) as pt:\n        pass", "pt"),
        ("match p:\n    case _:\n        pass", None),
    ],
)
def test_last_assigned_name(source, expected):
    assert last_name(source) == expected


def make_source(body):
    return "def _afar_magic_():\n" + textwrap.indent(textwrap.dedent(body), "    ")


@pytest.mark.parametrize(
    ("body", "display", "rv"),
    [
        ("x = 1\nx + 1", True, 2),
        ("x = 1", False, None),
        ("x = 1\nif x:\n    x + 1", False, None),
        ("for i in range(2):\n    i", False, None),
        ("print", True, print),
        ("x = 1\n(x\n +\n 2)", True, 3),
    ],
)
def test_create_func_display_expr(body, display, rv):
    func, display_expr, _ = create_func(make_source(body), {}, True)
    assert display_expr is display
    assert func() == rv
    # Outside IPython, the final expression is not returned
    func, display_expr, _ = create_func(make_source(body), {}, False)
    assert display_expr is False
    assert func() is None


def test_default_names_skip_comprehension_variable():
    with afar.run as results, afar.locally:
        # Not `[i for i in ...]`, which pyupgrade rewrites to `[*...]`, with no `i` to skip
        y = [10 * i for i in range(3)]
    assert results == {"y": [0, 10, 20]}


def test_default_names_walrus_in_comprehension():
    with afar.run as results, afar.locally:
        values = [1, 2, 3]
        any((hit := v) > 1 for v in values)
    assert results == {"hit": 2}


def test_default_names_skip_exception_name():
    with afar.run as results, afar.locally:
        try:
            x = 1
        except Exception as e:
            pass
    assert results == {"x": 1}


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        ("x + 1;", True),
        ("x + 1;  # comment", True),
        ("x + 1; y", False),
        ("x + 1", False),
        ("s = ';'", False),
        ("x + 1  # ;", False),
        ("if x:\n    y;\n", True),
    ],
)
def test_ends_with_semicolon(body, expected):
    assert ends_with_semicolon(make_source(body)) is expected
