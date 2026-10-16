"""Run many kinds of context bodies through afar, and compare with running them plainly.

afar needs the source of a context, so each body is written into a module that is imported.
"""

import importlib.util
import sys
import textwrap

import pytest

# Each body uses `y0` from outside the context and ends by assigning `z`
BODIES = {
    "closure": """
        def f(a):
            return a + y0
        z = f(1)
    """,
    "lambda": """
        g = lambda q: q * y0
        z = g(2)
    """,
    "recursion": """
        def fact(n):
            return 1 if n <= 1 else n * fact(n - 1)
        z = fact(y0)
    """,
    "nonlocal": """
        count = y0
        def inc():
            nonlocal count
            count += 1
        inc()
        inc()
        z = count
    """,
    "class": """
        class C:
            v = y0
            def m(self):
                return self.v + 1
        z = C().m()
    """,
    "super": """
        class A:
            def f(self):
                return y0
        class B(A):
            def f(self):
                return super().f() + 1
        z = B().f()
    """,
    "generator function": """
        def gen():
            yield y0
            yield y0 + 1
        z = list(gen())
    """,
    "comprehensions": """
        a = [y0 + i for i in range(3)]
        b = {i: y0 for i in range(2)}
        c = sum(y0 * i for i in range(3))
        z = (a, b, c)
    """,
    "walrus in comprehension": """
        a = [last := y0 + i for i in range(3)]
        z = (a, last)
    """,
    "try": """
        try:
            1 / 0
        except ZeroDivisionError as exc:
            err = type(exc).__name__
        else:
            err = None
        finally:
            done = True
        z = (err, done)
    """,
    "with": """
        import contextlib
        with contextlib.nullcontext(y0) as v:
            w = v + 1
        z = w
    """,
    "loops": """
        total = 0
        for i in range(y0):
            if i == 3:
                break
            total += i
        else:
            total = -1
        while total < 10:
            total += y0
        z = total
    """,
    "match": """
        match [y0, 2, 3]:
            case [first, *rest] if first > 0:
                z = (first, rest)
            case _:
                z = None
    """,
    "unpacking": """
        a, *b, c = range(y0)
        z = (a, b, c)
    """,
    "del": """
        tmp = y0
        z = tmp + 1
        del tmp
    """,
    "async def": """
        async def co():
            return y0
        z = co.__name__
    """,
    "many locals": "\n".join(f"v{i} = y0 + {i}" for i in range(300)) + "\nz = v299\n",
}


def make_module(tmp_path, where):
    lines = ["import afar", "", "y0 = 5", ""]
    for i, body in enumerate(BODIES.values()):
        body = textwrap.dedent(body).strip("\n")
        lines += [
            f"def plain_{i}():",
            textwrap.indent(body, " " * 4),
            "    return z",
            "",
            f"def with_afar_{i}():",
            f"    with afar.get as results, afar.{where}:",
            textwrap.indent(body, " " * 8),
            "    return results",
            "",
        ]
    path = tmp_path / f"afar_bodies_{where}.py"
    path.write_text("\n".join(lines))
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[path.stem] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module", params=["locally", "remotely"])
def bodies_module(request, tmp_path_factory):
    if request.param == "remotely":
        request.getfixturevalue("client")
    module = make_module(tmp_path_factory.mktemp("bodies"), request.param)
    yield module
    del sys.modules[module.__name__]


@pytest.mark.parametrize("index", range(len(BODIES)), ids=list(BODIES))
def test_body(bodies_module, index):
    expected = getattr(bodies_module, f"plain_{index}")()
    assert getattr(bodies_module, f"with_afar_{index}")() == {"z": expected}


@pytest.mark.skipif(sys.version_info < (3, 15), reason="`lazy import` is new in Python 3.15")
@pytest.mark.parametrize("where", ["locally", "remotely"])
def test_lazy_import(request, tmp_path, where):
    # `lazy import` is allowed only at module level, so the context must be there too
    if where == "remotely":
        request.getfixturevalue("client")
    path = tmp_path / f"afar_lazy_{where}.py"
    path.write_text(
        "import afar\n"
        f"with afar.get as results, afar.{where}:\n"
        "    lazy import json\n"
        "    lazy from os import path as os_path\n"
        "    s = json.dumps([os_path.basename('a/b')])\n"
    )
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.results == {"s": '["b"]'}
