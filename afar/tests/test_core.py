import importlib.util
import pickle
import textwrap

import pytest
from pytest import raises

import afar


def test_a_modest_beginning():
    with afar.run(), locally:
        x = 1
        y = x + 1

    with afar.run(), afar.locally:
        pass

    with afar.run(), locally:
        pass

    with afar.run(), afar.locally:
        pass

    with raises(NameError, match="locallyblah"):
        with afar.run(), locallyblah:
            pass

    with raises(Exception, match="`afar.run` is missing a location"):
        with afar.run():
            pass


# Not the final API, but a useful step
def test_temporary_assignment():
    z = 1

    def f():
        w = 10
        with afar.run() as results, locally:
            x = z
            y = x + 1 + w
        return results

    results = f()
    assert "x" not in results
    assert results["y"] == 12
    assert not hasattr(results, "w")
    assert not hasattr(results, "z")

    with afar.run as results, afar.locally:
        x = z
        y = x + 1
    with raises(UnboundLocalError):
        x
    assert results == {"y": 2}

    # fmt: off
    with \
        afar.run() as results, \
        locally \
    :
        x = z
        y = x + 1
    assert results == {"y": 2}
    # fmt: on

    with afar.run("a") as results, locally:
        a = 1
        b = a + 1
    assert results == {"a": 1}


def test_give_data():
    data = {"a": 1}
    run = afar.run(data=data)
    with run, locally:
        b = a + 1
    assert run.data is data
    assert data == {"a": 1, "b": 2}
    c = 10
    with run, locally:
        d = a + b + c
    assert data == {"a": 1, "b": 2, "d": 13}

    # singleton doesn't save data
    with afar.run as data2, locally:
        e = 100
    assert afar.run.data is None
    assert data2 == {"e": 100}


def test_endline():
    # fmt: off
    with afar.run as results, locally:
        a = 1
        b = (
            a
            +
            2
        )
    assert results == {"b": 3}
    # fmt: on


def test_pickle():
    run = afar.run()
    with run, locally:
        a = 1
    assert run.data == {"a": 1}
    func = run._magic_func
    s = pickle.dumps(func)
    func2 = pickle.loads(s)
    assert dict(func2()) == {"a": 1}
    assert func._scoped.func.__code__.co_code == func2._scoped.func.__code__.co_code


def test_failed_context_leaves_no_state():
    # The singleton must recover from a context that fails before it runs: a one-line body
    # (`__enter__` raises), a typo in the location (`__exit__` sees a NameError), and a
    # second context manager that is not a location (the body runs here)
    import contextlib

    with raises(RuntimeError, match="new line"):
        # fmt: off
        with afar.run, locally: pass
        # fmt: on
    with raises(NameError, match="locallyy"):
        with afar.run, locallyy:
            pass
    with afar.run, contextlib.nullcontext():
        pass
    assert afar.run.data is None
    with afar.run() as results, locally:
        x = 1
    assert results == {"x": 1}
    assert afar.run.data is None
    with afar.run as results, locally:
        y = 2
    assert results == {"y": 2}


def test_header_with_string_line_starting_with_with():
    # Since Python 3.12, the line number while `afar.run` enters is the line of `afar.run`;
    # searching back from it passes a line of the string that begins with "with"
    import contextlib

    with (
        contextlib.nullcontext("""
with
"""),
        afar.run as results,
        locally,
    ):
        x = 1
    assert results == {"x": 1}


def test_name_not_assigned():
    with raises(KeyError, match="'y' was not assigned in the afar context"):
        with afar.run, locally:
            while True:
                x = 1
                break
            else:
                y = 2
    with raises(KeyError, match="'w' was not assigned in the afar context"):
        with afar.run("x", "w"), locally:
            x = 1


def _context_a(n, out):
    for _ in range(n):
        try:
            with afar.run as results, locally:
                a = 1
        except Exception as exc:
            results = repr(exc)
        out.append(results)


def _context_b(n, out):
    for _ in range(n):
        try:
            with afar.run as results, locally:
                b = 2
        except Exception as exc:
            results = repr(exc)
        out.append(results)


def test_threads_share_the_singleton():
    # What `__enter__` finds is kept per thread.  Switch threads often to mix them up.
    import sys
    import threading

    n = 1000
    out_a = []
    out_b = []
    interval = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    try:
        threads = [
            threading.Thread(target=_context_a, args=(n, out_a)),
            threading.Thread(target=_context_b, args=(n, out_b)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    finally:
        sys.setswitchinterval(interval)
    assert [r for r in out_a if r != {"a": 1}] == []
    assert [r for r in out_b if r != {"b": 2}] == []
    assert len(out_a) == len(out_b) == n


def test_nested_same_run():
    # The outer context runs here, since `nullcontext()` is not a location, so its body may use
    # the same `Run` again
    import contextlib

    with afar.run as outer, contextlib.nullcontext():
        with afar.run as inner, locally:
            x = 1
        y = 2
    assert inner == {"x": 1}
    assert outer == {}
    assert y == 2


@pytest.mark.parametrize("final_newline", ["\n", ""])
@pytest.mark.parametrize("scope", ["module", "function"])
def test_end_of_file(tmp_path, scope, final_newline):
    # The context body is the end of the file, with or without a final newline
    body = "with afar.run(data=data), afar.locally:\n    x = 10\n    y = 2 * x"
    if scope == "function":
        source = "import afar\n\ndef f(data):\n" + textwrap.indent(body, "    ")
    else:
        source = "import afar\n\ndata = {}\n" + body
    path = tmp_path / f"afar_eof_{scope}_{len(final_newline)}.py"
    path.write_text(source + final_newline)
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if scope == "function":
        data = {}
        module.f(data)
    else:
        data = module.data
    assert data == {"y": 20}
