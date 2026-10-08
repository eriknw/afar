import subprocess
import sys
from operator import add

import pytest
from dask.distributed import Client, Future

import afar

# TODO: better testing infrastructure
if __name__ == "__main__":
    with Client(n_workers=1, threads_per_worker=1, dashboard_address=":0") as client:
        two = client.submit(add, 1, 1)

        with afar.run as results, afar.remotely:
            three = two + 1
        assert three.result() == 3

        with afar.get, afar.remotely(priority=1):
            five = two + three
        assert five == 5


def test_runme():
    assert subprocess.check_call([sys.executable, __file__]) == 0


def test_simple(client):
    two = client.submit(add, 1, 1)

    with afar.run as results, afar.remotely:
        three = two + 1
    three = results["three"]
    assert three.result() == 3
    with afar.get as results, afar.remotely(priority=1):
        five = two + three
    assert results == {"five": 5}


def test_innerscope_method(client, pytestconfig):
    import innerscope

    with afar.get as results, afar.remotely:
        import innerscope as remote_innerscope

        method = remote_innerscope.cfg.default_method
    assert results["method"] == innerscope.cfg.default_method
    expected = pytestconfig.getoption("--innerscope-method")
    if expected is not None:
        assert results["method"] == expected


def test_runs_in_worker_process(client):
    import os

    with afar.get as results, afar.remotely:
        import os as remote_os

        pid = remote_os.getpid()
    assert results["pid"] != os.getpid()


def test_no_names_still_runs(client, tmp_path, wait_for_outputs):
    # Nothing is assigned, so no future depends on the remote task; it must run anyway
    for i in range(5):
        path = tmp_path / f"ran-{i}"
        with afar.run as results, afar.remotely:
            path.touch()
        assert results == {}
    wait_for_outputs()
    assert sorted(p.name for p in tmp_path.iterdir()) == [f"ran-{i}" for i in range(5)]


def test_names_and_get(client):
    with afar.run("a", "b") as results, afar.remotely:
        a = 1
        b = a + 1
        c = b + 1
    assert results.keys() == {"a", "b"}
    assert all(isinstance(val, Future) for val in results.values())
    # Before Python 3.13, `a` and `b` are not set in this function, so pass them as data
    with afar.get(data=results), afar.remotely:
        d = a + b
    assert results["d"] == 3


def test_data_is_scattered(client):
    run = afar.run(data={"x": 10})
    with run, afar.remotely:
        y = x + 1
    # `x` was needed remotely, so it was scattered and is now a future too
    assert isinstance(run.data["x"], Future)
    assert run.data["x"].result() == 10
    assert run.data["y"].result() == 11


def test_print_is_captured(client, capsys, wait_for_outputs):
    with afar.run, afar.remotely:
        print("hello", "afar")
        print("oops", file=sys.stderr)
    wait_for_outputs()
    captured = capsys.readouterr()
    assert "hello afar\n" in captured.out
    assert "oops\n" in captured.err


def test_remote_error(client):
    with afar.run as results, afar.remotely:
        x = 1 / 0
    with pytest.raises(ZeroDivisionError):
        results["x"].result()


def test_cancel(client, wait_for_outputs):
    run = afar.run()
    # No worker has this resource, so the context waits until it is cancelled
    with run as results, afar.remotely(resources={"unavailable": 1}):
        x = 1
    future = results["x"]
    assert future.status == "pending"
    run.cancel()
    # The context never ran, so it never sent "finish"; this waits for afar to give up on it
    wait_for_outputs()
    assert future.status == "cancelled"


def test_remote_in_function(client):
    def f():
        with afar.get, afar.remotely:
            y = 41 + 1
        return y

    if sys.version_info >= (3, 13):
        # PEP 667: f_locals of a function is a write-through proxy, so `y` is set
        assert f() == 42
    else:
        with pytest.raises(UnboundLocalError):
            f()
