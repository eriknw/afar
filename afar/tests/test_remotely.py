import subprocess
import sys
import threading
import time
from concurrent.futures import CancelledError
from operator import add

import dask.distributed
import pytest
from dask.distributed import Client, Future

import afar
from afar._core import Run

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
    expected = pytestconfig.getoption("--innerscope-method", default=None)
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


def test_remote_name_not_assigned(client):
    with afar.run("x", "w") as results, afar.remotely:
        x = 1
    with pytest.raises(KeyError, match="'w' was not assigned in the afar context"):
        results["x"].result()


def test_remote_error_is_shown(client, capsys, wait_for_outputs):
    # Nothing is brought back, so no future would raise the error; it must be shown anyway
    with afar.run as results, afar.remotely:
        print("about to fail")
        1 / 0
    assert results == {}
    wait_for_outputs()
    captured = capsys.readouterr()
    assert captured.out == "about to fail\n"
    assert "ZeroDivisionError" in captured.err


def test_remote_error(client, wait_for_outputs):
    with afar.run as results, afar.remotely:
        x = 1 / 0
    with pytest.raises(ZeroDivisionError):
        results["x"].result()
    wait_for_outputs()


def test_input_failed(client, capsys, wait_for_outputs):
    # The second context never runs because its input failed, so it never sends its own error
    # or "finish"; afar must still report it and stop waiting
    with afar.run as results, afar.remotely:
        x = 1 / 0
    data = results
    with afar.run(data=data), afar.remotely:
        print("x + 1 =", x + 1)
    wait_for_outputs()
    err = capsys.readouterr().err
    assert "afar failed: ZeroDivisionError" in err
    # Only for the second context: the first reported its own error, as a traceback
    assert err.count("afar failed") == 1


def test_worker_dies(capsys, wait_for_outputs):
    # A worker that dies in the context never sends "finish"; afar must stop waiting for it.
    # Use a cluster of our own that does not retry the task, and leave the default client alone.
    with Client(
        n_workers=1, threads_per_worker=1, dashboard_address=":0", set_as_default=False
    ) as own_client:
        own_client.run_on_scheduler(_no_retries)
        with afar.run as results, afar.remotely(client=own_client):
            __import__("os")._exit(1)
        assert results == {}
        wait_for_outputs(timeout=60)
    assert "afar failed" in capsys.readouterr().err


def _no_retries(dask_scheduler):
    dask_scheduler.allowed_failures = 0


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


def get_waits_in_thread(get, client):
    """Enter a context of `get` in another thread, which waits until it is cancelled.

    Returns the thread, the futures of its context's tasks, and a dict for what it raised.
    """
    outcome = {}

    def target():
        try:
            # No worker has this resource, so the context waits until it is cancelled
            with get, afar.remotely(resources={"unavailable": 1}):
                x = 1
        except Exception as exc:
            outcome["raised"] = exc

    # A daemon, so that a failing test cannot keep the test run waiting for it
    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    # Wait until the context has submitted the task that brings back `x`
    deadline = time.monotonic() + 10
    while True:
        assert time.monotonic() < deadline
        try:
            futures = list(get._client_to_futures.get(client, ()))
        except RuntimeError:  # pragma: no cover (the thread added a future meanwhile)
            continue
        # (Not the scattered function, which afar releases at once)
        tasks = [f for f in futures if str(f.key).startswith(("run_afar", "get_afar"))]
        if len(tasks) == 2:
            return thread, tasks, outcome
        time.sleep(0.01)


def test_get_raises_if_cancelled(client, wait_for_outputs):
    get = afar.get()
    thread, _, outcome = get_waits_in_thread(get, client)
    get.cancel()
    thread.join(10)
    # Not a CancelledError as the value of `x`
    assert isinstance(outcome.get("raised"), CancelledError), outcome
    wait_for_outputs()


def test_interrupt_cancels_only_its_context(client, wait_for_outputs, monkeypatch):
    get = afar.get()
    thread, other_futures, outcome = get_waits_in_thread(get, client)
    try:
        running_before = set(Run._running)
        as_completed = dask.distributed.as_completed

        def interrupted(*args, **kwargs):
            # Ctrl-C reaches only the main thread: here, while it waits for its own context
            if threading.current_thread() is threading.main_thread():
                raise KeyboardInterrupt
            return as_completed(*args, **kwargs)  # pragma: no cover (the thread already waits)

        monkeypatch.setattr(dask.distributed, "as_completed", interrupted)
        with pytest.raises(KeyboardInterrupt):
            with get, afar.remotely(resources={"unavailable": 1}):
                y = 1
        monkeypatch.undo()
        # This context is cancelled...
        deadline = time.monotonic() + 10
        while set(Run._running) - running_before:
            assert time.monotonic() < deadline, "the interrupted context was not cancelled"
            time.sleep(0.01)
        # ...and the other thread's context of the same Run still waits
        assert [future.status for future in other_futures] == ["pending", "pending"]
        assert thread.is_alive()
    finally:
        get.cancel()
        thread.join(10)
    assert isinstance(outcome.get("raised"), CancelledError), outcome
    wait_for_outputs()


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


def test_remote_does_not_rebind_enclosing_variable(client):
    z = 10

    def inner():
        # `z` is a free variable here: inner only reads it
        with afar.run("z", "w") as results, afar.remotely:
            w = z + 1
        return results

    results = inner()
    assert results["w"].result() == 11
    assert z == 10


def test_remote_nonlocal(client):
    def outer():
        z = 10

        def inner():
            nonlocal z
            with afar.get, afar.remotely:
                z = 5

        inner()
        return z

    # Since Python 3.13, the body's `z` reaches `outer`, as `nonlocal` says it should
    assert outer() == (5 if sys.version_info >= (3, 13) else 10)
