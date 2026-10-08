import builtins
import time

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--innerscope-method",
        action="store",
        default=None,
        choices=["bytecode", "trace"],
        help=(
            "Select innerscope's default method, here and on the workers of the test cluster "
            "(default: innerscope's default for this Python)"
        ),
    )


def set_innerscope_method(method):
    import innerscope

    innerscope.cfg.default_method = method


def pytest_configure(config):
    method = config.getoption("--innerscope-method")
    if method is not None:
        set_innerscope_method(method)


@pytest.fixture(scope="session")
def client(pytestconfig):
    from dask.distributed import Client

    with Client(n_workers=2, threads_per_worker=1, dashboard_address=":0") as client:
        method = pytestconfig.getoption("--innerscope-method")
        if method is not None:
            client.run(set_innerscope_method, method)
        yield client


@pytest.fixture
def wait_for_outputs():
    """Wait until afar has handled the print and display events of every remote context"""
    from afar._core import Run

    def wait(timeout=10):
        deadline = time.monotonic() + timeout
        while Run._outputs or Run._running:
            if time.monotonic() > deadline:
                raise TimeoutError(f"afar outputs still pending: {Run._outputs}")
            time.sleep(0.01)

    return wait


@pytest.fixture(scope="module")
def ip(tmp_path_factory):
    """An in-process IPython shell that does not touch the user's IPython directory"""
    pytest.importorskip("IPython")
    from IPython.core.interactiveshell import InteractiveShell
    from traitlets.config import Config

    config = Config()
    config.HistoryManager.hist_file = ":memory:"
    builtins_before = set(vars(builtins))
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("IPYTHONDIR", str(tmp_path_factory.mktemp("ipython")))
        shell = InteractiveShell.instance(config=config)
        try:
            yield shell
        finally:
            InteractiveShell.clear_instance()
            # The shell adds `__IPYTHON__` and others to builtins, which would make other tests
            # behave as if they ran in IPython.
            for name in set(vars(builtins)) - builtins_before:
                delattr(builtins, name)
