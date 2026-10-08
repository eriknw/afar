"""afar in IPython: displaying the final expression, magics, and finding the source of cells."""

import pytest

pytest.importorskip("IPython")


def run_cell(ip, cell):
    result = ip.run_cell(cell, store_history=True)
    result.raise_error()
    return result


@pytest.fixture(scope="module")
def ip_afar(ip):
    run_cell(ip, "import afar\n%load_ext afar\nhere = afar.locally")
    return ip


def test_display_final_expression_locally(ip_afar, capsys):
    run_cell(ip_afar, "with afar.run, afar.locally:\n    x = 40\n    x + 2\n")
    assert capsys.readouterr().out == "42\n"


@pytest.mark.parametrize(
    "body",
    [
        "x = 1",
        "x = 1\nif x:\n    x + 1",
        "print('hi')",  # `print` returns None, which is not displayed
    ],
)
def test_no_display_locally(ip_afar, capsys, body):
    cell = "with afar.run, afar.locally:\n" + "".join(f"    {line}\n" for line in body.split("\n"))
    run_cell(ip_afar, cell)
    assert capsys.readouterr().out in {"", "hi\n"}


def test_cell_magic_locally(ip_afar):
    run_cell(ip_afar, "%%afar -w here x, y\nx = 1\ny = x + 1\nz = 3\n")
    assert ip_afar.user_ns["x"] == 1
    assert ip_afar.user_ns["y"] == 2
    assert "z" not in ip_afar.user_ns


def test_line_magic_locally(ip_afar, capsys):
    ip_afar.user_ns["y"] = 2
    run_cell(ip_afar, "w = %afar -w here y + 1\n")
    assert ip_afar.user_ns["w"] == 3
    # The final expression is also displayed
    assert capsys.readouterr().out == "3\n"


def test_time_magic(ip_afar):
    # The source of a `%%time` cell is not in linecache, so afar finds it in the history
    run_cell(ip_afar, "%%time\nwith afar.run as results, afar.locally:\n    t = 5\n")
    assert ip_afar.user_ns["results"] == {"t": 5}


def test_new_magic(ip_afar):
    from afar._magic import new_magic

    new_magic("afar_here", where=ip_afar.user_ns["here"])
    run_cell(ip_afar, "%%afar_here q\nq = 5\n")
    assert ip_afar.user_ns["q"] == 5
    assert "-w where" not in ip_afar.find_cell_magic("afar_here").__doc__


@pytest.mark.parametrize(
    ("cell", "match"),
    [
        ("%%afar -r not_defined\nx = 1\n", "not found in local namespace"),
        ("%%afar -w not_defined\nx = 1\n", "not found in local namespace"),
        ("%%afar -w here -w here\nx = 1\n", None),
        ("%%afar -w here 1x\nx = 1\n", "bad variable names"),
        ("not_a_where = 1", None),
        ("%%afar -w not_a_where\nx = 1\n", "must be of type Where"),
    ],
)
def test_magic_usage_errors(ip_afar, cell, match):
    from IPython.core.error import UsageError

    result = ip_afar.run_cell(cell, store_history=True)
    if match is None:
        return
    assert isinstance(result.error_in_exec, UsageError)
    assert match in str(result.error_in_exec)


def test_remote_cell_and_line_magic(ip_afar, client, capsys, wait_for_outputs):
    from dask.distributed import Future

    run_cell(ip_afar, "%%afar x, y\nx = 1\ny = x + 1\n")
    assert isinstance(ip_afar.user_ns["x"], Future)
    assert ip_afar.user_ns["y"].result() == 2

    # `run` returns a future of the final expression; `get` returns its value
    run_cell(ip_afar, "z = %afar x + y\n")
    assert ip_afar.user_ns["z"].result() == 3
    run_cell(ip_afar, "w = %afar -g x + y + 1\n")
    assert ip_afar.user_ns["w"] == 4
    wait_for_outputs()
    # Both final expressions are displayed asynchronously, in either order
    assert sorted(capsys.readouterr().out.split()) == ["3", "4"]


def test_remote_display_and_print(ip_afar, client, capsys, wait_for_outputs):
    run_cell(ip_afar, "with afar.run, afar.remotely:\n    print('hello')\n    40 + 2\n")
    wait_for_outputs()
    assert capsys.readouterr().out == "hello\n42\n"
