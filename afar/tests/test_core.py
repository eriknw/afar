import afar
import pytest
from pytest import raises


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
    assert results == {'y': 2}
    # fmt: on


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


def test_end_of_file():
    data = {}
    end_of_file(data)
    assert data == {"y": 20}


def end_of_file(data):
    with afar.run(data=data), locally:
        x = 10
        y = 2 * x
