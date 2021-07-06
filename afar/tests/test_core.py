import afar
import pytest
from pytest import raises


def test_a_modest_beginning():
    with afar.run(), remotely:
        pass

    with afar.run(), afar.remotely:
        pass

    with afar.run(), locally:
        pass

    with afar.run(), afar.locally:
        pass

    with raises(NameError, match="remotelyblah"):
        with afar.run(), remotelyblah:
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
    assert results.x == 1
    assert results.y == 12
    assert not hasattr(results, "w")
    assert not hasattr(results, "z")

    with afar.run() as results, afar.locally:
        x = z
        y = x + 1
    assert results.x == 1
    assert results.y == 2

    # fmt: off
    with \
        afar.run() as results, \
        locally \
    :
        x = z
        y = x + 1
    assert results.x == 1
    assert results.y == 2
    # fmt: on
