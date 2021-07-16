import subprocess
import sys

# TODO: better testing infrastructure
if __name__ == "__main__":
    import afar
    from operator import add
    from dask.distributed import Client

    client = Client()
    two = client.submit(add, 1, 1)

    with afar.run as results, afar.remotely:
        three = two + 1
    assert three.result() == 3

    with afar.get, afar.remotely(priority=1):
        five = two + three
    assert five == 5


def test_runme():
    assert subprocess.check_call([sys.executable, __file__]) == 0
