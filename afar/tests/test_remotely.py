import afar
from operator import add
from dask.distributed import Client

# TODO: better testing infrastructure
if __name__ == "__main__":
    client = Client()
    two = client.submit(add, 1, 1)

    with afar.run, remotely:
        three = two + 1

    assert three.result() == 3
