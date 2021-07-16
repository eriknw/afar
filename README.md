# **Afar**
[![Python Version](https://img.shields.io/badge/python-3.7%20%7C%203.8%20%7C%203.9%20%7C%20PyPy-blue)](https://img.shields.io/badge/python-3.6%20%7C%203.7%20%7C%203.8%20%7C%203.9)
[![Version](https://img.shields.io/pypi/v/afar.svg)](https://pypi.org/project/afar/)
[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://github.com/eriknw/afar/blob/master/LICENSE)
[![Code style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

> **_One man's magic is another man's engineering_**<br>
> _Robert A. Heinlein_
<hr>

`afar` allows you to run code on a remote [Dask](https://dask.org/) [worker](https://distributed.dask.org/en/latest/) using context managers.  For example:
```python
import afar

with afar.run, remotely:
    import dask_cudf
    df = dask_cudf.read_parquet("s3://...")
    result = df.sum().compute()
```
Outside the context, `result` is a [Dask Future](https://docs.dask.org/en/latest/futures.html) whose data resides on a worker.  `result.result()` is necessary to copy the data locally.

By default, only the last assignment is saved.  One can specify which variables to save:
```python
with afar.run("a", "b"), remotely:
    a = 1
    b = a + 1
```
`a` and `b` are now both Futures.  They can be used directly in other `afar.run` contexts:
```python
with afar.run as data, remotely:
    c = a + b

assert c.result() == 3
assert data["c"].result() == 3
```
`data` is a dictionary of variable names to Futures.  It may be necessary at times to get the data from here.

For motivation, see https://github.com/dask/distributed/issues/4003

### *This code is highly experimental and magical!*
