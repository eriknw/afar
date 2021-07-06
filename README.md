# **Afar**
[![Python Version](https://img.shields.io/badge/python-3.7%20%7C%203.8%20%7C%203.9%20%7C%20PyPy-blue)](https://img.shields.io/badge/python-3.6%20%7C%203.7%20%7C%203.8%20%7C%203.9)
[![Version](https://img.shields.io/pypi/v/afar.svg)](https://pypi.org/project/afar/)
[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://github.com/eriknw/afar/blob/master/LICENSE)
[![Code style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

> **_One man's magic is another man's engineering_**<br>
> _Robert A. Heinlein_
<hr>

`afar` explores new syntax around context managers.  For example:
```python
import afar
with afar.run() as results, locally:
    x = 1
    y = x + 1

>>> results.x
1
>>> results.y
2
```
Soon, we will be able to run code on a remote [dask](https://dask.org/) worker with syntax like:
```python
with afar.run() as result, remotely:
    import dask_cudf
    df = dask_cudf.read_parquet("s3://...")
    result = df.sum().compute()
```
For motivation, see https://github.com/dask/distributed/issues/4003

### *This code is highly experimental and magical!*
