# **Afar**
[![Python Version](https://img.shields.io/badge/python-3.7%20%7C%203.8%20%7C%203.9%20%7C%20PyPy-blue)](https://img.shields.io/badge/python-3.6%20%7C%203.7%20%7C%203.8%20%7C%203.9)
[![Version](https://img.shields.io/pypi/v/afar.svg)](https://pypi.org/project/afar/)
[![License](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://github.com/eriknw/afar/blob/main/LICENSE)
[![Build Status](https://github.com/eriknw/afar/workflows/Test/badge.svg)](https://github.com/eriknw/afar/actions)
[![Coverage Status](https://coveralls.io/repos/eriknw/afar/badge.svg?branch=main)](https://coveralls.io/r/eriknw/afar)
[![Code style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

> **_One man's magic is another man's engineering_**<br>
> _Robert A. Heinlein_
<hr>

**To install:** `pip install afar`

`afar` allows you to run code on a remote [Dask](https://dask.org/) [cluster](https://distributed.dask.org/en/latest/) using context managers.  For example:
```python
import afar
from dask.distributed import Client
client = Client()

with afar.run, remotely:
    import dask_cudf
    df = dask_cudf.read_parquet("s3://...")
    result = df.sum().compute()
```
Outside the context, `result` is a [Dask Future](https://docs.dask.org/en/latest/futures.html) whose data resides on a worker.  `result.result()` is necessary to copy the data locally.

By default, only the last assignment is saved.  One can specify which variables to save:
```python
with afar.run("one", "two"), remotely:
    one = 1
    two = one + 1
```
`one` and `two` are now both Futures.  They can be used directly in other `afar.run` contexts:
```python
with afar.run as data, remotely:
    three = one + two

assert three.result() == 3
assert data["three"].result() == 3
```
`data` above is a dictionary of variable names to Futures.  It may be necessary at times to get the data from here.  Alternatively, you may pass a mapping to `afar.run` to use as the data.
```python
run = afar.run(data={"four": 4})
with run, remotely:
    seven = three + four
assert run.data["seven"].result() == 7
```
If you want to automatically gather the data locally (to avoid calling `.result()`), use `afar.get` instead of `afar.run`:
```python
with afar.get, remotely:
    five = two + three
assert five == 5
```
If using IPython/Jupyter, the rich repr of the final expression will be displayed if it's not an assignment:
```python
with afar.run, remotely:
    three + seven
# displays 10!
```

### Is this a good idea?

I don't know!

For motivation, see https://github.com/dask/distributed/issues/4003

It's natural to be skeptical of unconventional syntax.  Often times, I don't think it's obvious whether new syntax will be nice to use, and you really just need to try it out and see.

We're still exploring the usability of `afar`.  If you try it out, please share what you think, and ask yourself questions such as:
- can we spell anything better?
- does this offer opportunities?
- what is surprising?
- what is lacking?

Here's an example of an opportunity:
```python
on_gpus = afar.remotely(resources={"GPU": 1})

with afar.run, on_gpus:
    ...
```
This now works!  Keyword arguments to `remotely` will be passed to [`client.submit`](https://distributed.dask.org/en/latest/api.html#distributed.Client.submit).

I don't know about you, but I think this is starting to look and feel kinda nice, and it could probably be even better :)
### *This code is highly experimental and magical!*
