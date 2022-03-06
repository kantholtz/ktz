# KTZ - Python Tools

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![master checks](https://github.com/kantholtz/ktz/actions/workflows/development.yml/badge.svg)

My personal Python toolbox.


## Installation

Python 3.9 is required.

``` console
$ conda create -n ktz python=3.9
$ conda activate ktz
$ pip install ktz
```

For a local installation with all dev dependencies:

``` console
$ git clone https://github.com/kantholtz/ktz.git
$ cd ktz
$ conda create -n ktz python=3.9
$ conda activate ktz
$ pip install .[dev]

# run tests
$ pytest

# to continually run tests
$ ptw -c

# to check code coverage
$ coverage run -m pytest
$ coverage report
$ coverage html
```
