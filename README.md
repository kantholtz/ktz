# KTZ - Python Tools

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
$ conda create -n ktz python=3.10
$ conda activate ktz
$ pip install -r requirements.txt
$ pip install -e .

# run tests
$ pytest

# to continually run tests
$ ptw -c

# to check code coverage
$ coverage run -m pytest
$ coverage report
$ coverage html
```


## Documentation

Code is documented. Documentation generation pending...
