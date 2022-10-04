.. KTZ Python Tools documentation master file, created by
   sphinx-quickstart on Thu Mar  3 16:40:40 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

KTZ Python Tools
================

Welcome to Kantholtz' Python toolbox. Here, common functionality is
gathered which is used throughout my different projects. Although this
mostly meant to be my own personal toolkit, maybe you find interesting
things to use too. Pull requests, questions etc. are always welcome!

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   ktz-collections
   ktz-dataclasses
   ktz-filesystem
   ktz-string


Indices and Tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


Installation
------------

Python 3.9 is required.

.. code-block:: none

   conda create -n ktz python=3.9
   conda activate ktz
   pip install ktz


For a local installation with all dev dependencies:

.. code-block:: none

   conda create -n ktz python=3.10
   conda activate ktz
   pip install -r requirements.txt
   pip install -e .

Run tests

.. code-block:: none

   pytest

Continually run tests

.. code-block:: none

   ptw -c

Check code coverage

.. code-block:: none

   coverage run -m pytest
   coverage report
   coverage html



Module Overview
---------------


.. topic:: Collections

   * :func:`ktz.collections.buckets`
     Sort sequences into buckets
   * :func:`ktz.collections.unbucket`
     Unbucket buckets
   * :func:`ktz.collections.flat`
     Flatten collections to desired depth
   * :func:`ktz.collections.Incrementer`
     Auto-assign unique ids
   * :func:`ktz.collections.drslv`
     Resolve string trails in deep dictionaries
   * :func:`ktz.collections.dflat`
     Flatten deep dictionaries
   * :func:`ktz.collections.dmerge`
     Merge deep dictionaries
   * :func:`ktz.collections.ryaml`
     Load and join yaml files

.. topic:: Dataclasses

   * :class:`ktz.dataclasses.Index`
     Create an inverted index for dataclasses
   * :class:`ktz.dataclasses.Builder`
     Build (immutable) dataclasses incrementally

.. topic:: Filesystem

   * :func:`ktz.filesystem.path`
     Shorthand utility to create pathlib.Paths
   * :func:`ktz.filesystem.path_rotate`
     Keep n previous versions of a file or directory
   * :func:`ktz.filesystem.git_hash`
     Obtain the current git hash

.. topic:: Functools

   * :class:`ktz.functools.Cascade`
     Cascade of functions with caching

.. topic:: Multiprocessing

   * :class:`ktz.multiprocessing.Actor`
     Process abstraction to be used with a Relay
   * :class:`ktz.multiprocessing.Handler`
     Execute code in the Relay's main process
   * :class:`ktz.multiprocessing.Relay`
     Wire Actors together and control the life-cycle

.. topic:: String

   * :func:`ktz.string.encode_line`
     Create a bystestring from a tuple
   * :func:`ktz.string.decode_line`
     Decode a bytestring with custom mappers
   * :func:`ktz.string.args_hash`
     Create a unique and persistent hash from args
