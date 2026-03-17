"""
PySpark Worker Bootstrap — Python 3.13+ typing.io compatibility patch.

Spark executors launch fresh Python processes that import pyspark.broadcast,
which tries `from typing.io import BinaryIO`. This was removed in Python 3.13.
This sitecustomize.py runs BEFORE any imports and patches the module namespace.
"""
import sys
import typing

if sys.version_info >= (3, 13) and not hasattr(typing, 'io'):
    from typing import BinaryIO

    class _MockIO:
        BinaryIO = BinaryIO

    _mock = _MockIO()
    typing.io = _mock
    sys.modules['typing.io'] = _mock
