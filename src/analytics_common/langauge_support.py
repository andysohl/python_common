# -*- coding: utf-8 -*-
"""Support Spark pattern matching.

Warning: this must use Python 3.4 or higher
"""
from ._globals import *
import pyspark.sql.functions as f
from pyspark.sql.types import DecimalType, LongType
from pyspark.sql.column import Column


def key_to_int64(value: str) -> int:
    """To convert a hex string back to a 64 bit signed integer that's compatible with Java

    If this is not used, Python will create a positive integer > 64 bits which will cause an overflow"""
    iv = int(value, base=16)
    return -(iv & 0x8000000000000000) | (iv & 0x7fffffffffffffff)


def int64_to_key(value: int) -> str:
    """To convert a 64 bit integer to a hex value"""
    return hex(value)[2:].zfill(16)


def spark_key_to_int64(col: Column) -> Column:
    """Convert a spark key column to a signed long"""
    return f.conv(col, 16, 10).cast(DecimalType(30, 0)).cast(LongType())


def spark_int64_to_key(col: Column) -> Column:
    """Convert a spark long to a key"""
    return f.lpad(f.hex(col), 16, "0")


def spark_cols_to_key(*cols: Column) -> Column:
    """Convert a set of columns to a key"""
    return spark_int64_to_key(f.xxhash64(*[cn for cn in cols]))
