# -*- coding: utf-8 -*-
import logging
from pyspark.sql import SparkSession
import sys

# The default parallelism
__DEFAULT_PARALLELISM = 4


def configure_spark(s: SparkSession):
    """Configure a Spark session"""
    global __DEFAULT_PARALLELISM

    # Configure spark (default to num of Cores
    __DEFAULT_PARALLELISM = s._sc.defaultParallelism
    s.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 100000)
    s.conf.set("spark.sql.shuffle.partitions", __DEFAULT_PARALLELISM)
    s._jsparkSession.sessionState().conf().setConf(
        s._jvm.org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS(), __DEFAULT_PARALLELISM)

    # Configure pandas
    lpd = globals().get("pd") or globals().get("pandas")
    if lpd is not None:
        lpd.set_option('display.max_rows', 1000)
        lpd.set_option('display.max_columns', None)
        lpd.set_option('display.width', None)
        lpd.set_option('display.max_colwidth', None)

    # Set the basic configuration for logging, send to stdout
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO, stream=sys.stdout)
    logging.getLogger("py4j.java_gateway").setLevel(level=logging.ERROR)

def get_parallelism() -> int:
    """Get the configured parallelism"""
    global __DEFAULT_PARALLELISM
    return __DEFAULT_PARALLELISM


def set_parallelism(s: SparkSession, level: int):
    """Set the configured parallelism"""
    if s is None or level is None:
        return

    global __DEFAULT_PARALLELISM
    __DEFAULT_PARALLELISM = level

    s.conf.set("spark.sql.shuffle.partitions", __DEFAULT_PARALLELISM)
    s._jsparkSession.sessionState().conf().setConf(
        s._jvm.org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS(), __DEFAULT_PARALLELISM)


def create_logger(s: SparkSession, name: str):
    """Create a Spark logger"""
    return s._jvm.org.apache.log4j.LogManager.getLogger(name)
