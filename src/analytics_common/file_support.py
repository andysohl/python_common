# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from delta.tables import *


def create_empty_file(path: str, format: str, schema: StructType, partition_by: str = None,
                      delete: bool = False) -> None:
    """Create an empty file"""
    s = SparkSession.getActiveSession()  # type: SparkSession

    hadoop_path = s.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    hadoop_path_uri = s.sparkContext._jvm.java.net.URI(hadoop_path.toString())
    filesystem = s.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_path_uri,
                                                                         s.sparkContext._jsc.hadoopConfiguration())

    # Check if the file exists
    file_exists = False
    try:
        filesystem.getFileStatus(hadoop_path)
        file_exists = True
    except:
        pass

    # Don't recreate if the file exists, and not instructed to
    if file_exists:
        if not delete:
            return
        else:
            filesystem.delete(hadoop_path, True)

    # Create the parent directory just in case it doesn't exist
    filesystem.mkdirs(hadoop_path.getParent())

    # Create a new, empty file with a specified schema
    df = s.createDataFrame(s.sparkContext.emptyRDD(), schema=schema) \
        .write.format(format)

    # Create the partition if needed
    if partition_by is not None:
        df = df.partitionBy(partition_by)

    # Save
    df.save(path)


def delete_file(path: str) -> None:
    """Delete a file (does not need to exist)"""
    s = SparkSession.getActiveSession()  # type: SparkSession

    hadoop_path = s.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    hadoop_path_uri = s.sparkContext._jvm.java.net.URI(hadoop_path.toString())
    filesystem = s.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_path_uri,
                                                                         s.sparkContext._jsc.hadoopConfiguration())

    try:
        filesystem.delete(hadoop_path, True)
    except:
        pass


def check_file_exists(path: str) -> bool:
    """Delete a file (does not need to exist)"""
    s = SparkSession.getActiveSession()  # type: SparkSession

    hadoop_path = s.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    hadoop_path_uri = s.sparkContext._jvm.java.net.URI(hadoop_path.toString())
    filesystem = s.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_path_uri,
                                                                         s.sparkContext._jsc.hadoopConfiguration())

    # Check if the file exists
    file_exists = False
    try:
        filesystem.getFileStatus(hadoop_path)
        file_exists = True
    except:
        pass

    return file_exists


def get_path_list(path: str) -> list:
    """List the contents of a directory. Return a list of tuples containing
    (name, isDirectory)"""

    if not check_file_exists(path):
        return []

    s = SparkSession.getActiveSession()  # type: SparkSession

    hadoop_path = s.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    hadoop_path_uri = s.sparkContext._jvm.java.net.URI(hadoop_path.toString())
    filesystem = s.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_path_uri,
                                                                         s.sparkContext._jsc.hadoopConfiguration())

    # Convert the hadoop path to absolute
    hadoop_path = filesystem.getFileStatus(hadoop_path).getPath()
    hadoop_path_uri = s.sparkContext._jvm.java.net.URI(hadoop_path.toString())

    # Get the list of files
    file_list = []
    try:
        for p in filesystem.listStatus(hadoop_path):
            file_list.append( (hadoop_path_uri.relativize(p.getPath().toUri()).toString(), p.isDirectory()) )
    except:
        pass
    return file_list


def compact_delta(path: str, max_files: int = 20, target_files: int = None,
                  max_hours: int = 240, partition: str = "key") -> None:
    """Compact a delta table if it grows into too many small files"""
    s = SparkSession.getActiveSession()  # type: SparkSession

    df = s.read.format("delta").load(path) \
        .repartition(s._sc.defaultParallelism, partition) \
        .write.option("dataChange", "false").format("delta").mode("overwrite").save(path)

    delta_table = DeltaTable.forPath(s, path)
    delta_table.vacumn(max_hours)
