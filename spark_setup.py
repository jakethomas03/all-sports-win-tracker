# win_tracker/spark_setup.py
import os
from pyspark.sql import SparkSession


def get_spark(app_name="Sports Analysis", driver_memory="12g", python_env_path=None):
    """
    Returns a SparkSession configured for a specific Python environment and driver memory.

    Parameters:
        app_name (str): Spark application name.
        driver_memory (str): Amount of memory for Spark driver (e.g., "8g").
        python_env_path (str): Full path to Python executable for PySpark (e.g., nfl_env).

    Returns:
        SparkSession
    """
    if python_env_path:
        os.environ["PYSPARK_PYTHON"] = python_env_path
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_env_path

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .getOrCreate()
    )

    return spark
