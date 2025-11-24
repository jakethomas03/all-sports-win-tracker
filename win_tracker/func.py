from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import when, col
from pyspark.sql.types import StringType
import subprocess
import sys
import os


def add_winner_column(
    df: DataFrame,
    home_goals_col: str,
    away_goals_col: str,
    home_team_col: str,
    away_team_col: str,
    output_col: str = "Winner",
) -> DataFrame:
    """
    Adds a winner column to any DataFrame based on home/away goals and team names.
    Works with arbitrary column names.
    """
    return df.withColumn(
        output_col,
        when(col(home_goals_col) > col(away_goals_col), col(home_team_col))
        .when(col(away_goals_col) > col(home_goals_col), col(away_team_col))
        .otherwise("Draw"),
    )


def map_teams(
    df: DataFrame, mapping: dict, home_col: str = "HomeTeam", away_col: str = "AwayTeam"
) -> DataFrame:
    """
    Maps HomeTeam and AwayTeam columns using the given mapping dictionary.
    Handles names → codes or codes → names automatically.

    Args:
        df: PySpark DataFrame with HomeTeam and AwayTeam columns.
        mapping: dict with key → value replacements.
        home_col: name of the home team column.
        away_col: name of the away team column.

    Returns:
        DataFrame with mapped HomeTeam and AwayTeam columns.
    """
    # Cast columns to string to avoid mixed type issues
    df = df.withColumn(home_col, col(home_col).cast(StringType()))
    df = df.withColumn(away_col, col(away_col).cast(StringType()))

    for key, val in mapping.items():
        df = df.withColumn(
            home_col, when(col(home_col) == str(key), str(val)).otherwise(col(home_col))
        ).withColumn(
            away_col, when(col(away_col) == str(key), str(val)).otherwise(col(away_col))
        )
    return df


def run_notebook_in_env(env_name, notebook_path):
    """
    Run a notebook in a specific conda environment.
    """
    subprocess.run(
        [
            "conda",
            "run",
            "-n",
            env_name,
            "jupyter",
            "nbconvert",
            "--to",
            "notebook",
            "--execute",
            "--inplace",
            notebook_path,
        ],
        check=True,
    )


import os
import shutil


def write_single_csv(df, output_path, header=True):
    folder, file_name = os.path.split(output_path)
    tmp_folder = os.path.join(folder, "tmp_csv_output")

    # Create folder if it doesn't exist
    os.makedirs(folder, exist_ok=True)

    # Coalesce to 1 partition and write to temp folder
    df.coalesce(1).write.mode("overwrite").csv(tmp_folder, header=header)

    # Find the part file
    part_file = [f for f in os.listdir(tmp_folder) if f.startswith("part-")][0]

    # Remove output file if it already exists
    if os.path.exists(output_path):
        os.remove(output_path)

    # Move and rename
    shutil.move(os.path.join(tmp_folder, part_file), output_path)

    # Remove temp folder
    shutil.rmtree(tmp_folder)

    print(f"CSV written to: {output_path}")
