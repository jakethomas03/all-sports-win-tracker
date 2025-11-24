# #!/usr/bin/env python
# # coding: utf-8

# # In[1]:


# import sys
# import os

# # Current notebook folder: win_tracker/soccer
# notebook_dir = os.getcwd()

# # Project root for imports: win_tracker/
# project_root = os.path.abspath(os.path.join(notebook_dir, ".."))

# # Add to sys.path if not already there
# if project_root not in sys.path:
#     sys.path.append(project_root)

# print("Project root added to sys.path:", project_root)

# from pyspark.sql import SparkSession

# spark = SparkSession.builder.getOrCreate()
# from win_tracker.soccer import data_load as prem_data_load


# prem_data_load.full_data_soccer.columns


# nfl_df = spark.read.csv(
#     "/Users/Jake/Documents/GitHub/all-sports-win-tracker/win_tracker/nfl/nfl_data.csv",
#     header=True,
#     inferSchema=True,
# )
# cfb_df = spark.read.csv(
#     "/Users/Jake/Documents/GitHub/all-sports-win-tracker/win_tracker/cfb/cfb_data.csv",
#     header=True,
#     inferSchema=True,
# )

# prem_data_load.full_data_soccer.show()
# nfl_df.show()
# cfb_df.show()

# full_data = prem_data_load.full_data_soccer.unionByName(nfl_df).unionByName(
#     cfb_df, allowMissingColumns=True
# )

# full_data = full_data.withColumn("Month", full_data.game_date.substr(6, 2).cast("int"))

# full_data.show(truncate=False)
# full_data

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, date_format, col
from win_tracker.soccer.data_load import full_data_soccer

spark = SparkSession.builder.getOrCreate()


def load_csv(path):
    df = spark.read.csv(path, header=True, inferSchema=True)
    df = df.withColumn("game_date", to_date(col("game_date")))
    df = df.withColumn("Month", date_format("game_date", "MM").cast("int"))
    return df


# Load NFL + CFB
nfl = load_csv(
    "/Users/Jake/Documents/GitHub/all-sports-win-tracker/win_tracker/nfl/nfl_data.csv"
)
cfb = load_csv(
    "/Users/Jake/Documents/GitHub/all-sports-win-tracker/win_tracker/cfb/cfb_data.csv"
)

# Union all sports
full_data = full_data_soccer.unionByName(nfl, allowMissingColumns=True).unionByName(
    cfb, allowMissingColumns=True
)

# Full schema is now consistent: game_date, homeTeam, awayTeam, homePoints, awayPoints, Winner, season, Month
full_data.show(truncate=False)
