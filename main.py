# from win_tracker import config
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# import os
# from win_tracker.join import full_data as full_data
# from win_tracker.func import run_notebook_in_env, write_single_csv


# # Initialize Spark
# spark = SparkSession.builder.appName("Sports Analysis").getOrCreate()

# nfl_env = "nfl_env"


# nfl_nb = os.path.abspath("win_tracker/nfl/data_load.ipynb")
# cfb_nb = os.path.abspath("win_tracker/cfb/data_load.ipynb")
# epl_nb = os.path.abspath("win_tracker/soccer/data_load.ipynb")

# run_notebook_in_env(nfl_env, nfl_nb)

# # =======================
# # Set the teams
# # =======================
# config.nfl_team_to_filter = "GB"
# config.cfb_team_to_filter = "Iowa"
# config.epl_team_to_filter = "Arsenal"
# config.season_to_filter = 2020


# # =======================
# # Apply team filter for leagues that are CSV-based
# # =======================
# full_data = full_data.filter(
#     (
#         (col("homeTeam") == config.nfl_team_to_filter)
#         | (col("awayTeam") == config.nfl_team_to_filter)
#         | (col("homeTeam") == config.epl_team_to_filter)
#         | (col("awayTeam") == config.epl_team_to_filter)
#         | (col("homeTeam") == config.cfb_team_to_filter)
#         | (col("awayTeam") == config.cfb_team_to_filter)
#     )
#     & (
#         (col("season") >= config.season_to_filter)
#         & (col("Month").isin([9, 10, 11, 12, 1, 2]))
#     )
# )

# full_data.show()
# write_single_csv(full_data, "win_tracker/outputs/final_data.csv")

# # =======================


from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from win_tracker import config
from win_tracker.join import full_data
from win_tracker.func import write_single_csv

spark = SparkSession.builder.appName("Sports Analysis").getOrCreate()

# Set teams
config.nfl_team_to_filter = "GB"
config.cfb_team_to_filter = "Iowa"
config.epl_team_to_filter = "Arsenal"
config.season_to_filter = 2020

full_data_filtered = full_data.filter(
    (
        col("homeTeam").isin(
            config.nfl_team_to_filter,
            config.epl_team_to_filter,
            config.cfb_team_to_filter,
        )
        | col("awayTeam").isin(
            config.nfl_team_to_filter,
            config.epl_team_to_filter,
            config.cfb_team_to_filter,
        )
    )
    # & (col("season") >= config.season_to_filter)
)

# full_data_filtered.show()

write_single_csv(full_data_filtered, "win_tracker/outputs/final_data.csv")
