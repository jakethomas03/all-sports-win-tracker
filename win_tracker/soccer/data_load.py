import sys
import os
from win_tracker.func import add_winner_column, map_teams
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, date_format, when, split

# Current notebook folder: win_tracker/soccer
notebook_dir = os.getcwd()

# Project root for imports: win_tracker/
project_root = os.path.abspath(os.path.join(notebook_dir, ".."))

# Add to sys.path if not already there
if project_root not in sys.path:
    sys.path.append(project_root)

print("Project root added to sys.path:", project_root)


# In[70]:


spark = SparkSession.builder.appName("FPL").getOrCreate()


# In[71]:


# Read in prior data
py_csv_file_path = "/Users/Jake/Documents/GitHub/all-sports-win-tracker/win_tracker/soccer/epl_final.csv"
prior_data = (
    spark.read.csv(
        py_csv_file_path,
        header=True,
        inferSchema=True,
    )
    .withColumnRenamed("home_team", "HomeTeam")
    .withColumnRenamed("away_team", "AwayTeam")
)
prior_data.show()


# In[72]:


prior_data = (
    prior_data.withColumn("FullTimeHomeGoals", col("FullTimeHomeGoals").cast("int"))
    .withColumn("FullTimeAwayGoals", col("FullTimeAwayGoals").cast("int"))
    .withColumn("HomeTeam", col("HomeTeam").cast("string"))
    .withColumn("AwayTeam", col("AwayTeam").cast("string"))
)


# In[73]:


prior_data = prior_data.select(
    "HomeTeam",
    "AwayTeam",
    "FullTimeHomeGoals",
    "FullTimeAwayGoals",
    "Season",
    "MatchDate",
)


# In[74]:


gw_data = (
    spark.read.csv(
        f"/Users/Jake/Documents/GitHub/FPL-Elo-Insights/data/2025-2026/By Gameweek/GW*/matches.csv",
        header=True,
        inferSchema=True,
    )
    .filter(col("tournament") == "prem")
    .select(
        "gameweek",
        "kickoff_time",
        "home_team",
        "home_team_elo",
        "home_score",
        "away_score",
        "away_team",
        "away_team_elo",
        "finished",
        "match_id",
        "tournament",
    )
    .withColumn("Season", col("kickoff_time").substr(1, 4))
    .withColumn("MatchDate", col("kickoff_time").substr(1, 10))
)
spark.sparkContext.setLogLevel("ERROR")


gw_data.show()


# In[75]:


from pyspark.sql.types import StringType

gw_data = gw_data.withColumn(
    "FullTimeHomeGoals", col("home_score").cast("int")
).withColumn("FullTimeAwayGoals", col("away_score").cast("int"))

gw_data.show()


# In[76]:


team_codes = {
    3.0: "Arsenal",
    7.0: "Aston Villa",
    90.0: "Burnley",
    91.0: "Bournemouth",
    94.0: "Brentford",
    36.0: "Brighton",
    21.0: "West Ham",
    39.0: "Wolves",
    8.0: "Chelsea",
    31.0: "Crystal Palace",
    11.0: "Everton",
    54.0: "Fulham",
    2.0: "Leeds",
    14.0: "Liverpool",
    43.0: "Man City",
    1.0: "Man Utd",
    4.0: "Newcastle",
    17.0: "Nott'm Forest",
    56.0: "Sunderland",
    6.0: "Spurs",
}

gw_data = map_teams(
    gw_data,
    team_codes,
    "home_team",
    "away_team",
)
gw_data.show()


full_data = prior_data.unionByName(gw_data, allowMissingColumns=True)

full_data = full_data.withColumn("day_of_week_name", date_format("MatchDate", "EEEE"))
full_data.show()
full_data.count()


full_data_soccer = add_winner_column(
    full_data,
    home_goals_col="FullTimeHomeGoals",
    away_goals_col="FullTimeAwayGoals",
    home_team_col="HomeTeam",
    away_team_col="AwayTeam",
)

# Rename columns
full_data_soccer = full_data_soccer.select(
    col("Season").alias("season"),
    col("gameweek").alias("week"),
    col("AwayTeam").alias("awayTeam"),
    col("HomeTeam").alias("homeTeam"),
    col("FullTimeAwayGoals").alias("awayPoints"),
    col("FullTimeHomeGoals").alias("homePoints"),
    col("Winner").alias("Winner"),
    col("MatchDate").alias("game_date"),
    col("day_of_week_name").alias("day_of_week_name"),
)

full_data_soccer = full_data_soccer.withColumn(
    "season", split(col("season"), "/")[0].cast("int")
)
