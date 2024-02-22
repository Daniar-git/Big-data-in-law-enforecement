from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, asc, rank
spark = SparkSession.builder.appName("EndtermBDSpark").getOrCreate()
path = "police_fatality_with_header.csv"
df = spark.read.csv(path, header=True, inferSchema=True)
#to count deaths in each state
action_counts = df.groupBy("City", "Action").count()

windowSpecDesc = Window.partitionBy("City").orderBy(desc("count"))
windowSpecAsc = Window.partitionBy("City").orderBy(asc("count"))
#making top most and less popular deaths
ranked_actions_desc = action_counts.withColumn("rank_desc", rank().over(windowSpecDesc))
ranked_actions_asc = action_counts.withColumn("rank_asc", rank().over(windowSpecAsc))

#most popular death reason
most_popular_actions = ranked_actions_desc.filter(col("rank_desc") == 1).select("City", "Action", "count").orderBy("City")
#least popular death reason
least_popular_actions = ranked_actions_asc.filter(col("rank_asc") == 1).select("City", "Action", "count").orderBy("City")

print("Most Popular Actions by City:")
most_popular_actions.show(truncate=False)
print("Least Popular Actions by City:")
least_popular_actions.show(truncate=False)