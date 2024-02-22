from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col, count, desc, asc, rank
from pyspark.sql.window import Window
schema = StructType([
    StructField("Person", StringType(), True),
    StructField("Action", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Gender", StringType(), True),
    StructField("Age", IntegerType(), True),
])

spark = SparkSession.builder.appName("EndtermBDSpark").getOrCreate()
path = "police_fatality_with_header.csv"
df = spark.read.csv(path, header=True, schema=schema)

action_counts = df.groupBy("City", "Action").count()

windowSpecDesc = Window.partitionBy("City").orderBy(desc("count"))
windowSpecAsc = Window.partitionBy("City").orderBy(asc("count"))

ranked_actions_desc = action_counts.withColumn("rank_desc", rank().over(windowSpecDesc))
ranked_actions_asc = action_counts.withColumn("rank_asc", rank().over(windowSpecAsc))

#most popular death reason
most_popular_actions = ranked_actions_desc.filter(col("rank_desc") == 1) \
    .select("City", "Action", "count") \
    .orderBy(desc("count"))
#least popular death reason
least_popular_actions = ranked_actions_asc.filter(col("rank_asc") == 1) \
    .select("City", "Action", "count") \
    .orderBy(asc("count"))

print("Most Popular Actions by City (sorted by count descending):")
most_popular_actions.show(truncate=False)
print("Least Popular Actions by City (sorted by count ascending):")
least_popular_actions.show(truncate=False)