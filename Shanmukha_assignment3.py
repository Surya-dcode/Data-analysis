#!/usr/bin/env python
# coding: utf-8

# Importing necessary libraries with updated variable names
import seaborn as ss
import matplotlib.pyplot as mlib
import pandas as pds
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType, FloatType
from pyspark.sql.functions import col, year, current_date, count, to_timestamp, sum, when, desc


# 1. Create a Spark session
spark = SparkSession.builder.appName("ChicagoCrimeData").getOrCreate()
spark


# 2. Define the schema for loading the Chicago crime dataset (https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2/data)
chicago_crime_schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("CaseNumber", StringType(), True),
    StructField("Date", StringType(), True),
    StructField("Block", StringType(), True),
    StructField("IUCR", StringType(), True),
    StructField("PrimaryType", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("LocationDescription", StringType(), True),
    StructField("Arrest", StringType(), True),
    StructField("Domestic", StringType(), True),
    StructField("Beat", IntegerType(), True),
    StructField("District", IntegerType(), True),
    StructField("Ward", IntegerType(), True),
    StructField("CommunityArea", IntegerType(), True),
    StructField("FBICode", StringType(), True),
    StructField("XCoordinate", FloatType(), True),
    StructField("YCoordinate", FloatType(), True),
    StructField("Year", IntegerType(), True),
    StructField("UpdatedOn", TimestampType(), True),
    StructField("Latitude", FloatType(), True),
    StructField("Longitude", FloatType(), True),
    StructField("Location", StringType(), True)
])


# 3. Load the Chicago crime data (you should get more than a million rows)
spark_session = SparkSession.builder.appName("Chicago Crime Data").getOrCreate()
chicago_crimedata = spark_session.read \
    .option("delimiter", ",") \
    .csv("D:/Hadoop/assignment3") \
    .toDF(*[field.name for field in chicago_crime_schema.fields])


print(chicago_crimedata.head(2))


# 4. Clean the data:
chicago_crimedata = chicago_crimedata.na.fill("Not Available")

chicago_crimedata = chicago_crimedata.withColumn("Date", to_timestamp("Date", "MM/dd/yyyy hh:mm:ss a"))
print(chicago_crimedata.printSchema())


# 5. Filter the data for last ten years
chicago_crimedata = chicago_crimedata.where(col("Year") >= 2012)


# 6. Remove all the records with the following crime types:

crimes_to_remove = ["NON-CRIMINAL (SUBJECT SPECIFIED)", "OTHER OFFENSE", "STALKING", "NON - CRIMINAL", "ARSON"]
chicago_crimedata = chicago_crimedata.filter(~col("PrimaryType").isin(crimes_to_remove))


# a)Merge the similar crime types.
chicago_crimedata = chicago_crimedata.select([when(col("PrimaryType").isin(["SEX OFFENSE", "PROSTITUTION"]), "SEX OFFENSE/PROSTITUTION").otherwise(col(name)).alias(name) for name in chicago_crimedata.columns])


# b) Analyze the data and present results:
crime_data_yearwise = chicago_crimedata.select(year("Date").alias("Year")).groupBy("Year").agg(count("*").alias("TotalCrimes")).orderBy(col("Year").desc()).limit(11)
crime_data_yearwise.show()

'''
crime_data_yearwise_df = crime_data_yearwise.toPandas()

fig, ax = mlib.subplots(figsize=(10,4))
ax.bar(crime_data_yearwise_df['Year'], crime_data_yearwise_df['TotalCrimes'], width=0.8)
ax.set_xlabel('Year')
ax.set_ylabel('Crimes')
ax.set_title('Year-wise trend of crimes')
mlib.show()
'''


from pyspark.sql.functions import hour as hr, desc
crime_data_hourwise = transformed_crime_data.groupBy(hr(col("Date")).alias("hour")).count().orderBy(desc("count"))
crime_data_hourwise.show(10)


from pyspark.sql.functions import hour as hr, desc
chicago_crimedata_hourwise = chicago_crimedata.groupBy(hr(col("Date")).alias("hour")).count().orderBy(desc("count"))
chicago_crimedata_hourwise.show()
#crime_data_hourwise_df = chicago_crimedata_hourwise.toPandas()

'''
fig, ax = mlib.subplots(figsize=(10,2))
ax.bar(crime_data_hourwise_df['hour'], crime_data_hourwise_df['count'], width=0.8)
ax.set_xlabel('Hour')
ax.set_ylabel('Crimes')
ax.set_title('Hour of the day')
mlib.show()
'''


from pyspark.sql.functions import desc
crime_data_top10 = chicago_crimedata.groupBy("PrimaryType").count().orderBy(desc("count")).limit(10)
crime_data_top10.show()
'''
crime_data_top10_df = crime_data_top10.toPandas()
crime_data_top10_df
fig, ax = mlib.subplots(figsize=(20,6))
ax.bar(crime_data_top10_df['PrimaryType'], crime_data_top10_df['count'], width=0.8)
ax.set_xlabel('Crime Type')
ax.set_ylabel('Crime rate')
ax.set_title('Crimes')
mlib.show()
'''


