# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import from_json
from datetime import timedelta, date

import re
import json
import adal

# COMMAND ----------

# MAGIC %md
# MAGIC Create a connection to input source (SQL)

# COMMAND ----------

# Read credentials from Key vault
dbUser = dbutils.secrets.get(scope="analytics-kv-secrets", key = "dikeAnalyticsId")
dbPwd = dbutils.secrets.get(scope="analytics-kv-secrets", key = "dikeAnalyticsSecret")

# COMMAND ----------

GAdata = (spark.read\
  .format("jdbc")\
  .option("url", "jdbc:sqlserver://vm-im-warehouseserver-prod.database.windows.net:1433;databaseName=analytics_input")\
  .option("dbtable", "ga_data_input")\
  .option("user", dbUser + "@vm-im-warehouseserver-prod")\
  .option("password", dbPwd)\
  .load()\
)

# COMMAND ----------

GAdata = GAdata.sort('date')
GAdata.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Extract GA data (as JSON) and date when imported

# COMMAND ----------

text = json.loads(GAdata.select('content').collect()[-1]['content'])
aquire_date = GAdata.select('date').collect()[-1]['date']

# COMMAND ----------

# MAGIC %md
# MAGIC Prepare for Spark RDD: 
# MAGIC - Create column headers
# MAGIC - Populate rows

# COMMAND ----------

columns = (["aquireDate", "publishDate", text[0]['columnHeader']['dimensions'][0].split(":")[-1], text[0]['columnHeader']['dimensions'][1].split(":")[-1], text[0]['columnHeader']['metricHeader']['metricHeaderEntries'][0]['name'].split(":")[-1], text[0]['columnHeader']['metricHeader']['metricHeaderEntries'][1]['name'].split(":")[-1]])

# COMMAND ----------

rows = []
NUMBER = re.compile('^ilkkapohjalainen/11846/(/)?(?P<number>\d*).*')
START_DATE = date(year=2020,month=2,day=16)

for i in text[0]['data']['rows']:
    if NUMBER.match(i['dimensions'][0]):
        match = NUMBER.match(i['dimensions'][0])
        date_nbr = int(match.groupdict().get('number'))
        publish_date = START_DATE + timedelta(days = date_nbr)
        row = (str(aquire_date), str(publish_date), i['dimensions'][0], i['dimensions'][1], int(i['metrics'][0]['values'][0]), int(i['metrics'][0]['values'][1]))
        rows.append(row)


# COMMAND ----------

# MAGIC %md
# MAGIC Create a SparkSession object

# COMMAND ----------


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rdd = spark.sparkContext.parallelize(rows)



# COMMAND ----------


dfFromRDD1 = rdd.toDF(columns)
dfFromRDD1.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC Store data to SQL database

# COMMAND ----------

dfFromRDD1.write \
    .format("jdbc")\
    .option("url", "jdbc:sqlserver://vm-im-warehouseserver-prod.database.windows.net:1433;database=analytics_data")\
    .option("dbtable", "[google_data].[interstitial]")\
    .option("user", dbUser + "@vm-im-warehouseserver-prod")\
    .option("password", dbPwd)\
    .mode("append")\
    .save()
