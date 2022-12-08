# Databricks notebook source
# MAGIC %sh
# MAGIC curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# MAGIC curl https://packages.microsoft.com/config/ubuntu/16.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
# MAGIC sudo apt-get update
# MAGIC sudo ACCEPT_EULA=Y apt-get -q -y install msodbcsql17

# COMMAND ----------

import logging

import pandas as pd
import pyodbc

from sklearn.preprocessing import scale
from sklearn.cluster import KMeans


# COMMAND ----------



# COMMAND ----------

# Read credentials from Key vault
dbUser = dbutils.secrets.get(scope="analytics-kv-secrets", key = "dikeAnalyticsId")
dbPwd = dbutils.secrets.get(scope="analytics-kv-secrets", key = "dikeAnalyticsSecret")

# COMMAND ----------

sql = 'SELECT * FROM [aggregations].[segments]'

# COMMAND ----------

conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};Server=tcp:vm-im-warehouseserver-prod.database.windows.net,1433;Database=sqldw-im-analytics-warehouse-prod;Uid='+ dbUser +';Pwd='+ dbPwd +';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
SQL_query = pd.read_sql_query(sql, conn)
df = pd.DataFrame(SQL_query)
df.shape

# COMMAND ----------

df.dtypes

# COMMAND ----------

## Data cleaning
df = df.drop_duplicates()
df = df.dropna()
df = df[df['age']<110]
df = df[df['age']>18]
df.shape

# COMMAND ----------

### Calculations / transformations
for ind in df.index:
    # Duration in hours 
    df['duration'] = df['last'].sub(df['first']).dt.total_seconds().div(60)
df

# COMMAND ----------

df.dtypes

# COMMAND ----------

## Model training
#kmeans_df = df.drop(columns=['account_id','gender', 'health', 'technology', 'general', 'sports', 'law_enforcement', 'politics', 'business', 'entertainment', 'science'])
kmeans_df = df[['age','visits', 'health', 'technology', 'general', 'sports', 'law_enforcement', 'politics', 'business', 'entertainment', 'science']]
try:
    X = scale(kmeans_df)
except Exception as e:
    logging.error('scale()')
    logging.error(e)
    logging.exception('scaling fails:')

try:
    clustering = KMeans(n_clusters=9, random_state=7)
except Exception as e:
    logging.error('KMeans()')
    logging.error(e)
    logging.exception('KMeans fails:')

try:
    clustering.fit(X)
except Exception as e:
    logging.error('fit()')
    logging.error(e)
    logging.exception('clustering.fit fails:')


# COMMAND ----------

inverse_transform(X)

# COMMAND ----------

clustering.cluster_centers_

# COMMAND ----------

# Add segment predictions to the dataframe
df['segment'] = clustering.labels_
df



# COMMAND ----------

# Divide into different segment dataframes
df_0 = df.query("segment==0")
df_1 = df.query("segment==1")
df_2 = df.query("segment==2")

# Views per segment per category
s0_views = [df_0['health'].sum(), df_0['technology'].sum(), df_0['general'].sum(), df_0['sports'].sum(), df_0['law_enforcement'].sum(), df_0['politics'].sum(), df_0['business'].sum(), df_0['entertainment'].sum(), df_0['science'].sum()]
s1_views = [df_1['health'].sum(), df_1['technology'].sum(), df_1['general'].sum(), df_1['sports'].sum(), df_1['law_enforcement'].sum(), df_1['politics'].sum(), df_1['business'].sum(), df_1['entertainment'].sum(), df_1['science'].sum()]
s2_views = [df_2['health'].sum(), df_2['technology'].sum(), df_2['general'].sum(), df_2['sports'].sum(), df_2['law_enforcement'].sum(), df_2['politics'].sum(), df_2['business'].sum(), df_2['entertainment'].sum(), df_2['science'].sum()]


# COMMAND ----------

s0_views

# COMMAND ----------

views_per_category = pd.DataFrame(columns=['category', 's0 = forget', 's1 = retain', 's2 = focus'])

categories = ['health', 'technology', 'general', 'sports', 'law_enforcement', 'politics', 'business', 'entertainment', 'science']
views_per_category['category'] = categories
views_per_category['s0 = forget'] = s0_views
views_per_category['s1 = retain'] = s1_views
views_per_category['s2 = focus'] = s2_views

return_csv = views_per_category.to_csv(sep='|', index=False)


# COMMAND ----------

return_csv
