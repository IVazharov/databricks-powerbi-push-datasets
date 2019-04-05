# Databricks notebook source
# DBTITLE 1,Load the pbiDatasetAPI Class
# MAGIC %run "./pbiDatasetAPI"

# COMMAND ----------

# DBTITLE 1,Create and populate a DataFrame with demo data
dfInject = spark.read.parquet('dbfs:/databricks-datasets/amazon/test4K')
dfInject = dfInject.select("brand", "img", "price", "rating", "review", "time").limit(200)

# COMMAND ----------

# DBTITLE 1,Initialise variables and create a pbiDatasetAPI class instance
# Initialise variables
username = "<USER_EMAIL>"
password = "<USER_PASSWORD>"
application_id = "********-****-****-****-************"  # Power BI application ID
groupId = "********-****-****-****-************"  # Set to None if not using Power BI groups
datasetName = "InjectedDataset"  # The name of the Power BI dataset
tableNames = ["AmazonReviews"]  # Tables name or list of table names
dataFrames = [dfInject]  # DataFrame name or list of DataFrame names

# Create a pbiDatasetAPI class instance
pbi = pbiDatasetAPI(username, password, application_id)

# COMMAND ----------

# DBTITLE 1,Create dataset and table in Power BI
# Create the dataset and the table in PBI
pbi.executePBIOperation("postdataset", groupId = groupId, datasetName = datasetName, tableNames = tableNames, dataFrames = dataFrames, reCreateIfExists = True)

# COMMAND ----------

# DBTITLE 1,Post rows to the table in the dataset
# Get the datasetKey for the dataset (by name)
datasetKey = pbi.executePBIOperation("getdatasetbyname", groupId = groupId, datasetName = datasetName)[0]["id"]

# Insert the contents of the DataFrame in the PBI dataset table
pbi.executePBIOperation("postrows", groupId = groupId, datasetKey = datasetKey, tableNames = tableNames, dataFrames = dataFrames)

# COMMAND ----------

# DBTITLE 1,Change the data type of a DataFrame column and update the metadata in Power BI
# Change the data type of the column 'rating' from double to string
dfInjectModified = dfInject.withColumn("rating", dfInject.rating.cast("string"))

# Get the datasetKey for the dataset (by name)
datasetKey = pbi.executePBIOperation("getdatasetbyname", groupId = groupId, datasetName = datasetName)[0]["id"]

# Update the metadata of the Power BI table
pbi.executePBIOperation("puttable", groupId = groupId, datasetKey = datasetKey, tableNames = tableNames, dataFrames = [dfInjectModified])

# COMMAND ----------

# DBTITLE 1,Delete all rows from the table
# Get the datasetKey for the dataset (by name)
datasetKey = pbi.executePBIOperation("getdatasetbyname", groupId = groupId, datasetName = datasetName)[0]["id"]

# Delete all rows from the table(s)
pbi.executePBIOperation("deleterows", groupId = groupId, datasetKey = datasetKey, tableNames = tableNames)

# COMMAND ----------

# DBTITLE 1,Delete the dataset
# Get the datasetKey for the dataset (by name)
datasetKey = pbi.executePBIOperation("getdatasetbyname", groupId = groupId, datasetName = datasetName)[0]["id"]

# Delete the dataset
pbi.executePBIOperation("deletedatasetbyid", groupId = groupId, datasetKey = datasetKey)