# Databricks notebook source
# MAGIC %md
# MAGIC ### TRANSFERING DATA FROM BRONZE TO SILVER

# COMMAND ----------

# MAGIC %md
# MAGIC ### INCULDING DIFFERENT REQUIRED NOTEBOOKS

# COMMAND ----------

# MAGIC %md
# MAGIC Including Notebook Named **TableAndDataMappingConfigs**

# COMMAND ----------

# MAGIC %run "./customConfig/TableAndDataMappingConfigs"

# COMMAND ----------

# MAGIC %md
# MAGIC Including Notebook Named **0_DefaultTablesCreation**

# COMMAND ----------

# MAGIC %run "./0_DefaultTablesCreation"

# COMMAND ----------

# MAGIC %md
# MAGIC Including Notebook Named **01_CommonFunctions**

# COMMAND ----------

# MAGIC %run "./01_CommonFunctions"

# COMMAND ----------

# MAGIC %md
# MAGIC Including Notebook Named **03_TransformationView**

# COMMAND ----------

# MAGIC %run "./03_TransformationView"

# COMMAND ----------

# MAGIC %md
# MAGIC ### DOWNLOADING REQUIRED MODULES FOR THE ACTION

# COMMAND ----------

# MAGIC %sh
# MAGIC python -m pip install oracledb

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGING DATA FROM TRANSFORMATION VIEW TO SILVER

# COMMAND ----------

upsertBronzeToSilver(soureTable="intercambio_common_tranformation",targetTable=silverDataBase+"."+silverCommonTable,sourceMatchingCol="CHAVE_NOTA",targetMatchingCol="CHAVE_NOTA")
upsertBronzeToSilver("intercambio_item_transformation",silverDataBase+"."+silverItemTable,"CD_SERVICO","DT_ATENDIMENTO")

# COMMAND ----------

# MAGIC %md
# MAGIC ### CHECKING THE SILVER TABLES DATA AFTER IGESTION

# COMMAND ----------

# MAGIC %md
# MAGIC Selecting the Silver DataBase

# COMMAND ----------

# MAGIC %sql
# MAGIC use intercambiosilverdb;

# COMMAND ----------

# MAGIC %md
# MAGIC Fetching Data From Item Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from IntercambioItemSilver;

# COMMAND ----------

# MAGIC %md
# MAGIC Fetching Data From Common Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from IntercambioCommonSilver;