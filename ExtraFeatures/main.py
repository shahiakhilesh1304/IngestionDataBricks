# Databricks notebook source
# MAGIC %md
# MAGIC ### Running All NoteBooks In this Book Successfully

# COMMAND ----------

# MAGIC %run "../customConfig/TableAndDataMappingConfigs"

# COMMAND ----------

# MAGIC %run "../0_DefaultTablesCreation"

# COMMAND ----------

# MAGIC %run "../01_CommonFunctions"

# COMMAND ----------

# MAGIC %run "../02_Source_To_Bronze"

# COMMAND ----------

# MAGIC %run "../03_TransformationView"

# COMMAND ----------

# MAGIC %run "../04_Bronze_To_Silver"

# COMMAND ----------

# MAGIC %run "../05_Silver_To_DataQueue"