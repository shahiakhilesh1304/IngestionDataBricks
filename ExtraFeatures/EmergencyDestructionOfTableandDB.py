# Databricks notebook source
# MAGIC %md
# MAGIC ### USE ONLY WHEN YOU WANT TO DESTROY THE DATABASE AND TABLEE WITHOUT KEEPING ANY DATA 

# COMMAND ----------

# MAGIC %md
# MAGIC ### USE BRONZE DB

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE intercambiobronzedb;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW intercambio_common_tranformation

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW intercambio_item_transformation

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE intercambio_dataingetion_status;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE intercambio_dataingetion_status_legacy

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE intercambiobronze

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE intercambiobronzedb

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### USE SILVER DB

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DATABASE intercambiosilverdb

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE intercambiocommonsilver;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE intercambioitemsilver

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE intercambiosilverdb;