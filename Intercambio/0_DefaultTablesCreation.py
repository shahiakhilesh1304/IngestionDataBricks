# Databricks notebook source
# MAGIC %md
# MAGIC ### TABLE CREATION

# COMMAND ----------

# MAGIC %md
# MAGIC ### INCULDING NOTEBOOKS TO THIS NOTEBOOK

# COMMAND ----------

# MAGIC %md
# MAGIC Including Notebook Named **TableAndDataMappingConfigs**

# COMMAND ----------

# MAGIC %run "./customConfig/TableAndDataMappingConfigs"

# COMMAND ----------

# MAGIC %md
# MAGIC ### MAJOR VARIABLES

# COMMAND ----------

# MAGIC %md
# MAGIC Data Base Variables

# COMMAND ----------

silverDataBase = DetlaLakeDatabases["SilverDatabase"]
bronzeDataBase = DetlaLakeDatabases["BronzeDatabase"]

# COMMAND ----------

# MAGIC %md
# MAGIC Table Variables

# COMMAND ----------

silverItemTable = TableName["silverItemTable"]
silverCommonTable = TableName["silverCommonTable"]
bronzeTable = TableName["bronzeTable"]

# COMMAND ----------

# MAGIC %md
# MAGIC Utility Table Variables

# COMMAND ----------

ingetionStatus = utilityTable["dataIngetionStatus"]
legacyIngetionStatus = utilityTable["dataIngetionLegacyStatus"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATING BRONZE DATABASE

# COMMAND ----------

databaseQuery = f"CREATE DATABASE IF NOT EXISTS {bronzeDataBase}"
useBronzeDBQuery = f"use {bronzeDataBase}"
spark.sql(databaseQuery)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATING SILVER DATABASE

# COMMAND ----------

databaseQuery = f"CREATE DATABASE IF NOT EXISTS {silverDataBase}"
useSilverDBQuery = f"use {silverDataBase}"
spark.sql(databaseQuery)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATE BRONZE TABLE

# COMMAND ----------

createBronzeTableQuery = '''CREATE TABLE IF NOT EXISTS {} (
DT_CARGA string,
DT_ALTERACAO TIMESTAMP,
ID_TRANSACAO bigint,
DT_TRANSACAO TIMESTAMP,
ID_STATUS_TRANSACAO bigint,
STATUS string,
CD_UNIMED_ORIGEM bigint,
UNIMED_ORIGEM string,
CD_UNIMED_DESTINO bigint,
UNIMED_DESTINO string,
CD_UNIMED_BENEFICIARIO bigint,
ID_BENEFICIARIO string,
COD_BENEFICIARIO string,
FG_RECEM_NATO string,
TIPO_PACIENTE bigint,
NOME_CONTRATADO_EXECUTANTE string,
CNES_CONTRATADO_EXECUTANTE string,
CNPJ_CONTRATADO_EXECUTANTE bigint,
TIPO_PRESTADOR string,
FG_RECURSO_PROPRIO string,
NOME_PROFISSIONAL_EXECUTANTE string,
SG_CONSELHO_PROFISSIONAL_EXECUTANTE string,
NR_CONSELHO_PROFISSIONAL_EXECUTANTE string,
SG_UF_PROFISSIONAL_EXECUTANTE bigint,
CD_CBO_PROFISSIONAL_EXECUTANTE bigint,
TIPO_CONSULTA bigint,
TIPO_ACIDENTE string,
NOME_PROFISSIONAL_SOLICITANTE string,
SG_CONSELHO_PROFISSIONAL_SOLICITANTE string,
NR_CONSELHO_PROFISSIONAL_SOLICITANTE string,
SG_UF_PROFISSIONAL_SOLICITANTE bigint,
CD_CBO_PROFISSIONAL_SOLICITANTE bigint,
CD_TECNICA_UTILIZADA string,
TIPO_PARTICIPACAO string,
TIPO_ATENDIMENTO string,
CD_CARATER_ATENDIMENTO bigint,
TIPO_ENCERRAMENTO string,
DT_EXECUCAO TIMESTAMP,
DT_ATENDIMENTO TIMESTAMP,
NR_SEQ_ITEM bigint,
CD_ITEM_UNICO string,
TIPO_TABELA bigint,
CD_SERVICO string,
TP_GUIA string,
TIPO_INTERNACAO bigint,
TIPO_REGIME_INTERNACAO string,
CD_CID string,
NR_GUIATISSPRESTADOR string,
CHAVE_NOTA string);'''.format(bronzeTable)
createIngetionStatus = '''CREATE TABLE IF NOT EXISTS {} (
queryStartDate timestamp,
queryEndDate timestamp, 
data_count bigint, 
exception string);'''.format(ingetionStatus)
createLegacyIngestionStatus = '''CREATE TABLE IF NOT EXISTS {} (queryStartDate timestamp,
queryEndDate timestamp, 
data_count bigint, 
exception string);'''.format(legacyIngetionStatus)

spark.sql(useBronzeDBQuery)
spark.sql(createBronzeTableQuery)
spark.sql(createIngetionStatus)
spark.sql(createLegacyIngestionStatus)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATE SILVER TABLE

# COMMAND ----------

silverCommonCreateTableQuery = '''CREATE TABLE IF NOT EXISTS {} (
CHAVE_NOTA string,
processStatus string,
dateCreated string,
lastUpdated string,
DT_TRANSACAO string,
STATUS string,
CD_UNIMED_ORIGEM string,
COD_BENEFICIARIO string, 
NOME_PROFISSIONAL_EXECUTANTE string, 
SG_CONSELHO_PROFISSIONAL_EXECUTANTE string, 
NR_CONSELHO_PROFISSIONAL_EXECUTANTE string,
SG_UF_PROFISSIONAL_EXECUTANTE string,
CD_CBO_PROFISSIONAL_EXECUTANTE string,
TP_GUIA string,
CD_UNIMED_DESTINO string,
FG_RECEM_NATO string,
TIPO_PACIENTE string,
TIPO_PRESTADOR string,
FG_RECURSO_PROPRIO string,
TIPO_CONSULTA string,
TIPO_ACIDENTE string,
NOME_PROFISSIONAL_SOLICITANTE string,
SG_CONSELHO_PROFISSIONAL_SOLICITANTE string,
NR_CONSELHO_PROFISSIONAL_SOLICITANTE string,
SG_UF_PROFISSIONAL_SOLICITANTE string,
CD_CBO_PROFISSIONAL_SOLICITANTE string,
CD_TECNICA_UTILIZADA string,
TIPO_PARTICIPACAO string,
CD_CARATER_ATENDIMENTO string,
TIPO_ENCERRAMENTO string,
TIPO_TABELA string,
TIPO_INTERNACAO string,
TIPO_REGIME_INTERNACAO string,
CD_CID string,
NR_GUIATISSPRESTADOR string);'''.format(silverCommonTable)
silverItemCreateTableQuery = '''CREATE TABLE IF NOT EXISTS {} (
CHAVE_NOTA string,
processStatus string,
dateCreated string,
lastUpdated string,
NOME_CONTRATADO_EXECUTANTE string,
CD_SERVICO string,
CNPJ_CONTRATADO_EXECUTANTE string,
DT_ATENDIMENTO string,
TIPO_ATENDIMENTO string)'''.format(silverItemTable)




spark.sql(useSilverDBQuery)
spark.sql(silverItemCreateTableQuery)
spark.sql(silverCommonCreateTableQuery)