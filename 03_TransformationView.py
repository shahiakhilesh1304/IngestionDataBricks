# Databricks notebook source
# MAGIC %md
# MAGIC ### TRANSFORMATION VIEW
# MAGIC 
# MAGIC Creating a transformation view with two views One which will store common Datas and Once which will store Items

# COMMAND ----------

# MAGIC %md
# MAGIC ### INCLUDING DIFFERENT NOTEBOOKS

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
# MAGIC ### Changing the DATABASE we are have been using previously to the Bronze Database

# COMMAND ----------

spark.sql(useBronzeDBQuery)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATING THE VIEW
# MAGIC 
# MAGIC Creating two views Common View and Item View

# COMMAND ----------

createCommonViewQuery = '''Create or Replace view intercambio_common_tranformation as 
Select 
bronze.CHAVE_NOTA CHAVE_NOTA,
'Pending' processStatus,
Current_Timestamp() dateCreated,
Current_Timestamp() lastUpdated,
bronze.DT_TRANSACAO DT_TRANSACAO,
bronze.STATUS STATUS,
bronze.CD_UNIMED_ORIGEM CD_UNIMED_ORIGEM,
bronze.COD_BENEFICIARIO COD_BENEFICIARIO, 
bronze.NOME_PROFISSIONAL_EXECUTANTE NOME_PROFISSIONAL_EXECUTANTE, 
bronze.SG_CONSELHO_PROFISSIONAL_EXECUTANTE SG_CONSELHO_PROFISSIONAL_EXECUTANTE, 
bronze.NR_CONSELHO_PROFISSIONAL_EXECUTANTE NR_CONSELHO_PROFISSIONAL_EXECUTANTE,
bronze.SG_UF_PROFISSIONAL_EXECUTANTE SG_UF_PROFISSIONAL_EXECUTANTE,
bronze.CD_CBO_PROFISSIONAL_EXECUTANTE CD_CBO_PROFISSIONAL_EXECUTANTE,
bronze.TP_GUIA TP_GUIA,
bronze.CD_UNIMED_DESTINO CD_UNIMED_DESTINO,
bronze.FG_RECEM_NATO FG_RECEM_NATO,
bronze.TIPO_PACIENTE TIPO_PACIENTE,
bronze.TIPO_PRESTADOR TIPO_PRESTADOR,
bronze.FG_RECURSO_PROPRIO FG_RECURSO_PROPRIO,
bronze.TIPO_CONSULTA TIPO_CONSULTA,
bronze.TIPO_ACIDENTE TIPO_ACIDENTE,
bronze.NOME_PROFISSIONAL_SOLICITANTE NOME_PROFISSIONAL_SOLICITANTE,
bronze.SG_CONSELHO_PROFISSIONAL_SOLICITANTE SG_CONSELHO_PROFISSIONAL_SOLICITANTE,
bronze.NR_CONSELHO_PROFISSIONAL_SOLICITANTE NR_CONSELHO_PROFISSIONAL_SOLICITANTE,
bronze.SG_UF_PROFISSIONAL_SOLICITANTE SG_UF_PROFISSIONAL_SOLICITANTE,
bronze.CD_CBO_PROFISSIONAL_SOLICITANTE CD_CBO_PROFISSIONAL_SOLICITANTE,
bronze.CD_TECNICA_UTILIZADA CD_TECNICA_UTILIZADA,
bronze.TIPO_PARTICIPACAO TIPO_PARTICIPACAO,
bronze.CD_CARATER_ATENDIMENTO CD_CARATER_ATENDIMENTO,
bronze.TIPO_ENCERRAMENTO TIPO_ENCERRAMENTO,
bronze.TIPO_TABELA TIPO_TABELA,
bronze.TIPO_INTERNACAO TIPO_INTERNACAO,
bronze.TIPO_REGIME_INTERNACAO TIPO_REGIME_INTERNACAO,
bronze.CD_CID CD_CID,
bronze.NR_GUIATISSPRESTADOR NR_GUIATISSPRESTADOR 
from {} as bronze;'''.format(bronzeTable)
createItemViewQuery = '''CREATE OR REPLACE VIEW intercambio_item_transformation AS 
SELECT 
bronzeItem.CHAVE_NOTA CHAVE_NOTA,
'Pending' processStatus,
Current_Timestamp() dateCreated,
Current_Timestamp() lastUpdated,
bronzeItem.NOME_CONTRATADO_EXECUTANTE NOME_CONTRATADO_EXECUTANTE,
bronzeItem.CD_SERVICO CD_SERVICO,
bronzeItem.CNPJ_CONTRATADO_EXECUTANTE CNPJ_CONTRATADO_EXECUTANTE,
bronzeItem.DT_ATENDIMENTO DT_ATENDIMENTO,
bronzeItem.TIPO_ATENDIMENTO TIPO_ATENDIMENTO
FROM {0} AS bronzeItem'''.format(bronzeTable)

spark.sql(createItemViewQuery)
spark.sql(createCommonViewQuery)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from intercambio_common_tranformation