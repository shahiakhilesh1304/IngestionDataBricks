# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### TRANSFERING DATA FROM SOURCE TO BRONZE
# MAGIC 
# MAGIC 
# MAGIC Extracting the data from Oracle DB (i.e. SOURCE) and Populating it to the Bronze Table (i.e. IntercambioBronzeDB)

# COMMAND ----------

# MAGIC %md
# MAGIC ### INCULDING DIFFERENT NOTEBOOKS

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
# MAGIC ### DOWNLOADING REQUIRED MODULES FOR THE ACTION

# COMMAND ----------

# MAGIC %sh
# MAGIC python -m pip install --upgrade pip
# MAGIC python -m pip install oracledb

# COMMAND ----------

# MAGIC %md
# MAGIC ### IMPORTANT CONNECTIONS DETAIL VARIABLES

# COMMAND ----------

user = ConnectionINFO["user"]
password = ConnectionINFO["password"]
dsn = ConnectionINFO["dsn"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEFAULT VARIABLES

# COMMAND ----------

isLegacyIngestion = Legacy["STATUS"]
end_date = None
initial_start_date = datetime(year=2023,month=1,day=1)
cardNumberIngestion = cardNumber["STATUS"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXTRACTION FROM SOURCE DATA

# COMMAND ----------

print(user)

# COMMAND ----------

import pandas as pd
from datetime import datetime
import oracledb
from pyspark.sql.functions import col,lit

try:
    connection = oracledb.connect(user = user,password = password,dsn = dsn)
    fetcher = connection.cursor()
    connection.commit()
    
    if isLegacyIngestion:
        tableName = legacyIngetionStatus
    else:
        tableName = ingetionStatus
    
    bronzeTableName = bronzeTable
    
    if(isLegacyIngestion):
        startDate = getLastUpdateDateLegacy(bronzeDataBase,tableName,"queryEndDate")
        start_date_str = start_date.strftime("%d-%b-%Y")
    else:
        try:
            spark.sql("SELECT * FROM InterCambio_DataIngetion_Status").collect()[0][0]
            startDate = getLastUpdateDate(bronzeDataBase,tableName,"queryEndDate")
            start_date_str = start_date.strftime("%d-%b-%Y")
        except:
            print("except")
            startDate = initial_start_date
            start_date_str = startDate.strftime("%d-%b-%Y")

            
        
    print("Start Date is ",start_date_str)   

    if(isLegacyIngestion):
        end_date = start_date + timedelta(hours=8)
        end_date_str = end_date.strftime("%d-%b-%Y")
    else:
        end_date = datetime.now()
        end_date_str = end_date.strftime("%d-%b-%Y")
    
    print("start_date_str is ",start_date_str)    
    print("end_date_str is ",end_date_str)
    
    
    if cardNumberIngestion:
        query = '''Select * From dw_ods.ODS_RES_A500_INTERCAMBIO WHERE COD_BENEFICIARIO in ({0}) or COD_BENEFICIARIO in ({1}) ORDER BY COD_BENEFICIARIO'''.format(str(cardNumberList1)[1:-1],str(cardNumberList2)[1:-1])
    elif isLegacyIngestion:
        query = '''Select * From dw_ods.ODS_RES_A500_INTERCAMBIO WHERE DT_ALTERACAO >= '{0}' and DT_ALTERACAO <= '{1}' ORDER BY DT_ALTERACAO'''.format(start_date_str,end_date_str)
    else:
        query = '''Select "DT_CARGA",
                "DT_ALTERACAO",
                "ID_TRANSACAO",
                "DT_TRANSACAO",
                "ID_STATUS_TRANSACAO",
                "STATUS",
                "CD_UNIMED_ORIGEM",
                "UNIMED_ORIGEM",
                "CD_UNIMED_DESTINO",
                "UNIMED_DESTINO",
                "CD_UNIMED_BENEFICIARIO",
                "ID_BENEFICIARIO",
                "COD_BENEFICIARIO",
                "FG_RECEM_NATO",
                "TIPO_PACIENTE",
                "NOME_CONTRATADO_EXECUTANTE",
                "CNES_CONTRATADO_EXECUTANTE",
                "CNPJ_CONTRATADO_EXECUTANTE",
                "TIPO_PRESTADOR",
                "FG_RECURSO_PROPRIO",
                "NOME_PROFISSIONAL_EXECUTANTE",
                "SG_CONSELHO_PROFISSIONAL_EXECUTANTE",
                "NR_CONSELHO_PROFISSIONAL_EXECUTANTE",
                "SG_UF_PROFISSIONAL_EXECUTANTE",
                "CD_CBO_PROFISSIONAL_EXECUTANTE",
                "TIPO_CONSULTA",
                "TIPO_ACIDENTE",
                "NOME_PROFISSIONAL_SOLICITANTE",
                "SG_CONSELHO_PROFISSIONAL_SOLICITANTE",
                "NR_CONSELHO_PROFISSIONAL_SOLICITANTE",
                "SG_UF_PROFISSIONAL_SOLICITANTE",
                "CD_CBO_PROFISSIONAL_SOLICITANTE",
                "CD_TECNICA_UTILIZADA",
                "TIPO_PARTICIPACAO",
                "TIPO_ATENDIMENTO",
                "CD_CARATER_ATENDIMENTO",
                "TIPO_ENCERRAMENTO",
                "DT_EXECUCAO",
                "DT_ATENDIMENTO",
                "NR_SEQ_ITEM",
                "CD_ITEM_UNICO",
                "TIPO_TABELA",
                "CD_SERVICO",
                "TP_GUIA",
                "TIPO_INTERNACAO,TIPO_REGIME_INTERNACAO,CD_CID,NR_GUIATISSPRESTADOR,CHAVE_NOTA From dw_ods.ODS_RES_A500_INTERCAMBIO WHERE DT_CARGA >= '{0}' and DT_CARGA <= '{1}' ORDER BY DT_CARGA'''.format(start_date_str,end_date_str)
        
    print("query : "+query)
    fetcher.execute(query)
    result = fetcher.fetchall()     
    print(result[0])
    

except Exception as e:
    print("Exception Occured : ",e)
    updateDataIngestionStats(bronzeDataBase,tableName,startDate, end_date, len(result), str(e))
    dbutils.notebook.exit(e)
finally:
    connection.close()


# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE SOURCE DATA IN BRONZE DATABASE

# COMMAND ----------

import pandas as pd


sourceTable = "Intecambio_view"
if len(result) > 0:
    if spark._jsparkSession.catalog().tableExists(bronzeDataBase, bronzeTableName):
        intercambio_df = pd.DataFrame(
            result,
            columns=[
                "DT_CARGA",
                "DT_ALTERACAO",
                "ID_TRANSACAO",
                "DT_TRANSACAO",
                "ID_STATUS_TRANSACAO",
                "STATUS",
                "CD_UNIMED_ORIGEM",
                "UNIMED_ORIGEM",
                "CD_UNIMED_DESTINO",
                "UNIMED_DESTINO",
                "CD_UNIMED_BENEFICIARIO",
                "ID_BENEFICIARIO",
                "COD_BENEFICIARIO",
                "FG_RECEM_NATO",
                "TIPO_PACIENTE",
                "NOME_CONTRATADO_EXECUTANTE",
                "CNES_CONTRATADO_EXECUTANTE",
                "CNPJ_CONTRATADO_EXECUTANTE",
                "TIPO_PRESTADOR",
                "FG_RECURSO_PROPRIO",
                "NOME_PROFISSIONAL_EXECUTANTE",
                "SG_CONSELHO_PROFISSIONAL_EXECUTANTE",
                "NR_CONSELHO_PROFISSIONAL_EXECUTANTE",
                "SG_UF_PROFISSIONAL_EXECUTANTE",
                "CD_CBO_PROFISSIONAL_EXECUTANTE",
                "TIPO_CONSULTA",
                "TIPO_ACIDENTE",
                "NOME_PROFISSIONAL_SOLICITANTE",
                "SG_CONSELHO_PROFISSIONAL_SOLICITANTE",
                "NR_CONSELHO_PROFISSIONAL_SOLICITANTE",
                "SG_UF_PROFISSIONAL_SOLICITANTE",
                "CD_CBO_PROFISSIONAL_SOLICITANTE",
                "CD_TECNICA_UTILIZADA",
                "TIPO_PARTICIPACAO",
                "TIPO_ATENDIMENTO",
                "CD_CARATER_ATENDIMENTO",
                "TIPO_ENCERRAMENTO",
                "DT_EXECUCAO",
                "DT_ATENDIMENTO",
                "NR_SEQ_ITEM",
                "CD_ITEM_UNICO",
                "TIPO_TABELA",
                "CD_SERVICO",
                "TP_GUIA",
                "TIPO_INTERNACAO",
                "TIPO_REGIME_INTERNACAO",
                "CD_CID",
                "NR_GUIATISSPRESTADOR",
                "CHAVE_NOTA",
            ],
        )
        display(intercambio_df)
        intercambio_sparkDF = spark.createDataFrame(intercambio_df)
        intercambio_sparkDF.createOrReplaceTempView(sourceTable)
        upsertSourceToBronze(sourceTable, bronzeTableName, "DT_CARGA", "DT_ALTERACAO")
    else:
        updateDataIngestionStats(
            bronzeDataBase,
            tableName,
            startDate,
            end_date,
            len(result),
            "BRONZE TABLE NOT CREATED",
        )
        dbutils.notebook.exit("Bronze tables not created!")


else:
    print(tableName)
    updateDataIngestionStats(
        bronzeDataBase,
        tableName,
        startDate,
        end_date,
        len(result),
        "THERE IS NO DATA IN BRONZE TABLE",
    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from IntercambioBronze