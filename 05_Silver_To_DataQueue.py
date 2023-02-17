# Databricks notebook source
# MAGIC %md
# MAGIC ### FINAL INGESTION STEP
# MAGIC Transferring Data from Silver to Minerva in HL7 format

# COMMAND ----------

# MAGIC %md
# MAGIC 
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
# MAGIC Including Notebook Named **01_CommonFunctions**

# COMMAND ----------

# MAGIC %run "./01_CommonFunctions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATION OF JSON OBJECT FUNCTION

# COMMAND ----------

import json 
def jsonCreation(data):
    pendingItemQuery = "SELECT * FROM {0} WHERE processStatus = 'Processing' and CHAVE_NOTA = '{1}'".format(silverDataBase+"."+silverItemTable,data["CHAVE_NOTA"])
    pendingItemRecords = spark.sql(pendingItemQuery).collect()
    try:
        json.dumps(mappingFinalJson(data,pendingItemRecords))
    except Exception as e:
        print(e)
    return json.dumps(mappingFinalJson(data,pendingItemRecords))

# COMMAND ----------

# MAGIC %md 
# MAGIC ###MOVEMENT OF DATA FROM SILVER TO DATABASE

# COMMAND ----------

from datetime import datetime

# import trackback

processedRecordList = []
failedRecordsList = []

# Establishing Connection In Mongo
conncetion = getMongoConnection()
collection = getMongoCollection()

# Getting Common Data
commonPendingRecord = getPendingData(silverCommonTable.lower())


if len(commonPendingRecord) > 0:
    print("SIZE OF COMMON PENDING RECORD", len(commonPendingRecord))
    commonPendingRecordList = [row["CHAVE_NOTA"] for row in commonPendingRecord]
    print("in if condition")
    # Marking all the data status as processing
    print("Updating Status Processing from Pending ...")
    updateRecordStatus(
        table=silverCommonTable,
        status="Pending",
        changeStatus="Processing",
        sourceKey="CHAVE_NOTA",
        sourceIdList=commonPendingRecordList,
    )
    updateRecordStatus(
        table=silverItemTable,
        status="Pending",
        changeStatus="Processing",
        sourceKey="CHAVE_NOTA",
        sourceIdList=commonPendingRecordList,
    )
    print("Status Updated ... Processing Data...")

    #     processing Now
    for row in commonPendingRecord:
        try:
            dbRecord = {
                "fileName": "temporary",
                "createdSource": "Intercambio",
                "jsonObject": jsonCreation(row),
                "retries": 0,
                "status": "PENDING",
                "additionalInfo": {"resourceType": "ExplanationOfBenefit"},
                "dateCreated": datetime.now(),
                "lastUpdated": datetime.now(),
            }
            print("Claim Id {0} added to the document".format(row["CHAVE_NOTA"]))
            processedRecordList.append(row["CHAVE_NOTA"])
            collection.insert_one(dbRecord)
        except Exception as e:
            print(
                "Exception while inserting the claim Id {0} and the Exception occured is {1}".format(
                    row["CHAVE_NOTA"],e
                )
            )
            failedRecordsList.append(row["CHAVE_NOTA"])
            
    # Updating status
    print("Updating Status...")
    if len(processedRecordList) > 0:
        updateRecordStatus(
            table=silverCommonTable,
            status="Processing",
            changeStatus="Processed",
            sourceKey="CHAVE_NOTA",
            sourceIdList=processedRecordList,
        )
        updateRecordStatus(
            table=silverItemTable,
            status="Processing",
            changeStatus="Processed",
            sourceKey="CHAVE_NOTA",
            sourceIdList=processedRecordList,
        )
    elif len(failedRecordsList) > 0:
        updateRecordStatus(
            table=silverCommonTable,
            status="Processing",
            changeStatus="Failed",
            sourceKey="CHAVE_NOTA",
            sourceIdList=failedRecordsList,
        )
        updateRecordStatus(
            table=silverItemTable,
            status="Processing",
            changeStatus="Failed",
            sourceKey="CHAVE_NOTA",
            sourceIdList=failedRecordsList,
        )
    print("Congratulations! Data Successfully Ingested...")
    print("Successfuly Ingested",len(processedRecordList)+1,"claim ids i.e. ",processedRecordList,)
    print("Failed while ingest ",len(failedRecordsList)," claim ids i.e. ",failedRecordsList)
else:
    print("No Data Found in Silver ...")

# COMMAND ----------

# MAGIC %sql SELECT * from intercambiosilverdb.intercambiocommonsilver 

# COMMAND ----------

# MAGIC %sql UPDATE intercambiosilverdb.intercambiocommonsilver SET processStatus = 'Pending' WHERE processStatus = 'Processing'