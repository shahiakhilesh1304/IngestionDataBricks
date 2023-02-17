# Databricks notebook source
# MAGIC %md
# MAGIC #### COMMON FUNCTIONS
# MAGIC 
# MAGIC Functions That will act as more power to the programme while working on Ingestion

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
# MAGIC ###DOWNLOADING REQUIRED MODULES FOR THE ACTION

# COMMAND ----------

# MAGIC %sh
# MAGIC python -m pip install --upgrade pip
# MAGIC python -m pip install pymongo==3.9

# COMMAND ----------

# MAGIC %md
# MAGIC ## COMMON FUNCTIONS

# COMMAND ----------

from pyspark.sql.functions import *
import pymongo as mongo

#To Extract The Connection Details
def conDetails():
    conDetails = dict()
    conDetails["ip"] = MongoDB["mongo_ip"]
    conDetails["port"] = MongoDB["mongo_port"]
    conDetails["user"] = MongoDB["mongo_user"]
    conDetails["password"] = MongoDB["mongo_password"]
    conDetails["db"] = MongoDB["mongo_db"]
    conDetails["collection"] = MongoDB["mongo_collection"]
    return conDetails


#To Establish the Mongo Connection
def getMongoConnection():
    conInformation = conDetails()
    connectionURI = f'mongodb://{conInformation.get("user")}:{conInformation.get("password")}@{conInformation.get("ip")}:{conInformation.get("port")}/{conInformation.get("db")}'
    return mongo.MongoClient(connectionURI)

#To get Collection from dataBase
def getMongoCollection():
    conDetail = conDetails()
    client = getMongoConnection()
    return client.get_database(conDetail["db"]).get_collection(conDetail["collection"])

# To Get the last Updated Date
def getLastUpdatedDate(dataBase,tableName,columnName):
    table_name = "{}.{}".format(dataBase,tableName)
    lastUpdated_date = None
    if spark._jsparkSession.catalog().tableExists(dataBase,tableName):
        lastUpdated_date = spark.sql(f"SELECT max("+columnName+") FROM "+table_name).collect()[0][0]
    return lastUpdated_date


#To get the last updated Legacy Date
def getLastUpdateDateLegacy(dataBase,tableName,columnName):
    table = "{}.{}".format(dataBase,tableName)
    lastDate = None
    if spark._jsparSession.catalog().tableExists(dataBase,tableName):
        lastDate = spark.sql(f"select max("+columnName+") from"+tableName).collect()[0][0]
    return lastDate

def getPendingData(table):
    query = "SELECT * FROM {0} WHERE processStatus = 'Pending'".format(silverDataBase+"."+table)
    print(query)
    return spark.sql(query).collect()

#To upsert the Data from Source to Bronze
def upsertSourceToBronze(soureTable,targetTable,sourceMatchingCol,targetMatchingCol):
    spark.sql("use {};".format(DetlaLakeDatabases['BronzeDatabase']))
    mergeQuery= "MERGE INTO {0}  dest  USING {1} src ON dest.{2} = src.{3}  WHEN MATCHED THEN UPDATE SET * \
            WHEN NOT MATCHED THEN INSERT  *".format(targetTable,soureTable,targetMatchingCol,sourceMatchingCol)
    spark.sql(mergeQuery)
    
    
# To Upsert the data from Bronze to Silver
def upsertBronzeToSilver(soureTable,targetTable,sourceMatchingCol,targetMatchingCol):
    mergeQuery= "MERGE INTO {0} dest USING {1} src ON dest.{2} = src.{3} WHEN MATCHED THEN UPDATE SET * \
           WHEN NOT MATCHED THEN INSERT *".format(targetTable,soureTable,targetMatchingCol,sourceMatchingCol)
    print(mergeQuery)
    spark.sql(mergeQuery)
    
# Updating the Data ingestion Status
def updateDataIngestionStats(dataBase,tableName,start_date,end_date,recordCount, exception):
    spark.sql("INSERT INTO {0} VALUES ('{1}', '{2}', {3},'{4}')".format(dataBase+"."+tableName,start_date,end_date,recordCount, exception));
    
    
    
#Updating the record status
def updateRecordStatus(table,status,changeStatus,sourceKey,sourceIdList):
    query = "UPDATE {0}.{1} SET processStatus = '{2}' WHERE processStatus = '{3}' and {4} in ({5})".format(silverDataBase,table,changeStatus,status,sourceKey,str(sourceIdList)[1:-1])
    try:
        spark.sql(query)
    except Exception as e:
        print("Unable to Update The Status",e)
    

# COMMAND ----------

# MAGIC %md
# MAGIC ##MAPPING FUNCTIONS

# COMMAND ----------

# MAGIC %md 
# MAGIC Mapping Item Data Functions

# COMMAND ----------

def mappingItemData(itemData):
    items = []
    count = 0
    for item in itemData:
        count += 1
        
        try:
            TIPO_ATENDIMENTO_str = TIPO_ATENDIMENTO[item["TIPO_ATENDIMENTO"]]
        except:
            TIPO_ATENDIMENTO_str = ""
            
        i = {
            "sequence": count,
            "productOrService": {"coding": [{"code": item["CD_SERVICO"]}]},
            "servicedDate": datetime.strptime(item["DT_ATENDIMENTO"], "%Y-%m-%d %H:%M:%S").strftime("%d-%m-%Y %H:%M:%S"),
            "locationAddress": {"line": item["CNPJ_CONTRATADO_EXECUTANTE"]},
            "category": {
                "coding": [
                    {"code": item["TIPO_ATENDIMENTO"], "display": TIPO_ATENDIMENTO_str}
                ]
            },
        }

        items.append(i)
    return items

# COMMAND ----------

# MAGIC %md
# MAGIC Mapping Common Data Functions

# COMMAND ----------

# mapping identifier
def mapping_Common_Identifier_Data(data):
    """Mapping identifier For Common Data"""
    return [
        {
            "type": {
                "coding": [{"system": "Unimed", "code": "uc", "display": "Claim Id"}]
            },
            "value": {"value": data["CHAVE_NOTA"]},
        }
    ]


# mapping patient
def mapping_Common_Patient_Data(data):
    """Mapping patient For Common Data"""
    return {
        "identifier": [{"value": {"value": data["COD_BENEFICIARIO"]}}]
    }  # "00566104000446014"


# mapping type
def mapping_Common_Type_Data(data):
    """Mapping type For Common Data"""
    return {"coding": [{"code": data["TP_GUIA"], "display": data["TP_GUIA"]}]}


# mapping careTeam
def mapping_Common_CareTeam_Data(data):
    """Mapping careTeam Common Data"""
    try:
        CD_CBO_PROFISSIONAL_SOLICITANTE_str = CD_CBO_PROFISSIONAL[
            data["CD_CBO_PROFISSIONAL_SOLICITANTE"].strip()
        ]
    except:
        CD_CBO_PROFISSIONAL_SOLICITANTE_str = ""

    try:
        CD_CBO_PROFISSIONAL_EXECUTANTE_str = CD_CBO_PROFISSIONAL[
            data["CD_CBO_PROFISSIONAL_EXECUTANTE"].strip()
        ]
    except:
        CD_CBO_PROFISSIONAL_EXECUTANTE_str = ""

    try:
        TIPO_PARTICIPACAO_str = TIPO_PARTICIPACAO[data["TIPO_PARTICIPACAO"].strip()]
    except:
        TIPO_PARTICIPACAO_str = ""

    try:
        SG_CONSELHO_PROFISSIONAL_EXECUTANTE_str = SG_CONSELHO_PROFISSIONAL_EXECUTANTE[
            data["SG_CONSELHO_PROFISSIONAL_EXECUTANTE"]
        ]
    except:
        SG_CONSELHO_PROFISSIONAL_EXECUTANTE_str = ""

    try:
        SG_CONSELHO_PROFISSIONAL_EXECUTANTE_str = SG_CONSELHO_PROFISSIONAL_EXECUTANTE[
            data["SG_CONSELHO_PROFISSIONAL_EXECUTANTE"]
        ]
    except:
        SG_CONSELHO_PROFISSIONAL_EXECUTANTE_str = ""

    try:
        SG_CONSELHO_PROFISSIONAL_SOLICITANTE_str = SG_CONSELHO_PROFISSIONAL_EXECUTANTE[
            data["SG_CONSELHO_PROFISSIONAL_SOLICITANTE"]
        ]
    except:
        SG_CONSELHO_PROFISSIONAL_EXECUTANTE_str = ""

    if data["TIPO_PARTICIPACAO"] is None:
        return [
            {
                "sequence": 1,
                "providerPractitioner": {
                    "resourceType": "Practitioner",
                    "identifier": [
                        {
                            "system": data["SG_UF_PROFISSIONAL_EXECUTANTE"],
                            "value": {
                                "value": data["NR_CONSELHO_PROFISSIONAL_EXECUTANTE"]
                            },
                            "type": {
                                "coding": [
                                    {
                                        "code": data[
                                            "SG_CONSELHO_PROFISSIONAL_EXECUTANTE"
                                        ],
                                        "display": SG_CONSELHO_PROFISSIONAL_EXECUTANTE_str,
                                    }
                                ]
                            },
                        },
                    ],
                    "active": True,
                    "name": [
                        {
                            "family": "",
                            "given": [data["NOME_PROFISSIONAL_EXECUTANTE"]],
                        }
                    ],
                    "qualification": [
                        {
                            "code": {
                                "coding": [
                                    {
                                        "code": data["CD_CBO_PROFISSIONAL_EXECUTANTE"],
                                        "display": CD_CBO_PROFISSIONAL_EXECUTANTE_str,
                                    }
                                ],
                                "text": CD_CBO_PROFISSIONAL_EXECUTANTE_str,
                            }
                        }
                    ],
                },
            },
            {
                "sequence": 2,
                "providerPractitioner": {
                    "resourceType": "Practitioner",
                    "identifier": [
                        {
                            "system": data["SG_UF_PROFISSIONAL_SOLICITANTE"],
                            "value": {
                                "value": data["NR_CONSELHO_PROFISSIONAL_SOLICITANTE"]
                            },
                        },
                        {
                            "type": {
                                "coding": [
                                    {
                                        "code": data[
                                            "SG_CONSELHO_PROFISSIONAL_SOLICITANTE"
                                        ],
                                        "display": SG_CONSELHO_PROFISSIONAL_SOLICITANTE_str,
                                    }
                                ]
                            }
                        },
                    ],
                    "active": True,
                    "name": [
                        {
                            "family": "",
                            "given": [data["NOME_PROFISSIONAL_SOLICITANTE"]],
                        }
                    ],
                    "qualification": [
                        {
                            "code": {
                                "coding": [
                                    {
                                        "code": data["CD_CBO_PROFISSIONAL_SOLICITANTE"],
                                        "display": CD_CBO_PROFISSIONAL_SOLICITANTE_str,
                                    }
                                ],
                                "text": CD_CBO_PROFISSIONAL_SOLICITANTE_str,
                            }
                        }
                    ],
                },
            },
        ]
    else:
        return [
            {
                "sequence": 1,
                "providerPractitioner": {
                    "resourceType": "Practitioner",
                    "identifier": [
                        {
                            "system": data["SG_UF_PROFISSIONAL_EXECUTANTE"],
                            "value": {
                                "value": data["NR_CONSELHO_PROFISSIONAL_EXECUTANTE"]
                            },
                            "type": {
                                "coding": [
                                    {
                                        "code": data[
                                            "SG_CONSELHO_PROFISSIONAL_EXECUTANTE"
                                        ],
                                        "display": SG_CONSELHO_PROFISSIONAL_EXECUTANTE_str,
                                    }
                                ]
                            },
                        },
                    ],
                    "active": True,
                    "name": [
                        {
                            "family": "",
                            "given": [data["NOME_PROFISSIONAL_EXECUTANTE"]],
                        }
                    ],
                    "qualification": [
                        {
                            "code": {
                                "coding": [
                                    {
                                        "code": data["CD_CBO_PROFISSIONAL_EXECUTANTE"],
                                        "display": CD_CBO_PROFISSIONAL_EXECUTANTE_str,
                                    }
                                ],
                                "text": CD_CBO_PROFISSIONAL_EXECUTANTE_str,
                            }
                        }
                    ],
                },
                "role": {
                    "coding": [
                        {
                            "code": data["TIPO_PARTICIPACAO"],
                            "display": TIPO_PARTICIPACAO_str,
                        }
                    ],
                    "text": TIPO_PARTICIPACAO_str,
                },
            },
            {
                "sequence": 2,
                "providerPractitioner": {
                    "resourceType": "Practitioner",
                    "identifier": [
                        {
                            "system": data["SG_UF_PROFISSIONAL_SOLICITANTE"],
                            "value": {
                                "value": data["NR_CONSELHO_PROFISSIONAL_SOLICITANTE"]
                            },
                        },
                        {
                            "type": {
                                "coding": [
                                    {
                                        "code": data[
                                            "SG_CONSELHO_PROFISSIONAL_SOLICITANTE"
                                        ],
                                        "display": SG_CONSELHO_PROFISSIONAL_SOLICITANTE_str,
                                    }
                                ]
                            }
                        },
                    ],
                    "active": True,
                    "name": [
                        {
                            "family": "",
                            "given": [data["NOME_PROFISSIONAL_SOLICITANTE"]],
                        }
                    ],
                    "qualification": [
                        {
                            "code": {
                                "coding": [
                                    {
                                        "code": data["CD_CBO_PROFISSIONAL_SOLICITANTE"],
                                        "display": CD_CBO_PROFISSIONAL_SOLICITANTE_str,
                                    }
                                ],
                                "text": CD_CBO_PROFISSIONAL_SOLICITANTE_str,
                            }
                        }
                    ],
                },
            },
        ]


# mapping insurer
def mapping_Common_Insurer_Data(data):
    """Mapping insurer Common Data"""
    return {"identifier": [{"value": {"value": data["CD_UNIMED_DESTINO"]}}]}


# mapping insurance
def mapping_Common_Insurance_Data(data):
    """Mapping insurance Common Data"""
    try:
        payor = data["COD_BENEFICIARIO"][0:4]
    except:
        payor = ""
    return [
        {
            "focal": True,
            "coverage": {
                "status" : "active",
                "identifier": [{"value": {"value": data["COD_BENEFICIARIO"]}}],  #"00566104000446014" 
                "beneficiary": {
                    "identifier": [{"value": {"value": data["COD_BENEFICIARIO"]  #"00566104000446014" 
                                             }
                        }
                    ],
                },
                "payorOrganization": [{
                    "identifier": [{"value": {"value": payor}}]  # payor
                }],
            },
        }
    ]


# mapping supporting info
def mapping_Common_SupportingInfo_Data(data):
    """Mapping supportingInfo Common Data"""
    #     1
    try:
        FG_RECEM_NATO_str = FG_RECEM_NATO[data["FG_RECEM_NATO"].strip().upper()]
    except:
        FG_RECEM_NATO_str = ""
    #    2
    try:
        TIPO_PRESTADOR_str = TIPO_PRESTADOR[data["TIPO_PRESTADOR"].strip()]
    except:
        TIPO_PRESTADOR_str = ""
    #     4
    try:
        TIPO_CONSULTA_str = TIPO_CONSULTA[data["TIPO_CONSULTA"].strip()]
    except:
        TIPO_CONSULTA_str = ""
    #    5
    try:
        CD_TECNICA_UTILIZADA_str = CD_TECNICA_UTILIZADA[
            data["CD_TECNICA_UTILIZADA"].strip()
        ]
    except:
        CD_TECNICA_UTILIZADA_str = ""
    #    6
    try:
        key = data["CD_CARATER_ATENDIMENTO"].strip()
        CD_CARATER_ATENDIMENTO_str = CD_CARATER_ATENDIMENTO[key]
        print(CD_CARATER_ATENDIMENTO_str)
    except:
        CD_CARATER_ATENDIMENTO_str = ""
    #    7
    try:
        TIPO_ENCERRAMENTO_str = TIPO_ENCERRAMENTO[data["TIPO_ENCERRAMENTO"].strip()]
    except:
        TIPO_ENCERRAMENTO_str = ""
    #     9
    try:
        TIPO_INTERNACAO_str = TIPO_INTERNACAO[data["TIPO_INTERNACAO"].split()]
    except:
        TIPO_INTERNACAO_str = ""
    #     10
    try:
        TIPO_REGIME_INTERNACAO_str = TIPO_REGIME_INTERNACAO[
            data["TIPO_REGIME_INTERNACAO"].strip()
        ]
    except:
        TIPO_REGIME_INTERNACAO_str = ""
    #     12
    try:
        TIPO_PACIENTE_str = TIPO_PACIENTE[data["TIPO_PACIENTE"].strip()]
    except:
        TIPO_PACIENTE_str = ""
    #    3
    try:
        FG_RECURSO_PROPRIO_str = FG_RECURSO_PROPRIO[
            data["FG_RECURSO_PROPRIO_str"].strip()
        ]
    except:
        FG_RECURSO_PROPRIO_str = ""
    #     8
    try:
        TIPO_TABELA_str = TIPO_TABELA[data["TIPO_TABELA"].strip()]
    except:
        TIPO_TABELA_str = ""
    supportingInfo = {
        1: {
            "sequence": 1,
            "code": {
                "coding": [
                    {
                        "code": data["FG_RECEM_NATO"],
                        "display": FG_RECEM_NATO_str,
                    }
                ]
            },
            "category": {
                "coding": [
                    {"code": "New Born Indicator", "display": "New Born Indicator"}
                ]
            },
        },
        2: {
            "sequence": 2,
            "code": {
                "coding": [
                    {
                        "code": data["TIPO_PRESTADOR"],
                        "display": TIPO_PRESTADOR_str,
                    }
                ]
            },
            "category": {
                "coding": [{"code": "Provider Type", "display": "Provider Type"}]
            },
        },
        3: {
            "sequence": 3,
            "code": {
                "coding": [
                    {
                        "code": data["FG_RECURSO_PROPRIO"],
                        "display": FG_RECURSO_PROPRIO_str,
                    }
                ]
            },
            "category": {
                "coding": [
                    {"code": "Owned or Contracted", "display": "Owned or Contracted"}
                ]
            },
        },
        4: {
            "sequence": 4,
            "code": {
                "coding": [
                    {
                        "code": data["TIPO_CONSULTA"],
                        "display": TIPO_CONSULTA_str,
                    }
                ]
            },
            "category": {"coding": [{"code": "Query Type", "display": "Query Type"}]},
        },
        5: {
            "sequence": 5,
            "code": {
                "coding": [
                    {
                        "code": data["CD_TECNICA_UTILIZADA"],
                        "display": CD_TECNICA_UTILIZADA_str,
                        "system": "",
                    }
                ]
            },
            "category": {
                "coding": [{"code": "Technique used", "display": "Technique used"}]
            },
        },
        6: {
            "sequence": 6,
            "code": {
                "coding": [
                    {
                        "code": data["CD_CARATER_ATENDIMENTO"],
                        "display": CD_CARATER_ATENDIMENTO_str,
                        "system": "",
                    }
                ]
            },
            "category": {
                "coding": [{"code": "Character Type", "display": "Character Type"}]
            },
        },
        7: {
            "sequence": 7,
            "code": {
                "coding": [
                    {
                        "code": data["TIPO_ENCERRAMENTO"],
                        "display": TIPO_ENCERRAMENTO_str,
                    }
                ]
            },
            "category": {
                "coding": [{"code": "Closing Type", "display": "Closing Type"}]
            },
        },
        8: {
            "sequence": 8,
            "code": {
                "coding": [{"code": data["TIPO_TABELA"], "display": TIPO_TABELA_str}]
            },
            "category": {"coding": [{"code": "Table Type", "display": "Table Type"}]},
        },
        9: {
            "sequence": 9,
            "code": {
                "coding": [
                    {
                        "code": data["TIPO_INTERNACAO"],
                        "display": TIPO_INTERNACAO_str,
                    }
                ]
            },
            "category": {
                "coding": [
                    {"code": "hospitalization Type", "display": "hospitalization Type"}
                ]
            },
        },
        10: {
            "sequence": 10,
            "code": {
                "coding": [
                    {
                        "code": data["TIPO_REGIME_INTERNACAO"],
                        "display": TIPO_REGIME_INTERNACAO_str,
                        "system": "",
                    }
                ]
            },
            "category": {
                "coding": [{"code": "hospital Regime", "display": "hospital Regime"}]
            },
        },
        11: {
            "sequence": 11,
            "category": {
                "coding": [
                    {
                        "code": "Provider TISS Guide Number",
                        "display": "Provider TISS Guide Number",
                    }
                ]
            },
            "valueString": data["NR_GUIATISSPRESTADOR"],
        },
        12: {
            "sequence": 12,
            "code": {
                "coding": [
                    {
                        "code": data["TIPO_PACIENTE"],
                        "display": TIPO_PACIENTE_str,
                    }
                ]
            },
            "category": {
                "coding": [{"code": "Patient Type", "display": "Patient Type"}]
            },
        },
    }

    info = []
    for key, value in supportingInfo.items():
        if key == 1:
            try:
                FG_RECEM_NATO_str = FG_RECEM_NATO[data["FG_RECEM_NATO"].strip().upper()]
                info.append(value)
            except:
                FG_RECEM_NATO_str = ""
        elif key == 2:
            try:
                TIPO_PRESTADOR_str = TIPO_PRESTADOR[data["TIPO_PRESTADOR"].strip()]
                info.append(value)
            except:
                TIPO_PRESTADOR_str = ""
        elif key == 3:
            try:
                FG_RECURSO_PROPRIO_str = FG_RECURSO_PROPRIO[
                    data["FG_RECURSO_PROPRIO_str"].strip()
                ]
                info.append(value)
            except:
                FG_RECURSO_PROPRIO_str = ""
        elif key == 4:
            try:
                TIPO_CONSULTA_str = TIPO_CONSULTA[data["TIPO_CONSULTA"].strip()]
                info.append(value)
            except:
                TIPO_CONSULTA_str = ""
        elif key == 5:
            try:
                CD_TECNICA_UTILIZADA_str = CD_TECNICA_UTILIZADA[
                    data["CD_TECNICA_UTILIZADA"].strip()
                ]
                info.append(value)
            except:
                CD_TECNICA_UTILIZADA_str = ""
        elif key == 6:
            try:
                key = data["CD_CARATER_ATENDIMENTO"].strip()
                CD_CARATER_ATENDIMENTO_str = CD_CARATER_ATENDIMENTO[key]
                info.append(value)
            except:
                CD_CARATER_ATENDIMENTO_str = ""
        elif key == 7:
            try:
                TIPO_ENCERRAMENTO_str = TIPO_ENCERRAMENTO[
                    data["TIPO_ENCERRAMENTO"].strip()
                ]
                info.append(value)
            except:
                TIPO_ENCERRAMENTO_str = ""
        elif key == 8:
            try:
                TIPO_TABELA_str = TIPO_TABELA[data["TIPO_TABELA"].strip()]
                info.append(value)
            except:
                TIPO_TABELA_str = ""
        elif key == 9:
            try:
                TIPO_INTERNACAO_str = TIPO_INTERNACAO[data["TIPO_INTERNACAO"].split()]
                info.append(value)
            except:
                TIPO_INTERNACAO_str = ""
        elif key == 10:
            try:
                TIPO_REGIME_INTERNACAO_str = TIPO_REGIME_INTERNACAO[
                    data["TIPO_REGIME_INTERNACAO"].strip()
                ]
                info.append(value)
            except:
                TIPO_REGIME_INTERNACAO_str = ""
        elif key == 11:
            info.append(value)
        elif key == 12:
            try:
                TIPO_PACIENTE_str = TIPO_PACIENTE[data["TIPO_PACIENTE"].strip()]
            except:
                TIPO_PACIENTE_str = ""
    return info


# Mapping Diagnosis
def mapping_Common_Diagnosis_Data(data):
    """Mapping diagnosis Common Data"""
    return [
        {
            "diagnosisCodeableConcept": {"coding": [{"code": data["CD_CID"]}]},
            "sequence": 1,
        }
    ]


# Mapping Provider Organization
def mapping_Common_Provider_Organization_Data(data):
    """Mapping providerOrganization Common Data"""
    return {"identifier": [{"system": "", "value": {"value": data["CD_UNIMED_ORIGEM"]}}]}


# mapping Accident
def mapping_Common_Accident_data(data):
    """Mapping accident Common Data"""
    try:
        TIPO_ACIDENTE_str = TIPO_ACIDENTE[data["TIPO_ACIDENTE"].strip()]
    except:
        TIPO_ACIDENTE_str = ""
    return {
        "date" : datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
        "type": {
            "coding": [
                {
                    "code": data["TIPO_ACIDENTE"],
                    "display": TIPO_ACIDENTE_str,
                }
            ]
        }
    }

# COMMAND ----------

# MAGIC %md
# MAGIC MAPPING MAJOR JSON

# COMMAND ----------

def mappingFinalJson(data, pendingItemRecords):
    if data["CD_CID"] is None:
        return {
            "ExplanationOfBenefit": {
                "resourceType": "ExplanationOfBenefit",
                "created": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "identifier": mapping_Common_Identifier_Data(data),
                "status": "active",
                "use": "claim",
                "outcome": "complete",
                "type": mapping_Common_Type_Data(data),
                "patient": mapping_Common_Patient_Data(data),
                "insurer": mapping_Common_Insurer_Data(data),
                "insurance": mapping_Common_Insurance_Data(data),
                "careTeam": mapping_Common_CareTeam_Data(data),
                "item": mappingItemData(pendingItemRecords),
                "supportingInfo": mapping_Common_SupportingInfo_Data(data),
                "accident": mapping_Common_Accident_data(data),
                "providerOrganization": mapping_Common_Provider_Organization_Data(data),
            }
        }
    else:
        return {
            "ExplanationOfBenefit": {
                "resourceType": "ExplanationOfBenefit",
                "created": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "identifier": mapping_Common_Identifier_Data(data),
                "status": "active",
                "use": "claim",
                "outcome": "complete",
                "type": mapping_Common_Type_Data(data),
                "patient": mapping_Common_Patient_Data(data),
                "insurer": mapping_Common_Insurer_Data(data),
                "insurance": mapping_Common_Insurance_Data(data),
                "careTeam": mapping_Common_CareTeam_Data(data),
                "item": mappingItemData(pendingItemRecords),
                "supportingInfo": mapping_Common_SupportingInfo_Data(data),
                "accident": mapping_Common_Accident_data(data),
                "diagnosis": mapping_Common_Diagnosis_Data(data),
                "providerOrganization": mapping_Common_Provider_Organization_Data(data),
            }
        }