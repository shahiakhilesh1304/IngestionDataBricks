# Databricks notebook source
from datetime import datetime

DetlaLakeDatabases = {
    "BronzeDatabase": "QA_IntercambioBronzeDB",
    "SilverDatabase": "QA_IntercambioSilverDB"
}

TableName = {
    "silverItemTable" : "IntercambioItemSilver",
    "silverCommonTable" : "IntercambioCommonSilver",
    "bronzeTable" : "IntercambioBronze"
}

utilityTable = {
    "dataIngetionStatus" : "InterCambio_DataIngetion_Status",
    "dataIngetionLegacyStatus" : "InterCambio_DataIngetion_Status_Legacy"
}

ConnectionINFO ={
    "user" : "T01168",
    "password" : "TGvTDhD799kB",
    "dsn" : "10.144.80.209:1521/DBPRD11"
}

Legacy = {
    "STATUS" : False
}

InitialDate = {
    "date" : datetime(year=2023,month=1,day=1)
}

cardNumber = {
    "STATUS" : True
}

MongoDB={
"mongo_ip" : '10.31.10.69',
"mongo_port" : '27018',
"mongo_user" : 'unimqa_minerva',
"mongo_password" : 'unimqa_minerva',
"mongo_db" : 'unimqa_minervadb',
"mongo_collection" : "financialDataCollection"
}


# COMMAND ----------

# MAGIC %md
# MAGIC ### CARD NUMBERS

# COMMAND ----------

cardNumberList1 = [
    "01810000002314505" 
 ]

cardNumberList2 = [
    "01810000002789449"
]



# COMMAND ----------

# MAGIC %md
# MAGIC ###CODEABLE CONCEPTS DISPLAY DATA

# COMMAND ----------

TIPO_PRESTADOR = {
    "01": "Physician",
    "02": "Hospital",
    "03": "Laboratory",
    "04": "Clinic",
    "05": "Individual – Non-doctor",
    "06": "Diagnostic Center",
    "08": "Home Care (home care)",
    "09": "Cooperative",
    "10": "Day Hospital",
    "11": "Emergency Care",
    "13": "Specialty Clinic",
    "14": "Oncology Center",
    "15": "Multiprofessional Center",
    "16": "Hemodialysis Center",
    "17": "Hemodynamics Center",
}

TIPO_CONSULTA = {
    "1": "First Consultation",
    "2": "Return",
    "3": "Prenatal",
    "4": "By referral",
}

TIPO_ACIDENTE = {
    "0": "Accident at work",
    "1": "Traffic accident",
    "2": "Accident – others",
    "9": "No accident",
}

TIPO_PACIENTE = {"1": "Medical Assistance", "9": "Occupational Health"}

FG_RECEM_NATO = {"Y": "Yes", "N": "No"}

CD_TECNICA_UTILIZADA = {"1": "Conventional", "2": "Video", "3": "Robotics"}

CD_CARATER_ATENDIMENTO = {"1": "Elective", "2": "Urgency/Emergency"}


TIPO_ENCERRAMENTO = {
    "11": "Alta Curado",
    "12": "Alta Melho rado",
    "14": "Alta a pedido",
    "16": "Po r evasão",
    "15": "Alta co m previsão  de reto rno  para aco mpanhamento do  paciente",
    "61": "Alta da mãe/puérpera e recém-nascido",
    "18": "Alta po r Outro s Mo tivo s",
    "62": "Alta da mãe/puérpera e permanência do  recém-nascido",
    "19": "Alta de Paciente Agudo em Psiquiatria",
    "63": "Altadamãe/puérperae ó bito  do  recém-nascido",
    "64": "Alta da mãe/puérpera co m ó bito  feta",
    "21": "Po r características  pró prias da do ença",
    "22": "Po r interco rrência",
    "23": "Po r impo ssibilidade só cio-familiar",
    "24": "Po r pro cesso  de doação  de ó rgãos, tecido s e células –do ado r vivo",
    "25": "Po r pro cesso  de doação  de ó rgãos, tecido s e células –do ado r mo rto",
    "26": "Po r mudança de Pro cedimento",
    "27": "Po r reo peração",
    "28": "Outro s Mo tivo s",
    "31": "Transferido  para o utro  estabelecimento",
    "32": "Transferênciapara Internação  Do miciliar",
    "41": "Co m declaração  de ó bito  fo rnecida pelo  médico  assistente",
    "42": "Co m declaração  de ó bito  fo rnecida pelo  InstitutoMédico  Legal –IML",
    "43": "Co m declaração  de ó bito  fo rnecida pelo  Serviço  de Verificação  de ó bito  –SVO",
    "65": "Óbito  da gestante e do  co ncepto",
    "66": "Óbito  da mãe/puérpera  e alta do  recém-nascido",
    "67": "Óbito  da mãe/puérpera  e permanênciadorecém-nascido",
}

TIPO_INTERNACAO = {
    "1": "Clinical Admission",
    "2": "Surgical Admission",
    "3": "Obstetric Admission",
    "4": "Pediatric Admission",
    "5": "Psychiatric Admission",
}

TIPO_REGIME_INTERNACAO = {"1": "Hospital", "2": "Day hospital", "3": "Home"}


TIPO_ATENDIMENTO = {
    "01": "Remo ção02Pequena Cirurgia",
    "03": "Outras Terapias04Co nsulta",
    "05": "Exame Ambulatorial",
    "06": "Atendimento  Do miciliar",
    "07": "Internação",
    "08": "Quimio terapia",
    "09": "Radio terapia",
    "10": "Terapia RenalSubstitutiva (TRS)",
    "11": "Pro nto  So co rro",
    "13": "Pequeno Atendimento  (sutura, gesso e outro s)",
    "14": "Saúde Ocupacio nal -Admissio nal",
    "15": "Saúde Ocupacional -Demissional",
    "16": "Saúde Ocupacio nal -Perió dico",
    "17": "Saúde Ocupacio nal –Reto rno  ao  trabalho",
    "18": "Saúde Ocupacional–Mudança de função",
    "19": "Saúde Ocupacio nal –Pro mo ção  a saúde",
    "20": "Saúde Ocupacio nal –Beneficiário  No vo",
    "21": "Saúde Ocupacio nal –Assistênciaa demitido s",
    "22": "TELESSAÚDE",
    "23": "Exame",
}

TIPO_PARTICIPACAO = {
    "00": "Surgeon",
    "01": "First Assistant",
    "02": "Second Assistant",
    "03": "Third Assistant",
    "04": "Room Assistant",
    "06": "Anesthesiologist",
    "07": "Anesthesiologist Assistant",
    "09": "Perfusionist",
    "10": "Pediatrician in the delivery room",
    "11": "SADT Assistant",
    "12": "Clinician",
    "13": "Intensivist",
}
CD_CBO_PROFISSIONAL = {
    "226310": "Arteterapeuta251605Assistente social",
    "322230": "Auxiliar de enfermagem",
    "224105": "Avaliado r físico221105Biólogo",
    "221205": "Bio médico",
    "999999": "CBO desconhecido ou não informado pelo solicitante",
    "223204": "Cirurgião dentista -auditor",
    "223208": "Cirurgião dentista -clínico geral",
    "223280": "Cirurgião dentista -dentística",
    "223284": "Cirurgião dentista -disfunção temporomandibular e dor orofacial",
    "223212": "Cirurgião dentista-endodontista",
    "223216": "Cirurgião dentista -epidemiologista",
    "223220": "Cirurgião dentista -estomatologista",
    "223224": "Cirurgião dentista -implantodontista",
    "223228": "Cirurgião dentista -odontogeriatra",
    "223276": "Cirurgião dentista -odontologia dotrabalho",
    "223288": "Cirurgião dentista -odontologia para pacientes com necessidades especiais",
    "223232": "Cirurgião dentista -odontologista legal",
    "223236": "Cirurgião dentista -odontopediatra",
    "223240": "Cirurgião dentista -ortopedista e ortodontista",
    "223244": "Cirurgião dentista -patologista bucal",
    "223248": "Cirurgião dentista -periodontista",
    "223252": "Cirurgião dentista -protesiólogo bucomaxilofacial",
    "223256": "Cirurgião dentista -protesista",
    "223260": "Cirurgião dentista -radiologista",
    "223264": "Cirurgião dentista -reabilitador oral",
    "223268": "Cirurgião dentista -traumatologista bucomaxilofacial",
    "223272": "Cirurgião dentista de saúde coletiva",
    "223293": "Cirurgião-dentista da estratégia de saúde da família",
    "516210": "Cuidador de idosos",
    "223705": "Dietista",
    "322135": "Do ula",
    "223505": "Enfermeiro",
    "223510": "Enfermeiro auditor",
    "223565": "Enfermeiro da estratégia de saúde da família",
    "223515": "Enfermeiro de bordo",
    "223520": "Enfermeiro de centro cirúrgico",
    "223525": "Enfermeiro de terapia intensiva",
    "223530": "Enfermeiro do trabalho",
    "223535": "Enfermeiro nefrologista",
    "223540": "Enfermeiro neonatologista",
    "223545": "Enfermeiro obstétrico",
    "223550": "Enfermeiro psiquiátrico",
    "223555": "Enfermeiro puericultor e pediátrico",
    "223560": "Enfermeiro sanitarista",
    "226315": "Equo terapeuta",
    "223405": "Farmacêutico",
    "223415": "Farmacêutico analista clínico",
    "223420": "Farmacêutico de alimentos",
    "223430": "Farmacêutico em saúde pública",
    "223445": "Farmacêutico hospitalar e clínico",
    "223435": "Farmacêutico industrial",
    "223425": "Farmacêutico práticas integrativas e complementares",
    "223440": "Farmacêutico toxicologista",
    "213150": "Físico médico",
    "223650": "Fisioterapeuta acupunturista",
    "223660": "Fisioterapeuta do trabalho",
    "223655": "Fisioterapeuta esportivo",
    "223605": "Fisioterapeuta geral",
    "223630": "Fisioterapeuta neurofuncional",
    "223640": "Fisioterapeuta osteopata",
    "223645": "Fisioterapeuta quiropraxista",
    "223625": "Fisioterapeuta respiratória",
    "223635": "Fisioterapeuta traumato-ortopédica funcional",
    "223810": "Fonoaudiólogo",
    "223815": "Fonoaudiólogo  educacional",
    "223820": "Fonoaudiólogo  em audiologia",
    "223825": "Fonoaudiólogo  em disfagia",
    "223830": "Fonoaudiólogo  em linguagem",
    "223835": "Fonoaudiólogo  em motricidade orofacial",
    "223840": "Fonoaudiólogo  em saúde coletiva",
    "223845": "Fonoaudiólogo  em voz",
    "201115": "Geneticista",
    "131220": "Gero ntó logo",
    "322225": "Instrumentador cirúrgico",
    "224110": "Ludo mo tricista",
    "322120": "Massoterapeuta",
    "225260": "Médico  neurocirurgião",
    "225105": "Médico acupunturista",
    "225110": "Médico alergista e imunologista",
    "225148": "Médico anatomopatologista",
    "225151": "Médico anestesiologista",
    "225115": "Médico angiologista",
    "225154": "Médico antroposófico",
    "225290": "Médico cancerologista cirurgico",
    "225122": "Médico cancerologista pediátrico",
    "225120": "Médico cardiologista",
    "225210": "Médico cirurgião cardiovascular",
    "225295": "Médico cirurgião da mão",
    "225215": "Médico cirurgião de cabeça e pescoço",
    "225220": "Médico cirurgião do aparelho digestivo",
    "225225": "Médico cirurgião geral",
    "225230": "Médico cirurgião pediátrico",
    "225235": "Médico cirurgião plástico",
    "225240": "Médico cirurgião torácico",
    "225305": "Médico citopatologista",
    "225125": "Médico clínico",
    "225142": "Médico da estratégia de saúda da família",
    "225130": "Médico de família e comunidade",
    "225135": "Médico dermatologista",
    "225140": "Médico do trabalho",
    "225203": "Médico em cirurgia vascular",
    "225310": "Médico em endoscopia",
    "225145": "Médico em medicina de tráfego",
    "225150": "Médico em medicina intensiva",
    "225315": "Médico em medicina nuclear",
    "225320": "Médico em radiologia e diagnóstico por imagem",
    "225155": "Médico endocrinologista e metabologista",
    "225160": "Médico fisiatra",
    "225165": "Médico gastroenterologista",
    "225170": "Médico generalista",
    "225175": "Médico geneticista",
    "225180": "Médico geriatra",
    "225250": "Médico ginecologista e obstetra",
    "225185": "Médico Hematologista",
    "225340": "Médico hemoterapeuta",
    "225345": "Médico hiperbarista",
    "225195": "Médico Homeopata",
    "225103": "Médico infectologista",
    "225106": "Médico legista",
    "225255": "Médico Mastologista",
    "225109": "Médico Nefrologista",
    "225350": "Médico neurofisiologista",
    "225112": "Médico neurologista",
    "225118": "Médico nutrologista",
    "225265": "Médico oftalmologista",
    "225121": "Médico oncologista clínico",
    "225270": "Médico ortopedista e traumatologista",
    "225275": "Médico otorrinolaringologista",
    "225325": "Médico patologista",
    "225335": "Médico patologista clínico / medicina laboratorial",
    "225124": "Médico pediatra",
    "225127": "Médico pneumologista",
    "225280": "Médico proctologista",
    "225133": "Médico psiquiatra",
    "225355": "Médico  radio lo gista intervencio nista",
    "225330": "Médico radioterapeuta",
    "225136": "Médico reumatologista",
    "225139": "Médico sanitarista",
    "225285": "Médico urologista",
    "226305": "Musico terapeuta",
    "226320": "Naturó lo go",
    "251545": "Neuropsicólogo",
    "239440": "Neuro psico pedagogo clinico",
    "239445": "Neuro psico pedagogo institucio nal",
    "223710": "Nutricionista",
    "223910": "Ortoptista",
    "226110": "Osteo pata",
    "223570": "Perfusionista",
    "203015": "Pesquisador em biologia demicroorganismos eparasitas",
    "224115": "Preparado r de atleta",
    "224120": "Preparado r físico",
    "224140": "Pro fissio nal de educação  física na saúde",
    "251550": "Psicanalista",
    "251555": "Psicó lo go  acupunturista",
    "251510": "Psicólogo clínico",
    "223915": "Psico mo tricista",
    "239425": "Psicopedagogo",
    "226105": "Quiro praxista",
    "224125": "Técnico  de desporto  individual e co letivo  (exceto  futebo l)",
    "322205": "Técnico de enfermagem",
    "322220": "Técnico de enfermagem psiquiátrica",
    "224130": "Técnico  de labo rató rio  e fiscalização   despo rtiva",
    "322125": "Terapeuta ho lístico",
    "223905": "Terapeuta ocupacional",
    "224135": "Treinado r pro fissio nal de futebo l",
}


SG_CONSELHO_PROFISSIONAL_EXECUTANTE = {
    "Código": "Descrição",
    "COREN": "Conselho Regio nal de Enfermagem",
    "CRESS": "Conselho Regio nal de Serviço Social",
    "CRBM": "Conselho Regio nal de Bio medicina",
    "CREFITO": "Conselho Regio nal de Fisio terapia e Terapia Ocupacio nal",
    "CRF": "Conselho Regio nal de Farmácia",
    "CREFONO": "Conselho Regio nal de Fo noaudiolgia",
    "CRM": "Conselho Regio nal de Medicina",
    "CRN": "Conselho Regio nal de Nutrição",
    "CRO": "Co nselho Regio nal de Odo ntolo gia",
    "Código": "Descrição",
    "CRP": "Conselho Regio nal de Psico lo gia",
    "OUT": "Outro s Co nselhos",
    "CRBIO": "Conselho Regio nal de Bio lo gia",
    "CREF": "Conselho Regio nal de Educação  Física",
    "CRMV": "Co nselho Regio nal de Medicina Veterinária",
    "CRTR": "Conselho Regio nal de Técnico s em Radio lo gia",
}

FG_RECURSO_PROPRIO = {"S": "cooperated", "Y": "accredited", "N": "accredited"}

TIPO_TABELA = {"22": "TUSS - Health procedures and events"}

# COMMAND ----------

# MAGIC %md
# MAGIC Supporting Info Structure