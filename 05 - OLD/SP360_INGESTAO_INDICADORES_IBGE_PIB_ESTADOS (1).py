# Databricks notebook source
# MAGIC %md ### Ingestão dos dados 

# COMMAND ----------

# MAGIC %md
# MAGIC * **Conjunto de Dados:**  Valores de Indicadores Estaduais dos Estados brasileiros
# MAGIC
# MAGIC * **Fonte:** IBGE
# MAGIC
# MAGIC * **Endereço:** https://apisidra.ibge.gov.br/values/t/5938/n3/all/v/37/p/all/f/u
# MAGIC
# MAGIC * **Forma de acesso:**  Webservices - API Rest
# MAGIC
# MAGIC * **Classificação:**  Público
# MAGIC
# MAGIC * **Autor:**  Daniel Mota Lopes

# COMMAND ----------

# DBTITLE 1,Definir parâmentros básicos
NOME_FONTE_DADOS = 'indicadores'
PASTA = 'indicadores_municipais'
NOME_CONJUNTO_DADOS ='ibge_pib_estados'
EXTENSAO_ARQUIVO_RAW = '.xml'
NOMEESQUEMA = 'sp360_ingestao'

# COMMAND ----------

# DBTITLE 1,Carregar notebook de funções de apoio a ingestão de dados
# MAGIC %run ../../compartilhado/SP360_INGESTAO_FUNCOES_BASICAS

# COMMAND ----------

# DBTITLE 1,Carregar os pacotes específicos
import pandas as pds
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


import os
import datetime
import requests
import json
import numpy as np
from pyspark import pandas as pd
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Obter o conjunto de dados
retry_strategy = Retry(
    total=4, 
    status_forcelist=[429, 500, 502, 503, 504],
)

adapter = HTTPAdapter(max_retries=retry_strategy)
 
session = requests.Session()
url = f'https://apisidra.ibge.gov.br/values/t/5938/n3/all/v/37/p/all/f/u'
session.mount(url, adapter)

r = requests.get(url)
conteudo = r.text

conteudojson = json.loads(conteudo)
print('Registros baixados' ,len(conteudojson))

# COMMAND ----------

# DBTITLE 1,Transformar o conjunto para formato tabular
dfpd_dados= pd.DataFrame(conteudojson[1:])
dfpd_dados = dfpd_dados.rename(columns={
    'NC': 'Nível Territorial (Código)',
    'NN': 'Nível Territorial',
    'MN': 'Unidade de Medida',
    'V': 'Valor',
    'D1C': 'UF (Código)',
    'D1N': 'UF',
    'D2N': 'Variável',
    'D3N': 'Ano'
})
dfpd_dados['Valor'] = pd.to_numeric(dfpd_dados['Valor'], errors='coerce')

# COMMAND ----------

print(dfpd_dados)
