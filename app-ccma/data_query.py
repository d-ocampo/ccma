#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  1 01:07:33 2022

@author: davidsaw
"""
from pymongo import MongoClient
import pandas as pd
from data_load import conectar_colection_mongo_ccma
import json

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

# conectar a base local de mongo
clientes_col = conectar_colection_mongo_ccma('clientes', 1)
# límite de consultas
limit = 10000


# 1.llamadas
# crear queries de filtroy proyección
filter_q = {'características.LLAMADAS': {'$size': 1}}
project = {'características.LLAMADAS': 1}

# crear la consulta a la base
result_llamadas = clientes_col.find(
    filter=filter_q,
    projection=project,
    limit=limit
)

# transformar el cursor a lista y luego a tabla
llamadas_list = [doc['características']['LLAMADAS'][0]
                 for doc in result_llamadas]
llamadas = pd.DataFrame.from_dict(llamadas_list, orient='columns')


# 2. demanda
# crear queries de filtroy proyección
filter_q = {'características.DEMANDA': {'$size': 1}}
project = {'características.DEMANDA': 1}

# crear la consulta a la base
result_demanda = clientes_col.find(
    filter=filter_q,
    projection=project,
    limit=limit
)

demanda_list = [doc['características']['DEMANDA'][0] for doc in result_demanda]
demanda = pd.DataFrame.from_dict(demanda_list, orient='columns')


# 3. demanda
# crear queries de filtroy proyección
filter_q = {'características.INTERES': {'$size': 1}}
project = {'características.INTERES': 1}

# crear la consulta a la base
result_interes = clientes_col.find(
    filter=filter_q,
    projection=project,
    limit=limit
)

interes_list = [doc['características']['INTERES'][0] for doc in result_interes]
interes = pd.DataFrame.from_dict(interes_list, orient='columns')

######################
### buscar contacto ##
######################


def datos_contacto(buscar):
    busqueda = clientes_col.find_one(filter={'identificación': buscar})
    try:
        tel = busqueda['características']['LLAMADAS'][0]['NUEVO TELÉFONO1']
        mail = busqueda['características']['LLAMADAS'][0]['NUEVO E-MAIL1']
    except:
        tel = 'nan'
        mail = 'nan'
    return tel, mail

# #tel
# llamadas['CEDULA_NEW'].apply(lambda x: datos_contacto('1-'+str(x))[0])


def devolver_persona(buscar):
    try:
        lista = clientes_col.find_one(filter={'identificación': buscar})
        result = 'identificación:' + \
            json.dumps(lista['identificación'], indent=4)+'\n'
        result = result+json.dumps(lista['características'], indent=4)
    except:
        result = ''
    return result
