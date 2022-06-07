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


def devolver_persona(buscar):
    try:
        lista = clientes_col.find_one(filter={'identificación': buscar})
        result = 'identificación:' + \
            json.dumps(lista['identificación'], indent=4)+'\n'
        result = result+json.dumps(lista['características'], indent=4)
    except:
        result = ''
    return result


### Buscar si existe
def buscar_existe(buscar):
    buscado=clientes_col.find_one({'identificación':buscar})
    if buscado is None:
        return 0
    else:
        return 1

### Actualizar cliente si existe
#Buscar los datos del cliente original
def buscar_datos_existe(buscar,base):
    #buscar='1-1007845123'
    existe=clientes_col.find_one({'identificación':buscar})
    
    tipo_id,ident=buscar.split('-')
    
    #Segun la selección de la base llenar ese espacio
    if base=="Experiencia":
        existe['características']['EXPERIENCIA'] = data[data['CEDULA_NEW']==ident].to_dict('records')
    elif base=="Llamadas":
        existe['características']['LLAMADAS'] = data[(data['TIPO DE DOCUMENTO']==tipo_id) & (data['CEDULA_NEW']==ident)].to_dict('records')
    elif base=="Cuentas":
        existe['características']['CUENTAS'] = data[(data['Tipo_documento_value']==tipo_id) & (data['CEDULA_NEW']==ident)].to_dict('records')
    elif base=="Demanda":
        existe['características']['DEMANDA'] = data[data['CEDULA_NEW']==ident].to_dict('records')
    elif base=="Interés":
        existe['características']['INTERES'] = data[data['CEDULA_NEW']==ident].to_dict('records')
    elif base=="Contactos":   
        existe['características']['CONTACTOS'] = data[(data['TIPO DE DOCUMENTO']==tipo_id) & (data['CEDULA_NEW']==ident)].to_dict('records')
        
    #buscar ese cliente
    myquery = { "identificación": buscar }
    # crear los datos nuevos
    newvalues = { "$set": {'características':
            # Carga data experience
            'EXPERIENCIA':existe['características']['EXPERIENCIA'],
            # Carga interés
            'INTERES':existe['características']['INTERES'],
            # Carga contactos
            'CONTACTOS':existe['características']['CONTACTOS'],
            # Carga Demanda
            'DEMANDA':existe['características']['DEMANDA'],
            # Carga llamadas
            'LLAMADAS':existe['características']['LLAMADAS'],
            # Carga cuentas
            'CUENTAS':existe['características']['CUENTAS']
            }
        }
    
    #Actualizar el cliente en la base
    clientes_col.update_one(myquery, newvalues)
    print('Se subió satisfactioriamente:', buscar)


#### Crear nuevo si no existe
# nuevo='1-1015438726'
def agregar_nuevo(nuevo,base):
    #separar el tipoid e identificación
    tipo_id,ident=nuevo.split('-')
    
    dict_personas={
    # Se debe convertir a string la unión entre tipo_doc - doc
    'identificación':str(tipo_id)+'-'+str(ident),
    #características de los clientes - vacías porque no se tiene data
        'características':{
        # Carga data experience
        'EXPERIENCIA':[],
        # Carga interés
        'INTERES':[],
        # Carga contactos
        'CONTACTOS':[],
        # Carga Demanda
        'DEMANDA':[],
        # Carga llamadas
        'LLAMADAS':[],
        # Carga cuentas
        'CUENTAS':[]
        }
    }
    
    #Segun la selección de la base llenar ese espacio
    if base=="Experiencia":
        dict_personas['características']['EXPERIENCIA'] = data[data['CEDULA_NEW']==ident].to_dict('records')
    elif base=="Llamadas":
        dict_personas['características']['LLAMADAS'] = data[(data['TIPO DE DOCUMENTO']==tipo_id) & (data['CEDULA_NEW']==ident)].to_dict('records')
    elif base=="Cuentas":
        dict_personas['características']['CUENTAS'] = data[(data['Tipo_documento_value']==tipo_id) & (data['CEDULA_NEW']==ident)].to_dict('records')
    elif base=="Demanda":
        dict_personas['características']['DEMANDA'] = data[data['CEDULA_NEW']==ident].to_dict('records')
    elif base=="Interés":
        dict_personas['características']['INTERES'] = data[data['CEDULA_NEW']==ident].to_dict('records')
    elif base=="Contactos":   
        dict_personas['características']['CONTACTOS'] = data[(data['TIPO DE DOCUMENTO']==tipo_id) & (data['CEDULA_NEW']==ident)].to_dict('records')
    
    # subir a la base
    clientes_col.insert_one(dict_personas)
    print('Se subió satisfactioriamente:', nuevo)





















