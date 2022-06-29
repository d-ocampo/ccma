#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: d-ocampo jdquinoneze
"""

#Librerías cargadas
import pandas as pd    # Para el manejo de dataframes
import numpy as np     # Para algunas operaciones numÃ©ricas
import time            # Para calcular tiempo de ejecuciÃ³n
import re              # Para manipular cadenas

import os
from os import listdir                   # Para listar directorios
from os.path import isfile, join, isdir  # Para manipular archivos

from datetime import datetime # para manejo de datetime
import xlrd #para manipular formatos de excel

import json # Para manipulación de diccionarios

#Para cargar el conector con mongo
import pymongo
from pymongo import MongoClient


# from datetime import datetime, time
import time


########################################################################

#################################
### 0. Fixed Data ###############
#################################

# Rutas
data_path = os.getcwd()+"/data/"
exp_path = "Experiencia del cliente/"

files_exp_path = [f for f in listdir(data_path + exp_path) if isfile(join(data_path + exp_path, f))]

# Columnas para verificar en los archivos de experiencia de datos
exp_cols = ['Origen', 'Identificación de respuesta ', 'Nombre del evento',
       'Fecha creación', 'Cédula ', 'Nombre ', 'Empresa', 'Cargo', 'Nit',
       'Correo electrónico', 'Teléfono', 'Sede ', 'Pregunta Nivel1',
       'Pregunta Nivel2', 'Respuesta']

#Diccionarios de agrupación de servicios
dict_agrupacion={
    "CP": "Capacitación",
    "SR": "Servicios registrales",
    "OT": "Otros",
    "SE": "Servicios especializados",
    "FE": "Fortalecimiento empresarial",
    "MA": "Métodos alternativos para la solución de conflictos"
    }

# Diccionario de nivel educativo
dict_nivel_educativo={4: "Universitario",
    5 :"Especialista",
    3 :"Tecnólogo",
    2 :"Técnico",
    6 :"Magister",
    1 :"Bachiller",
    10:"Secundaria",
    9:"Primaria",
    7:"Doctor",
    8:"Otro"   
    }

#Diccionario de tipos de documentos
dict_tipo_doc={1:'Cedula de ciudadanía',
    2:'Cedula de extranjería',
    3:'Pasaporte',
    4:'Tarjeta de identidad',
    5:'NIT',
    6:'Empresa Extranjera',
    7:'Número único de identificación personal NUIP',
    8:'Registro Civil'
                }

# Diccionario maestra de temas
dict_temas={20080:"Gobierno corporativo",
    200100:"Internacionalización",
    200110:"Jurídica",
    20040:"Desarrollo del ser",
    20030:"Contabilidad y finanzas",
    200130:"Mercadeo y Servicio al Cliente",
    20010:"Arte",
    20070:"Gestión humana",
    20060:"Gerencia y Administración",
    20050:"Economía",
    20090:"Innovación y Transformación Digital",
    200150:"Productividad",
    200180:"Ventas",
    20020:"Cine",
    200120:"Literatura",
    200140:"Música",
    200160:"Registros",
    200170:"Solución de Conflictos",
    20040:"Calidad y procesos",
    20050:"Comunicación",
    20070:"Contabilidad"
    }



#Cruce entre interés demanda e intereses
#Datos de demanda
# Registros                              117076
# Gerencia y Administración               75081
# Jurídica                                66807
# Innovación y Transformación Digital     39582
# Contabilidad y Finanzas                 32321
# Mercadeo y Servicio al Cliente          27926
# Ventas                                  20834
# Proyectos y servicios                   16998
# Gestión Humana                          14980
# Productividad                           11122
# Economía                                 8372
# Negocios y networking                    7467
# Gobierno Corporativo                     5372
# Internacionalización                     4482
# Música                                   3581
# Desarrollo del Ser                       1566
# Gobierno                                 1513
# Literatura                                810
# Cine                                      745
# Arte                                      736
# Solución de Conflictos                    706
# Propiedad intelectual                     146
# Sostenibilidad                            138

# maestra_interes=pd.read_excel(data_path+"Maestra Temas Interes.xlsx")

################################
### Funciones de arreglo data ##
################################


# function to get unique values
def unique(list1):
    # insert the list to the set
    list_set = set(list1)
    # convert the set to the list
    unique_list = (list(list_set))
    return unique_list


#Crear diccionario de homolagación de cargos a partir del excel
def diccionario_cargos(ruta):
    areas_homologar=pd.read_excel(ruta)
    areas_homologar.index=areas_homologar['Área de cargo']
    areas_homologar=areas_homologar.drop(['Área de cargo'],axis=1)
    # areas_homologar['Palabras que contengan']=areas_homologar['Palabras que contengan'].apply(lambda x: x.lower().split(","))
    areas_homologar['Palabras que contengan']=areas_homologar['Palabras que contengan'].apply(lambda x: x.lower())
    dict_areas=areas_homologar.to_dict()['Palabras que contengan']
    return dict_areas

# Enviar diccionario de cargos a data
with open(data_path+'dict_cargo.json', 'w') as outfile:
    json.dump(diccionario_cargos(data_path + 'Tabla homologación áreas de cargo .xlsx'), outfile)

# Cargar diccionario de cargos al entorno
with open(data_path+'dict_cargo.json') as json_file:
    dict_cargo = json.load(json_file)


#Buscar cargo similar según validación
def homologar_cargo(dictionary, value):
    foundkeys = []
    for key in dictionary:
        for i in value.lower().split():
            if i in dictionary[key]:
                foundkeys.append(key)
    foundkeys.append("Otro")
    return foundkeys[0]

# Homologar tipo asistente
def validar_tipo_asistente(palabra):
    if 'EMPRE' in palabra.upper():
        return 'EMPRESA'
    else:
        return 'INDEPENDIENTE'

#Función para devolver la llave de un diccionario
def devolver_llave(diccionario, item):
    res=list(filter(lambda x: x[1]==item,list(diccionario.items())))
    return res[0][0]

# Función para eliminar registros con n_percent de nulos
def nulls_filter(n, data):

    # Cantidad de columnas
    cols_size = data.shape[1]

    # Cantidad de columnas con nulos
    data = data.replace("Otro", np.nan).replace("", np.nan)
    data['nulls_by_row'] = data.isnull().sum(axis = 1)

    # Porcentaje aceptable de eliminación de filas según cantidad de filas nulas
    percent_accept = n

    data['percent'] = data.apply(lambda x: x['nulls_by_row']/cols_size, axis = 1)
    data['mantener'] = data['percent'].apply(lambda x: 1 if x <= percent_accept else 0)
    data['mantener'].value_counts()

    # Filtrar filas aceptables
    data = data[data['mantener']==1]
    data.drop(['nulls_by_row', 'mantener', 'percent'], inplace = True, axis = 1)
    return data


#####################################
### Funciones de minería de datos ###
#####################################

#### 1. Data experience #############


#Función que genera los archivos unificados de experiencias
def generar_data_exp(exp_cols):
    # Inicio de la ejecución
    print("Inicia unión de data experience")
    start_time = time.time()
    # Para cargar todos los archivos de experiencia
    data_experience = pd.DataFrame()
    
    #Ciclo para unir todos los archivos de cliente
    for file in files_exp_path:
       df = pd.concat(pd.read_excel(data_path + exp_path + file,
                      sheet_name=None, usecols=exp_cols), ignore_index=True, sort=False)
       data_experience = data_experience.append(df, ignore_index=True)
    
    
    # Para corregir los nombres de las columnas
    exp_cols = [col.strip().upper().replace(" ", "_").replace("Ã", "E").replace("Ã", "O").replace("É", "E").replace("Ó", "O") for col in exp_cols]
    data_experience.columns = exp_cols
    
    #FInaliza ejecución
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")

    return data_experience

# función para arreglar data de experiencia
def arreglar_data_exp(data_experience,exp_cols,dict_cargo):
    # Inicio de la ejecución
    start_time = time.time()
    print('Inicia arreglo de data experience')

    #Estabdarizar columnas
    exp_cols = [col.strip().upper().replace(" ", "_").replace("Ã", "E").replace("Ã", "O").replace("É", "E").replace("Ó", "O") for col in exp_cols]
    data_experience.columns = exp_cols
    
    # Nueva variable de categorí­a a partir del Origen
    data_experience['ORIGEN_CAT'] = data_experience['ORIGEN'].apply(lambda x: x[:2])
    
    # Para completar los datos nulos
    data_experience['NOMBRE_DEL_EVENTO'].fillna("Sin especificar", inplace = True)
    data_experience['SEDE'].fillna("Sin especificar", inplace = True)
    
    
    # Correcciones sobre el nÃºmero de cÃ©dula
    data_experience['CEDULA_NEW'] = data_experience['CEDULA'].apply(lambda x: "".join(re.findall('\d+', str(x))))
    data_experience['CEDULA_NEW'] = data_experience.apply(lambda x: "".join(re.findall('\d+', str(x['NOMBRE']))) if x['CEDULA_NEW'] == "" else x['CEDULA_NEW'], axis = 1)
    data_experience['CEDULA_NEW'] = data_experience['CEDULA_NEW'].apply(lambda x: 0 if x == "" else int(x))
    
    # Correcciones sobre el nombre
    data_experience['NOMBRE_NEW'] = data_experience.apply(lambda x: x['CEDULA'] if len("".join(re.findall('\d+', str(x['NOMBRE']))))>0  else x['NOMBRE'], axis = 1 )
    
    # Correcciones sobre el email y el telÃ©fono
    data_experience['CORREO_NEW'] = data_experience.apply(lambda x: str(x['CORREO_ELECTRONICO']) if "@" in str(x['CORREO_ELECTRONICO']) else "", axis = 1)
    data_experience['TELEFONO_NEW'] = data_experience.apply(lambda x: x['TELEFONO'] if len("".join(re.findall('\d+', str(x['TELEFONO']))))>=7 else "", axis = 1)

    #Homologar cargos con el diccionario de cargos
    data_experience['CARGO_HOMOLOGO']=data_experience['CARGO'].apply(lambda x: homologar_cargo(dict_cargo,str(x)))

    # Para presentar el tiempo transcurrido desde el inicio de la ejecuciÃ³n
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    
    return data_experience


#### 2. Temas de interés ##############

#Cargar las bases de interés
def cargar_interes():
    start_time = time.time()
    print('Inicia carga de temas de interés')
    #Cargar la tabla
    temas_interes=pd.read_excel(data_path+'TemasInteres.xlsx')
    #tiempo
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    return temas_interes

# Arreglar la tabla de interés
def arreglar_interes(temas_interes):
    start_time = time.time()
    print('Inicia arreglo de temas de interés')
    #reemplazar columnas
    temas_interes.columns=[col.upper() for col in temas_interes.columns]
    
    #reemplazar nombre de cédula
    temas_interes.rename(columns={'NRO_CEDULA':'CEDULA'},inplace=True)
    
    # Correcciones sobre el nÃºmero de cÃ©dula
    temas_interes['CEDULA_NEW'] = temas_interes['CEDULA'].apply(lambda x: "".join(re.findall('\d+', str(x))))
    temas_interes['CEDULA_NEW'] = temas_interes.apply(lambda x: "".join(re.findall('\d+', str(x['NOMBRE']))) if x['CEDULA_NEW'] == "" else x['CEDULA_NEW'], axis = 1)
    temas_interes['CEDULA_NEW'] = temas_interes['CEDULA_NEW'].apply(lambda x: 0 if x == "" else int(x))
    
    #Poner en mayúscula los nombres
    temas_interes['NOMBRE']=temas_interes['NOMBRE'].apply(lambda x: str(x).upper()) 
    temas_interes['APELLIDO']= temas_interes['NOMBRE'].apply(lambda x: str(x).upper()) 
    
    #Reemplazar cargo
    temas_interes['CARGO_HOMOLOGO']=temas_interes['CARGO'].apply(lambda x: homologar_cargo(dict_cargo,str(x)))
    
    #Arreglar nivel educativo
    temas_interes['NIVEL_EDUCATIVO']=temas_interes['NIVEL_EDUCATIVO'].fillna(8).apply(lambda x: x if str(x).isdigit() else 8).astype(int).apply(lambda x: x if x in list(dict_nivel_educativo.keys()) else 8)
    temas_interes['NOMB_NIVEL_EDUCATIVO']=temas_interes['NIVEL_EDUCATIVO'].apply(lambda x: dict_nivel_educativo[x])
    
    #Arreglar tipo asistente
    temas_interes['TIPO_ASISTENTE']=temas_interes['TIPO_ASISTENTE'].fillna('OTRO').apply(lambda x: 'OTRO' if str(x).isdigit() else validar_tipo_asistente(x).upper() )

    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    return temas_interes

#### 3. Contactos ##############

def cargar_contactos():
    start_time = time.time()
    print('Inicia carga de contactos')
    #Cargar los datos
    contactos=pd.read_excel(data_path+'Contactos.xlsx')
    
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    return contactos
    
def arreglar_contactos(contactos,devolver_llave,dict_tipo_doc):
    start_time = time.time()
    print('Inicia arreglo de contactos')
    #modificar las columnas
    contactos.columns=[col.upper().strip() for col in contactos.columns]
    #Cambiar nombre de columna cedula
    contactos.rename(columns={'NÚMERO DE DOCUMENTO':'CEDULA'},inplace=True)
    #Arreglar el tipo de identificación
    contactos['TIPO DE DOCUMENTO']=contactos['TIPO DE DOCUMENTO'].replace('C','Cedula de ciudadanía')
    #Llenar el tipo de contacto
    contactos['TIPO DE CONTACTO'].fillna('Otro',inplace=True)
    #Resumir por número de cédula
    contactos=contactos.groupby(['TIPO DE DOCUMENTO','CEDULA']).agg(
        {'NOMBRE COMPLETO':['first','count'],
         'DIRECCIÓN':'first', 
         'CIUDAD':'first', 
         'DEPARTAMENTO':'first', 
         'PAÍS':'first', 
         'TIPO DE CONTACTO':'first',
         'MIEMBRO DE LA JUNTA DIRECTIVA CCMA':'max'}
        )
    #MOdificar índices de filas y columnas
    contactos.columns=['NOMBRE COMPLETO','CANTIDAD_LLAMADAS', 'DIRECCIÓN', 'CIUDAD', 'DEPARTAMENTO', 'PAÍS',
           'TIPO DE CONTACTO', 'MIEMBRO DE LA JUNTA DIRECTIVA CCMA']
    contactos.reset_index(inplace=True)
    #Arreglar cédula    
    contactos['CEDULA_NEW'] = contactos['CEDULA'].apply(lambda x: "".join(re.findall('\d+', str(x))))
    contactos['CEDULA_NEW'] = contactos.apply(lambda x: "".join(re.findall('\d+', str(x['NOMBRE COMPLETO']))) if x['CEDULA_NEW'] == "" else x['CEDULA_NEW'], axis = 1)
    contactos['CEDULA_NEW'] = contactos['CEDULA_NEW'].apply(lambda x: 0 if x == "" else int(x))
   
    #Cambiar el tipo doc por la codificación
    contactos['TIPO DE DOCUMENTO']=contactos['TIPO DE DOCUMENTO'].apply(lambda x: devolver_llave(dict_tipo_doc,x))

    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    return contactos

##### 4. Demanda #########

def cargar_demanda():
    start_time = time.time()
    print('Inicia carga de demanda')
    #Cargar los datos
    demanda=pd.read_excel(data_path+'Demanda de los servicios.xlsx')
    
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    return demanda

def arreglar_demanda(demanda):
    start_time = time.time()
    print('Inicia arreglo de demanda')
    #Modificar las columnas de demanda
    demanda.columns=[col.upper().strip() for col in demanda.columns]
    
    #eliminar datos de los eventos
    demanda_personas=demanda[['FECHAINSCRIPCIÓN', 'REGIÓN', 'IDENTASISTENTE', 'NOMBREASISTENTE',
           'NITRE', 'RAZÓNSOCIAL', 'CARGO', 'TIPOACTIVIDAD', 'FORMATONOMBRE','ID']]
    
    #Cambiar todo a string
    demanda_personas=demanda_personas.fillna('')
    
    #Cambiar fecha por string
    demanda_personas['FECHAINSCRIPCIÓN']=demanda_personas['FECHAINSCRIPCIÓN'].astype(str)
    demanda_personas['FORMATONOMBRE']=demanda_personas['FORMATONOMBRE'].astype(str)
    demanda_personas['ID']=demanda_personas['ID'].astype(str)
    
    #agrupar preferencias de la persona
    demanda_personas=demanda_personas.groupby('IDENTASISTENTE').agg(
        {'FECHAINSCRIPCIÓN':','.join,
         'REGIÓN':','.join,
         'NOMBREASISTENTE':'first',
         'NITRE':'first',
         'RAZÓNSOCIAL':'first',
         'CARGO':'first',
         # se propone, pero se puede cambiar
         'TIPOACTIVIDAD':','.join,
         'FORMATONOMBRE':','.join,
         'ID':','.join
         }
        ).reset_index()
    
    #Crear la validación de cargos
    demanda_personas['CARGO_HOMOLOGO']=demanda_personas['CARGO'].apply(lambda x: homologar_cargo(dict_cargo,str(x)))
    
    #Cambiar nombre de identifiación
    demanda_personas.rename(columns={'IDENTASISTENTE':'CEDULA'},inplace=True)
    
    
    #Arreglar cédula
    demanda_personas['CEDULA_NEW'] = demanda_personas['CEDULA'].apply(lambda x: "".join(re.findall('\d+', str(x))))
    demanda_personas['CEDULA_NEW'] = demanda_personas.apply(lambda x: "".join(re.findall('\d+', str(x['NOMBREASISTENTE']))) if x['CEDULA_NEW'] == "" else x['CEDULA_NEW'], axis = 1)
    demanda_personas['CEDULA_NEW'] = demanda_personas['CEDULA_NEW'].apply(lambda x: 0 if x == "" else int(x))
       
    
    
    #Agurpar los eventos que se ha asistido
    eventos=demanda[['ID','NOMBRE', 'LUGAR', 'PROYECTO', 'LÍNEA', 'TEMA','REGIÓN']]
    eventos=eventos.drop_duplicates(subset='ID')
    
    #tiempo
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    return demanda_personas,eventos


#### 5. Llamadas ##############

def carga_llamadas():
    start_time = time.time()
    print('Inicia carga de llamada')
    
    #Cargar los datos
    llamada=pd.read_excel(data_path+'LlamadasTMK.xlsx')
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    return llamada


def arreglo_llamadas(llamada):
    start_time = time.time()
    print('Inicia arreglar llamadas')
    
    #cambiar columnas
    llamada.columns=[col.upper().strip() for col in llamada.columns]
    
    llamada=llamada.rename(columns={'NIT':'CEDULA'})

    #cambiar cedula a estandar    
    llamada['CEDULA_NEW'] = llamada['CEDULA'].apply(lambda x: "".join(re.findall('\d+', str(x))))
    llamada['CEDULA_NEW'] = llamada.apply(lambda x: "".join(re.findall('\d+', str(x['NOMBRE Y APELLIDO NUEVO CONTACTO1']))) if x['CEDULA_NEW'] == "" else x['CEDULA_NEW'], axis = 1)
    llamada['CEDULA_NEW'] = llamada['CEDULA_NEW'].apply(lambda x: 0 if x == "" else int(x))
           
    #cambiar el tipo de documento
    llamada['TIPO DE DOCUMENTO']=llamada['CEDULA_NEW'].astype(int).apply(lambda x: 5 if str(x)[0] in ["8","9"] and len(str(x))==9  else 1)
    
    #cambiar la fecha a str
    llamada['FECHA LLAMADA 1']=llamada['FECHA LLAMADA 1'].astype(str)
    
    #agrupación por tipo de cédula y número
    llamada=llamada.groupby(['TIPO DE DOCUMENTO','CEDULA_NEW']).agg(
        {'CEDULA':['count','first'], 
         'MATRICULA':'first', 
         'FECHA LLAMADA 1':','.join, 
         'RESPONSABLE1':','.join,
         'ESTADO DEL TELEMERCADEO1':'first', 
         'NOMBRE Y APELLIDO NUEVO CONTACTO1':'first',
         'CARGO1':'first', 
         'NUEVO TELÉFONO1':'first', 
         'NUEVO E-MAIL1':'first', 
         'INTENCIÓN RENOVACIÓN1':'first',
         'AGENDA CITA RENOV1':'first', 
         'TIPOSERVICIO':'first', 
         'TEMA DEL SERVICIO1':'first',
         'COMENTARIOS1':'first', 
         'UTILIDAD LLAMADA1':'first', 
         'DURACIÓN':'first', 
         'CONTADOR CONTACTADO':'first',
         'CONTACTO PERGAMINO':'first', 
         'RAZÓN SOCIAL':'first', 
         'CANAL POR EL QUE RENOVARÁ':'first',
         'NOMBRE PERGAMINO':'first', 
         'ENTRANTE':'first', 
         'TEXTO':'first', 
         'TIPO DE ELEMENTO':'first',
         'RUTA DE ACCESO':'first'
         }
        ).reset_index()
    
    #cambiar el nombre de las columnas
    llamada.columns=['TIPO DE DOCUMENTO','CEDULA_NEW','NO_LLAMADAS','CEDULA', 
                     'MATRICULA', 'FECHA LLAMADA 1', 'RESPONSABLE1',
           'ESTADO DEL TELEMERCADEO1', 'NOMBRE Y APELLIDO NUEVO CONTACTO1',
           'CARGO1', 'NUEVO TELÉFONO1', 'NUEVO E-MAIL1', 'INTENCIÓN RENOVACIÓN1',
           'AGENDA CITA RENOV1', 'TIPOSERVICIO', 'TEMA DEL SERVICIO1',
           'COMENTARIOS1', 'UTILIDAD LLAMADA1', 'DURACIÓN', 'CONTADOR CONTACTADO',
           'CONTACTO PERGAMINO', 'RAZÓN SOCIAL', 'CANAL POR EL QUE RENOVARÁ',
           'NOMBRE PERGAMINO', 'ENTRANTE', 'TEXTO', 'TIPO DE ELEMENTO',
           'RUTA DE ACCESO']
    
    #tiempo transcurrido
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    
    return llamada


#### 6. Cuentas ######################

#Función para cargar cuentas
def cargar_cuentas():

    start_time = time.time()
    print('Inicia carga de cuentas')

    # Cargar los datos
    cuentas = pd.read_excel(data_path+'Cuentas.xlsx')
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    
    return cuentas
    
def arreglar_cuentas(cuentas):
    start_time = time.time()
    print('Inicia arreglo de cuentas')
  
    # Tabla de homologación de los tipos de documento de la base de Cuentas
    dict_tipo_doc_new = {
        'C': 1,
        'Cedula de ciudadanía': 1,
        'NIT': 5,
        'Pasaporte': 3,
        'Empresa Extranjera': 6,
        'Cedula de extranjería': 2,
        'Tarjeta de identidad': 4,
    }

    # Creación del dataframe desde el diccionario
    dict_tipo_doc_new = pd.DataFrame.from_dict([dict_tipo_doc_new]).T.reset_index()
    dict_tipo_doc_new.columns = ['Tipo de documento', 'Tipo_documento_value']

    # Para realizar la corrección en el dataframe original
    cuentas = cuentas.merge(
        dict_tipo_doc_new, on="Tipo de documento", how="left")
    cuentas['Tipo_documento_value'] = cuentas['Tipo_documento_value'].fillna(
        1)
    cuentas['Tipo_documento_value'] = cuentas['Tipo_documento_value'].astype(
        'int')

    # Para corregir las cédulas
    cuentas['CEDULA_NEW'] = cuentas['Número de documento'].apply(
        lambda x: "".join(re.findall('\d+', str(x))))
    cuentas['CEDULA_NEW'] = cuentas['CEDULA_NEW'].apply(
        lambda x: 0 if x == "" else int(x))
    cuentas = cuentas[cuentas['CEDULA_NEW'] != 0]

    # Para corregir la razón social y el nombre comercial
    cuentas['Razón Social'] = cuentas['Razón Social'].apply(
        lambda x: x.upper() if isinstance(x, str) else "NO ESPECIFICADO")
    cuentas['Nombre comercial'] = np.where(pd.isnull(
        cuentas['Nombre comercial']), cuentas['Razón Social'], cuentas['Nombre comercial'].str.upper())

    # Para corregir variables con valores Sin definir
    not_defined_vars = ['Naturaleza',
                        'TPCM',
                        'Tamaño',
                        'Es proponente',
                        'Representante legal',
                        'Empresa activa en Cámara',
                        'Ciudad dirección comercial',
                        'Departamento dirección comercial',
                        'Cámara de comercio/Regional',
                        'Ciudad  dirección notificación judicial',
                        'Departamento dirección notificación judicial',
                        'Dirección notificación judicial'
    ]

    for col in not_defined_vars:
        cuentas[col] = np.where(
            pd.isnull(cuentas[col]), "Sin definir", cuentas[col])


    # Para crear variables binarias con las que lo permiten
    dummy_vars = ["Es afiliado",
                "¿Empresa miembro de junta directiva?",
                "Clúster VIP",
                "Es empresa de familia",
                "Exporta",
                "Importa?",
                "Pertenece a RNT",
                "Es proveedor CCMA"]

    for col in dummy_vars:
        cuentas[col] = cuentas[col].apply(
            lambda x: 0 if x == "No" else 1)

    # Para convertir los números decimales en fechas (date)
    # date_cols = ['Fecha de matrícula', 'Fecha de última renovación']

    # for col in date_cols:
    #     cuentas[col + "_new"] = cuentas[col].fillna("")
    #     cuentas[col + "_new"] = cuentas[col + "_new"].apply(lambda x: datetime(xlrd.xldate_as_tuple(x, 0)).date() if x != "" else "")

    # Para eliminar columnas sobrantes
    cols_to_drop = [
        "No. sucursales en Colombia",
        "No. Sucursales en el exterior",
        'Número de documento'
    ]
    # cuentas.drop(cols_to_drop + date_cols, axis=1, inplace=True)
    cuentas.drop(cols_to_drop, axis=1, inplace=True)

    # tiempo transcurrido
    time_lapse = time.strftime('%X', time.gmtime(time.time() - start_time))
    print(f"Tiempo transcurrido: {time_lapse}")
    
    return cuentas




##################################
### Funciones de carga de data ###
##################################

def cargar_todo(n):

    #### 1. Data experience ################
    data_exp=generar_data_exp(exp_cols)
    data_exp=arreglar_data_exp(data_exp,exp_cols,dict_cargo)
    data_exp=nulls_filter(n, data_exp)

    #### 2. Temas de interés ###############
    interes=cargar_interes()
    interes=arreglar_interes(interes)
    interes=nulls_filter(n, interes)

    #### 3. Contactos #####################
    contactos=cargar_contactos()
    contactos=arreglar_contactos(contactos,devolver_llave,dict_tipo_doc)
    contactos=nulls_filter(n, contactos)

    #### 4. Demanda #######################

    demanda=cargar_demanda()
    demanda,eventos=arreglar_demanda(demanda)
    demanda=nulls_filter(n, demanda)


    #### 5. Llamadas ######################
    llamada=carga_llamadas()
    llamada=arreglo_llamadas(llamada)
    llamada=nulls_filter(n, llamada)
    
    #### 6. Cuentas ######################
    cuentas=cargar_cuentas()
    cuentas=arreglar_cuentas(cuentas)
    cuentas=nulls_filter(n, cuentas)

    return [data_exp, interes, contactos, demanda, eventos, llamada,cuentas]



##################################
### Funciones de creación de BD ##
##################################

#### Creación de las llaves tipo_doc cedula_new

def crear_cedulas_base(data_exp,interes,contactos,demanda,llamada,cuentas):
    ## Llave de la bd personas 
    cedulas=[]
    
    # Ingresar las cédulas al vector único
    # data experience
    cedulas.extend([(1,i) for i in list(data_exp['CEDULA_NEW'].unique())])
    # interés
    cedulas.extend([(1,i) for i in list(interes['CEDULA_NEW'].unique())])
    # contactos
    cedulas.extend(list(zip(contactos['TIPO DE DOCUMENTO'],contactos['CEDULA_NEW'])))
    #demanda
    cedulas.extend([(1,i) for i in list(demanda['CEDULA_NEW'].unique())])
    #llamdas
    cedulas.extend(list(zip(llamada['TIPO DE DOCUMENTO'],llamada['CEDULA_NEW'])))
    #cuentas
    cedulas.extend(list(zip(cuentas['Tipo_documento_value'],cuentas['CEDULA_NEW'])))
    
    #Obtener las únicas
    cedulas=unique(cedulas)
    return cedulas


def crear_cedulas_carga(data):
    ## Llave de la bd personas 
    cedulas=[]
    
    # Ingresar las cédulas al vector único
    # data experience    
    try:
        if 'TIPO DE DOCUMENTO' in data.columns:
            tipo_doc=data['TIPO DE DOCUMENTO']
        else:
            tipo_doc=data['Tipo_documento_value']
    except:
        tipo_doc=[1]*len(data)
        
    #cuentas
    cedulas.extend(list(zip(tipo_doc,data['CEDULA_NEW'])))
    
    #Obtener las únicas
    cedulas=unique(cedulas)
    return cedulas


### Creación de los diccionarios por persona

def dict_personas_pm(tipo_id,ident,data_exp,interes,contactos,demanda,llamada,cuentas):
    dict_personas={
    # Se debe convertir a string la unión entre tipo_doc - doc
    'identificación':str(tipo_id)+'-'+str(ident),
    #características de ñps clientes
        'características':{
        # Carga data experience
        'EXPERIENCIA':data_exp[data_exp['CEDULA_NEW']==ident].to_dict('records'),
        # Carga interés
        'INTERES':interes[interes['CEDULA_NEW']==ident].to_dict('records'),
        # Carga contactos
        'CONTACTOS':contactos[(contactos['TIPO DE DOCUMENTO']==tipo_id) & (contactos['CEDULA_NEW']==ident)].to_dict('records'),
        # Carga Demanda
        'DEMANDA':demanda[demanda['CEDULA_NEW']==ident].to_dict('records'),
        # Carga llamadas
        'LLAMADAS':llamada[(llamada['TIPO DE DOCUMENTO']==tipo_id) & (llamada['CEDULA_NEW']==ident)].to_dict('records'),
        # Carga cuentas
        'CUENTAS':cuentas[(cuentas['Tipo_documento_value']==tipo_id) & (cuentas['CEDULA_NEW']==ident)].to_dict('records')
        }
    }
    
    return dict_personas
    

####  Conectar la base de datos

#Base en mongo atlas
def conectar_colection_mongo_ccma(coleccion,base):
    if base==1:
        # client = pymongo.MongoClient('hostname', 27017)
        client = pymongo.MongoClient('mongodb://localhost:27017')
    else:
        #cambiar la dirección si se utiliza mongo
        client = pymongo.MongoClient("mongodb+srv://proyecto_uniandes:WpyATG4YVumTaPEd@cluster0.66yl3.mongodb.net/ccma?retryWrites=true&w=majority")
    #Cargar la base de ccma
    db = client['ccma']
    #cargar la colección elegida
    collection = db[coleccion]
    return collection


    
# #### Enviar a la base de mongo

#Conectar la colección elegida
# clientes_col=conectar_colection_mongo_ccma('clientes')


# #traer las cédulas unicas
# cedulas_unicas=crear_cedulas_base()

# #iteración para cargar a mongo
# for i,j in cedulas_unicas[0:100]:
#     #insertar 1 a 1 cada registro
#     clientes_col.insert_one(dict_personas_pm(i,j))


# clientes_col.find({})

# db.student.find({}, {roll:1, _id:0})


# ################################
# #### COnsultas útiles ##########
# ################################

# #cantidad clientes
# len(cedulas_unicas)

# #Encuestas de experiencia
# len(data_exp['CEDULA_NEW'].unique())
# len(data_exp['CEDULA_NEW'].unique())/len(cedulas_unicas)

# #Interes
# len(interes['CEDULA_NEW'].unique())
# len(interes['CEDULA_NEW'].unique())/len(cedulas_unicas)

# #Contactos
# len(contactos['CEDULA_NEW'].unique())
# len(contactos['CEDULA_NEW'].unique())/len(cedulas_unicas)

# #Demanda
# len(demanda['CEDULA_NEW'].unique())
# len(demanda['CEDULA_NEW'].unique())/len(cedulas_unicas)

# #Llamadas
# len(llamada['CEDULA_NEW'].unique())
# len(llamada['CEDULA_NEW'].unique())/len(cedulas_unicas)

# #Eventos
# len(eventos['ID'].unique())

# #cuentas
# len(cuentas['CEDULA_NEW'].unique())
# len(cuentas['CEDULA_NEW'].unique())/len(cedulas_unicas)

# ########## serie de tiempo de eventos

# #aplanar la lista de eventos
# flat_list = []
# for sublist in demanda['FECHAINSCRIPCIÓN'].str.split(',').to_list():
#     for item in sublist:
#         flat_list.append(item)

# #contar los eventos en plano
# x=Counter(flat_list)

# # Diccionario del conteo
# ordered_dict=OrderedDict(sorted(x.items()))
# #convertir fecha en js
# def fecha_js(fecha):
#     d = datetime.date(int(fecha.split('-')[0]), int(fecha.split('-')[1]), int(fecha.split('-')[2]))
#     for_js = int(time.mktime(d.timetuple())) * 1000
#     return for_js

# #print de la fecha
# for i in [[fecha_js(i[0]),i[1]] for i in list(ordered_dict.items())]:
#     print(i,',')


# ########## onteo de tipo de eventos

# #aplanar la lista de eventos
# flat_list = []
# for sublist in demanda['TIPOACTIVIDAD'].str.split(',').to_list():
#     for item in sublist:
#         flat_list.append(item)

# #contar los eventos en plano
# x=Counter(flat_list)

# #porcetnaje de asistencia
# [i/539180 for i in [33932, 361867, 456, 97108,45817]]

# ######### COnteo de temas

# #aplanar la lista de eventos
# flat_list = []
# for sublist in interes['TEMAS_INTERES'].dropna().str.split(',').to_list():
#     for item in sublist:
#         try:
#             if int(item) in dict_temas.keys():
#                 flat_list.append(item)
#         except:
#             pass

# #contar los eventos en plano
# x=Counter(flat_list)

# [i[0] for i in [[dict_temas[int(i[0])],i[1]] for i in list(x.items())[0:10]]]
    
# suma=sum([i[1] for i in [[dict_temas[int(i[0])],i[1]] for i in list(x.items())[0:10]]])
# [round(i/suma*100) for i in [i[1] for i in [[dict_temas[int(i[0])],i[1]] for i in list(x.items())[0:10]]]]
    
#     print('{')
#     print(' x:"',i[0],'",')
#     print(' y:',round(i[1]/1000),',')
#     print('},')


# import random
# no_of_colors=len(list(x.items()))
# color=["#"+''.join([random.choice('0123456789ABCDEF') for i in range(6)])
#        for j in range(no_of_colors)]


# dsample=data_exp.sample(10000)

# data_exp.columns

# group_data=data_exp.groupby(['ORIGEN_CAT', 'CARGO_HOMOLOGO']).agg({'CEDULA_NEW':'count'}).reset_index()

# group_data['ORIGEN_CAT']=group_data['ORIGEN_CAT'].apply(lambda x: dict_agrupacion[x])



# group_data=group_data.sort_values(by=['CARGO_HOMOLOGO'])


# group_data['CARGO_HOMOLOGO'].unique()

# group_data['CEDULA_NEW'].to_list()


